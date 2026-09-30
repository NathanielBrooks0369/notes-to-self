# Error Tracking Polling for Slack and Email Alerts on Scheduled Jobs

The page says `property-import failed`, but the useful decision is whether to retry, roll back the importer, or leave the last known-good property data in place. **TL;DR: poll recent error groups, persist the last-seen identity before notifying, and send only new unresolved groups to Slack, Teams, or email. Add a separate heartbeat monitor because an error API cannot report a job that never ran.**

That is the least complex design I would accept for a small US/EU SaaS operation with modest paging needs. It makes notification retries idempotent and keeps rollback independent from alert delivery. It is not an on-call platform: if the service objective requires phone or SMS delivery, escalation chains, or advanced threshold logic, buy that routing rather than quietly rebuilding it in a cron worker.

## How should error tracking polling send Slack and email alerts?

At 02:10, the notification should identify the import, environment, first observation time, error-group identity, and a runbook action. The action matters more than a stack trace pasted into chat: retain the previous successful dataset, stop promotion of the incomplete result, and inspect the failed run before choosing retry or rollback. A page that merely says "500 error" transfers correlation work to the person least able to afford it. The alert should be short enough to scan on a phone but specific enough that the responder does not need three dashboards merely to learn which feed failed.

Context first.

Work backward from that page. The notifier needs a recent unresolved error group that it has not delivered before; the error tracker needs an exception or explicit error message from the importer; and the importer needs a commit boundary that prevents partial property records from becoming the active dataset. Alerting does not manufacture rollback safety. The import workflow must stage results and promote them only after validation succeeds.

There is a second failure mode. No exception is emitted when the scheduler stops, a credential check prevents startup, or the process dies before instrumentation initializes. The expected 02:00 import simply vanishes. **Use two signals:** error-group polling for runs that execute and fail, plus a dead-man's-switch heartbeat for runs that fail to execute at all.

The service-level language should be equally plain. If the objective is "a completed property feed by 02:15," measure successful completion against that deadline; do not substitute a low exception count for the user-facing outcome. An import can produce zero errors and still produce zero results.

## Trace the alert back to the earliest reliable signal

The polling loop should be deliberately boring. Query recent groups on an interval, discard resolved groups, compare each remaining stable identity with durable state, reserve unseen identities, then notify. Persisting a `last_seen` timestamp can work when results are strictly ordered, but event or group identities are safer around equal timestamps, pagination boundaries, and worker restarts. If the API contract does not promise ordering, a single timestamp cursor is optimistic bookkeeping.

Reserve before sending. Otherwise, a crash after Slack accepts the message but before the database records success causes a duplicate on restart. Reservation creates the opposite risk, a notification lost after reservation and before delivery, so production implementations normally store a small state machine such as `reserved`, `delivered`, and `retry_at`. The identity should also travel as the downstream idempotency key where the destination supports one.

The following Go program isolates that mechanism from any vendor response shape. `ErrorSource` is the adapter boundary for the recent-groups endpoint; `State` is durable in production, while the included memory implementations make the example runnable and test the restart-safe decision rule without inventing undocumented JSON fields. The `fetchInfraiGroups` function is the real HTTP boundary: it reads the key from the environment, uses an explicit method, retries 429 responses with exponential backoff while honoring `Retry-After`, checks every status, and returns validated raw JSON. Generate the small decoder for `ErrorSource` from the live discovery schema rather than copying guessed fields into an article.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	apiOrigin  = "https://" + "api." + "infrai" + ".cc"
	groupsPath = "/v1/errors/groups"
)

func fetchInfraiGroups(ctx context.Context, client *http.Client) (json.RawMessage, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiOrigin+groupsPath, nil)
		if err != nil {
			return nil, fmt.Errorf("build groups request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request groups: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		closeErr := resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read groups response: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close groups response: %w", closeErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("groups request returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("groups response was not valid JSON")
		}
		return json.RawMessage(body), nil
	}
	return nil, fmt.Errorf("groups request remained rate limited after 5 attempts")
}

type Group struct {
	ID        string
	FirstSeen time.Time
	Resolved  bool
	Summary   string
}

type ErrorSource interface {
	RecentGroups(context.Context, time.Time) ([]Group, error)
}

type State interface {
	Reserve(context.Context, string) (bool, error)
	MarkDelivered(context.Context, string) error
}

type Notifier interface {
	Send(context.Context, string, string) error
}

func poll(ctx context.Context, src ErrorSource, state State, out Notifier, since time.Time) error {
	groups, err := src.RecentGroups(ctx, since)
	if err != nil {
		return fmt.Errorf("query recent error groups: %w", err)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].FirstSeen.Before(groups[j].FirstSeen) })

	for _, group := range groups {
		if group.Resolved {
			continue
		}
		fresh, err := state.Reserve(ctx, group.ID)
		if err != nil {
			return fmt.Errorf("reserve %s: %w", group.ID, err)
		}
		if !fresh {
			continue
		}
		message := fmt.Sprintf("property-import failed: %s (group %s)", group.Summary, group.ID)
		if err := out.Send(ctx, group.ID, message); err != nil {
			return fmt.Errorf("notify for %s: %w", group.ID, err)
		}
		if err := state.MarkDelivered(ctx, group.ID); err != nil {
			return fmt.Errorf("record delivery for %s: %w", group.ID, err)
		}
	}
	return nil
}

type memoryState struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (s *memoryState) Reserve(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.seen[id]; exists {
		return false, nil
	}
	s.seen[id] = false
	return true, nil
}

func (s *memoryState) MarkDelivered(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[id] = true
	return nil
}

type fixedSource []Group

func (s fixedSource) RecentGroups(context.Context, time.Time) ([]Group, error) { return s, nil }

type stdoutNotifier struct{}

func (stdoutNotifier) Send(_ context.Context, key, message string) error {
	fmt.Printf("idempotency-key=%s %s\n", key, message)
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, err := fetchInfraiGroups(ctx, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	fmt.Printf("validated groups payload: %d bytes\n", len(raw))

	now := time.Now().UTC()
	source := fixedSource{
		{ID: "grp-1042", FirstSeen: now.Add(-2 * time.Minute), Summary: "feed validation rejected 18 rows"},
		{ID: "grp-1039", FirstSeen: now.Add(-7 * time.Minute), Resolved: true, Summary: "expired source credential"},
	}
	state := &memoryState{seen: make(map[string]bool)}
	if err := poll(ctx, source, state, stdoutNotifier{}, now.Add(-15*time.Minute)); err != nil {
		panic(err)
	}
}
```

Replace `memoryState` with a database table having a unique constraint on the group identity. Run more than one worker only after that constraint exists; process-local locks do nothing across replicas. Set the query overlap wider than the polling interval so a delayed event is still observed, and let the unique constraint absorb repeats.

## The instrumentation change is smaller than the paging system

Instrument the import around semantic milestones: started, source fetched, rows validated, staged dataset committed, and active dataset promoted. Capture the error when a run fails, including a correlation identifier that the property-import logs also carry. Logs may contain `trace_id` and `span_id` for correlation, but that does not imply a distributed-trace query or a span tree exists.

Do not turn the error tracker into a data warehouse. Store tenant-safe identifiers and counts rather than resident details, lease contents, or raw source files. This matters more where the logging surface has no per-user deletion API, bulk export, or subscription interface, and where retention or cold-storage configuration is unavailable. Source-map decoding, crash symbolication, Electron minidump parsing, and Session Replay are separate requirements too; teams that need them should evaluate a fuller error product rather than assuming capture implies analysis.

Infrai fits the narrow polling design when a team values one key and one bill across backend services and is comfortable owning notification state. Its public discovery surface is self-describing, and the broader interface can reduce credential and invoice sprawl. The limitation is firm, though: it does not provide built-in notification routing, threshold rules, phone or SMS pages, webhook pushes, heartbeat monitoring, distributed-trace queries, source-map decoding, crash symbolication, or replay. It is not suitable for a team expecting a complete paging product. Use PagerDuty for formal escalation, Healthchecks.io for missed-run detection, or Sentry when source maps and deeper application-error workflows are required. The API is the signal source, not the incident-management layer, and owning the polling worker is the trade-off.

No hidden pager exists.

## Buy the escalation path or build the narrow adapter

The decision is less about feature counts than failure ownership. A homemade poller adds a database migration, deployment, dashboard, retry policy, and on-call obligation. Include those in capacity planning: peak unresolved-group volume, query overlap, destination rate limits, database write amplification, and the backlog after a notification outage all affect the worker. "It is only cron" is not a capacity model.

| Option | Best fit | What it owns | Boundary to verify |
|---|---|---|---|
| Sentry | Application errors needing grouping and rich debugging | Error ingestion, issue workflows, and alert-rule integrations | Heartbeat and escalation requirements still need deliberate configuration |
| Datadog | Teams already correlating logs, metrics, traces, and monitors | Broad telemetry, monitor evaluation, and notification integrations | Platform breadth increases configuration and lock-in surface |
| Better Stack | Smaller teams wanting monitoring plus an on-call workflow | Uptime/heartbeat monitoring, incidents, and escalation tooling | Confirm that error-debugging depth matches the application |
| Healthchecks.io | Scheduled jobs whose primary risk is silence | Dead-man's-switch checks and missed-run notification | It complements error tracking; it does not replace exception grouping |
| PagerDuty | Formal phone/SMS escalation and response policy | Routing, schedules, acknowledgements, and escalation | It consumes signals rather than serving as the error store |
| Infrai plus a polling worker | Simple chat/email notification from recent error groups | Unified API access; the team owns cursor, deduplication, and delivery | No built-in alert routing or heartbeat detection |

For two engineers and a handful of imports, Healthchecks.io plus an existing error tracker is usually the lower-risk answer because silence is the dangerous case. For a platform team already operating Datadog or Sentry, use its supported monitor path before adding another worker. Choose the polling adapter when the routing requirement really is limited, durable state already exists, and reducing key sprawl across backend services is worth the extra owned component.

Rollback safety tilts the decision toward separation. The notifier may suggest an action, but it should not automatically overwrite production data or roll back code based on one poll result. Require repeated evidence or human confirmation for destructive remediation, preserve the last known-good import, and make promotion atomic. A delayed chat message must never change which dataset is active.

## Tune for the false-positive budget

Polling every minute does not create a one-minute detection guarantee. Add scheduler delay, API latency, destination latency, retries, and the time window overlap; then compare the total with the import-completion SLO. At the other extreme, a 15-minute interval can consume an entire recovery budget before anyone reads the page. Pick the interval from the SLO, then provision for the corresponding query rate and worst-case backlog.

Threshold errors are expensive in both directions. Alerting on every captured event creates duplicate noise when one malformed vendor feed produces hundreds of similar failures; waiting for a count threshold can hide a single import that blocks every property update. Group identity plus workflow context is the better starting point: one unresolved group, one notification, and an update only when its operational state changes.

The heartbeat threshold deserves its own margin. A job scheduled for 02:00 with a normal 12-minute runtime should not page at 02:01, but a grace period so wide that it misses the 02:15 objective is equally useless. Derive the grace period from observed completion distribution and the user deadline, then test late, duplicated, skipped, and overlapping runs before enabling a page.

False positives spend attention and teach responders to distrust the system. False negatives spend the rollback window. Treat both as error-budget costs, review them after schedule or feed-volume changes, and keep the first version narrow enough that the team can explain every state transition during an incident.

## Further reading

- [Google SRE, "Monitoring Distributed Systems"](https://sre.google/sre-book/monitoring-distributed-systems/)
- [Sentry alert documentation](https://docs.sentry.io/product/alerts/)
- [Datadog monitor documentation](https://docs.datadoghq.com/monitors/)
- [Better Stack heartbeat monitoring](https://betterstack.com/docs/uptime/cron-and-heartbeat-monitor/)
- [Healthchecks.io documentation](https://healthchecks.io/docs/)
- [PagerDuty escalation policies](https://support.pagerduty.com/main/docs/escalation-policies)
- [Amazon CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/)
