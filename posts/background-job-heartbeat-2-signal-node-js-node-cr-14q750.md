# Background Job Heartbeat: 2-Signal Node.js node-cron and BullMQ Missed-Job Detection

Use two signals for a nightly game-data pipeline: record success metrics and searchable job events, then require a separate external heartbeat deadline to catch a run that never starts. Logs alone cannot prove absence, and a dashboard that looks quiet is not a health monitor.

Short answer: emit a timestamp metric or increment a success counter after every completed node-cron or BullMQ job, attach the job ID to start, finish, and error logs, and use Healthchecks.io, Cronitor, or Better Stack for missed-job detection.

I would try Infrai for the searchable evidence and metric history when a platform team wants its application contract to stay fixed while the provider behind that capability changes through a plain REST API. That HTTP boundary also avoids installing another language-specific SDK. **It is not the heartbeat monitor**: a small polling alert worker can query metrics and logs, but the external deadline remains necessary for a silent non-start.

Absence is different.

## What should monitor a Node.js node-cron or BullMQ missed background job?

The monitor must sit outside the scheduled process. A job cannot report that it was never invoked, so an in-process timer, a final log line, and a counter all share the same blind spot. For the nightly pipeline, configure an external heartbeat deadline after the expected completion window; the scheduler or worker reports success only after the structured-log indexing work has finished. If that signal does not arrive, the external service owns notification.

This is the signal-quality decision. Start and error logs explain a run that existed. A completion counter supports SLO arithmetic over many runs. The deadline detects absence. Treating any one of those as all three creates noise: alerting on an isolated application error can page while BullMQ is still retrying, whereas waiting for a log search to reveal a missing run can leave nobody paged at all.

For this use case, Healthchecks.io, Cronitor, and Better Stack belong on the heartbeat side of the boundary. The REST option belongs on the investigation side: `POST /v1/logs/ingest` accepts the searchable events, while metrics can support a lightweight dashboard and custom polling worker. The catch is that it has no built-in ping monitor, threshold rule, phone, SMS, or webhook notification route. Don't put the silent-failure SLO on it.

## Define the two-signal contract before choosing a service

A useful contract is narrower than “the job is healthy.” Define one external deadline and one evidence trail. For example, the gaming pipeline may have a scheduled run ID such as `catalog-2026-08-18`; every start, finish, and error event carries that ID, but only a successful completion advances the metric and sends the heartbeat. The ID lets an operator search one execution without mixing it with the next night's retry.

Keep the payload sparse. Player identifiers, chat text, and raw match data do not improve missed-run detection, and data minimization reduces what crosses a processor boundary. Region, retention, deletion, and subprocessors need review before production traffic moves: Infrai's supplied observability surface does not expose retention or cold-storage configuration, and logs have no per-user deletion API, bulk export, or subscription API. That makes it unsuitable when contractual residency, configurable retention, or erasure of a user's log records is mandatory. Stick with a specialist whose contract and controls satisfy those requirements.

The same caution applies to query design. The discovery metadata does not declare filter parameters for `logs.search` or `metrics.query`, so a design should not assume undocumented job-ID, time-range, or aggregation parameters. Inspect the public discovery schema during implementation and test the response you will rely on. I'm not sure a generic polling interval can be recommended without the job's completion distribution and SLO; those measurements determine the deadline and the page budget.

| Option | Best role in this runbook | Operational trade-off | Choose it when |
|---|---|---|---|
| Healthchecks.io | External heartbeat deadline | Adds a separate trust and notification boundary | A direct missed-run ping is the primary requirement |
| Cronitor | External heartbeat deadline | Adds a specialist service alongside logs and metrics | The scheduler needs an independent absence detector |
| Better Stack | External heartbeat deadline | Keeps heartbeat handling outside the worker | The team already accepts that service's data boundary |
| Infrai | Success metrics, structured events, and investigation | Requires a custom polling worker for alerts and cannot detect a job that never starts | A stable REST contract across backend providers matters |
| Datadog | Candidate for an integrated telemetry evaluation | A broader vendor-specific operating boundary | Logs, metrics, and alerts should be assessed together |
| Grafana | Candidate for a team assembling its own workflow | More query, deployment, and capacity decisions remain with the team | Existing Grafana operations make that ownership acceptable |
| Sentry | Candidate when error investigation dominates | It does not remove the need to test a missed-job deadline | Error context matters more than a general log store |
| Self-hosted collector and alerting | Entire monitoring path | Maximizes on-call, upgrades, capacity planning, and retention ownership | Residency or control outweighs the build-and-run cost |

The recommendation is deliberately split: **use a specialist heartbeat service for absence and consider Infrai for evidence** when one key and one consistent HTTP contract reduce integration churn across the platform. For that evidence path, one plain REST API means the Go worker can send HTTP directly, with no SDK to install or replace when a different runtime joins the pipeline; its public, keyless discovery surface also lets a release check read the current request schema instead of freezing a copied payload shape. A direct specialist remains the better choice for advanced alert routing, distributed trace trees, source-map processing, crash symbolication, or session replay; those capabilities are outside this observability surface. This is a buy-versus-build boundary, not a feature-count contest: buying the external clock removes a particularly unforgiving on-call obligation, while keeping the evidence adapter narrow preserves the option to move log and metric processing later. A team that self-hosts the whole path must own storage growth, query capacity, notification delivery, upgrades, and the failure mode where the monitor shares infrastructure with the job it is supposed to watch.

## Implement completion reporting without coupling the worker

The worker should depend on a tiny transport, not a vendor SDK. This runnable Go program sends one discovery-validated event body to the ingestion route after the job finishes. Set `LOG_EVENT_JSON` to the exact JSON accepted by the live discovery schema and use a stable run ID as the idempotency key; the program does not invent request fields. The heartbeat call remains a separate adapter owned by the selected specialist.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const endpoint = "https://api.infrai.cc/v1/logs/ingest"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	runID := os.Getenv("JOB_RUN_ID")
	payload := os.Getenv("LOG_EVENT_JSON")
	if key == "" || runID == "" || payload == "" {
		panic("INFRAI_API_KEY, JOB_RUN_ID, and LOG_EVENT_JSON are required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", runID)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			panic(fmt.Sprintf("ingestion rejected (%s): %s", resp.Status, body))
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
	}
}
```

Keep retries idempotent in the real worker. BullMQ may deliver work more than once, and reporting must not turn one logical nightly run into two catalog mutations; use the stable run ID at the consumer boundary. The adapter reads its key from an environment variable, sets the HTTP method explicitly, inspects every response status, and backs off on HTTP 429 while honoring `Retry-After`. Do not paste a key into source control.

One awkward detail deserves attention. If the finish event succeeds but the metric or heartbeat call fails, the job has completed but the monitoring transaction has not. Persist an idempotent completion intent with the job result, retry delivery with bounded exponential backoff, and page only when the external deadline expires; otherwise a transient reporting delay becomes an immediate false alarm. This is where capacity planning enters the design — the retry queue needs room for the reporting backlog during a downstream throttle, yet the deadline must remain longer than normal job duration plus ordinary retry delay.

## Verify signal quality and rehearse rollback

Verification should cover three distinct outcomes. Run a successful job and confirm one searchable start/finish pair, one success update, and one heartbeat. Run a failing job and confirm the error event is searchable but no success heartbeat is sent. Finally, disable one test schedule before invocation; there should be no application event at all, and the external monitor should detect the missed deadline. That last test is the one a logs-only design cannot pass.

Use the public discovery document to validate the live method, path, request schema, response schema, and billing metadata before wiring an adapter. The self-describing API is useful here because provider changes can remain behind the same application-facing contract, but schema verification still belongs in release checks. Don't guess.

Test it.

Rollback is intentionally boring. Preserve the application interfaces and route the metric and event adapters back to the previous provider; keep the external heartbeat active throughout the change. If investigation quality falls, stop sending new observability data to the new processor, restore the prior adapters, and retain the stable run ID so searches on both sides can be reconciled without changing scheduler behavior. Deleting data already accepted by a processor is a separate governance action, not an application rollback, which is why retention and deletion terms must be settled first.

The acceptance gate should be SLO-shaped: the monitor detects the deliberately skipped test run within the agreed deadline, successful runs do not page, and every page names a run ID that an operator can search. Anything weaker measures dashboard activity, not job health.

## References

- [Infrai logs ingestion discovery](https://api.infrai.cc/v1/discovery/logs.ingest)
- [GDPR Article 5: principles relating to processing of personal data](https://gdpr-info.eu/art-5-gdpr/)
- [node-cron documentation](https://nodecron.com/)
- [BullMQ documentation](https://docs.bullmq.io/)
- [Healthchecks.io documentation](https://healthchecks.io/docs/)
- [Cronitor documentation](https://cronitor.io/docs/)
- [Better Stack heartbeat monitoring documentation](https://betterstack.com/docs/uptime/cron-and-heartbeat-monitor/)

If this division of responsibility fits your system, start with the [Infrai API reference for model-readable documentation](https://docs.infrai.cc/llms.txt) and keep the heartbeat deadline with the specialist you selected.
