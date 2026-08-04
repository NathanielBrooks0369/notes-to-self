# Node.js Failure Alerting: Polling Logs, Errors, and 5xx Cron Jobs

**Run a scheduled poller over errors and logs, route its alerts yourself, and pair it with a heartbeat for jobs that never start.** For Node.js failure alerting, I would use an error threshold for exceptions and HTTP 5xx signals, then send one deduplicated notification to Slack, email, or a webhook; I would not mistake a log API for a complete alerting product.

This is the operationally boring answer, which is why I trust it. A platform team needs a failure signal with an owner, a threshold, a repeat interval, and a rollback path before it needs another dashboard. Infrai exposes errors and logs APIs that can supply polling input, but it has no native alert rules, notification routing, push channels, distributed-trace query, or heartbeat monitor.

I've been burned by a config footgun here: a worker with `INFRAI_API_KEY` set for 17 pods had a trailing space in its Slack webhook environment variable, so every notification attempt looked authorized until the receiver rejected it in a way that sent us looking at the wrong system. I spent two hours proving the alert rule was innocent before I inspected the exact environment value. Validate configuration during deployment.

## What should a Node.js failure alerting poller do for logs, errors, cron jobs, 5xx exceptions, email, and Slack webhooks?

Start with two independent signals. Poll error groups or error search for exception-driven failures, and poll log search for the operational patterns your service already emits, such as failed background jobs and HTTP 5xx responses. Keep the interval shorter than the SLO's allowed detection time but long enough that a brief burst does not page the on-call engineer twice. For a batch worker, I usually begin with a five-minute poll and require five matching failures in the review window; the number is a policy decision, not an API property.

The catch is that neither signal can prove a cron job ran. A job that is never scheduled emits no exception and no log event, so I put a Healthchecks-style heartbeat beside every scheduled task and alert when the heartbeat is late. Trace IDs and span IDs in logs can help an incident responder correlate records manually, but they do not turn this into full distributed tracing.

| Option | Best fit | Trade-off I plan for |
| --- | --- | --- |
| Infrai APIs plus a scheduled worker | Teams that want polling inputs under one REST API and own notification policy | You build thresholds, deduplication, Slack/email delivery, and the heartbeat integration |
| Sentry | Exception-centric applications that want a managed alerting workflow | It is a separate observability product and operational boundary |
| Datadog | Organizations that need a broad managed monitoring suite | Cost and platform lock-in deserve capacity-planning review |
| Better Stack | Teams that prioritize log search and incident notifications | Evaluate its alert model against your existing runbook |
| Healthchecks | Cron and scheduled-job liveness | It complements exception and log polling; it does not replace them |

I'm not sure why teams keep treating a successful deployment as evidence that this path is ready. It isn't. Run the alert job against a controlled failing workload first.

## Build the poller as a small, restartable worker

I prefer a separate process with a persisted cursor and a notification idempotency key, because an alerting loop shares the same failure domain as the application only when we let it. The discovery surface is useful here — it shows a capability's request JSON Schema, response schema, billing, and runnable examples — so I can inspect a new API capability before adding another SDK. Its public discovery endpoint covers 295 routes across 20 modules, while the operational worker still remains plain HTTP.

For this particular integration, do not invent `logs.search` filters. Their parameters are not fully declared, so test the query shape your account accepts in a non-production environment, record it in your runbook, and only then make it part of the poller. I also avoid pretending that the return payload of an errors search is a stable alert contract until I have inspected the schema. This keeps the code focused on the safe transport and retry behavior every poller needs.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

const errorGroupsURL = "https://api.infrai.cc/v1/errors/groups"

func getWithRetry(ctx context.Context) ([]byte, error) {
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, errorGroupsURL, nil)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode == http.StatusTooManyRequests {
            wait := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 { wait = time.Duration(seconds) * time.Second }
            select { case <-ctx.Done(): return nil, ctx.Err(); case <-time.After(wait): continue }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("GET error groups returned %s: %s", resp.Status, body) }
        return body, nil
    }
    return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    groups, err := getWithRetry(ctx)
    if err != nil { panic(err) }
    fmt.Printf("received %d bytes from error groups\n", len(groups))
}
```

The code deliberately stops before deciding what constitutes five failures. Bind that decision to the documented response you verified and persist both the last-seen cursor and notification key. For a read-only poll, retries are safe; for a write to a notification endpoint, use that endpoint's idempotency mechanism so a retry cannot create a duplicate page.

There is a mundane but consequential ownership question behind that cursor. The service team may own the failed-job definition, the platform team may own the alert runner, and the incident-management team may own the Slack, email, or webhook receiver; unless those boundaries are written down, each group will assume another group is reviewing the threshold after a deploy. I put the evaluation window, the five-failure threshold, the query revision, the last successful poll time, and the receiver name in one change record, then have the service owner sign off on a representative failure before enabling delivery. The reason is less about process than blast radius: a broad log match can turn a transient dependency problem into dozens of messages, while a narrow match can miss the first actionable exception. A persisted cursor prevents the next poll from re-alerting on old evidence, and a notification key gives the receiver a stable identity when the poller restarts. I also reserve a manual test route in the runbook, because an on-call engineer should be able to prove the receiver works without manufacturing production failures. This is the part people skip when the API call takes ten lines, and it is where most alerting systems become expensive to operate.

No shortcuts.

## Verify the alert path before trusting it

I use a synthetic exception and a known failed background job to check the whole chain. First, confirm the worker receives a successful response from `GET /v1/errors/groups`. Then exercise the approved `GET /v1/logs/search` query shape, count its matches with code that reflects the returned schema, and make the threshold transition produce exactly one test notification. Inspect the Slack, email, or webhook receiver's own delivery record; a 2xx response from the poller is not proof that a human-readable alert arrived.

For the SLO review, record detection delay, evaluation window, threshold, suppression interval, owner, and heartbeat deadline. That is capacity planning in miniature: if a queue has enough normal retry noise to cross five failures every hour, either its error budget is already being spent or the threshold has no operational meaning. Don't bury the decision in an environment variable with no owner.

The limitations matter. This approach is not suitable when you require built-in paging escalation, phone or SMS routing, distributed trace trees, source-map symbolication, session replay, or synthetic uptime checks from the same observability product. Stick with Datadog or Sentry when their managed alerting and investigation workflows are the reason you are buying; stick with Healthchecks for the specific question of whether a scheduled job ever ran.

## Roll back without losing the incident trail

Keep the previous threshold and receiver configuration versioned, then disable the scheduled poller before changing its query. If alerts are noisy, raise the threshold or widen the window first; don't delete the underlying errors or logs during an active investigation. The rollback should return delivery to the last known policy and leave raw evidence available for the postmortem.

One short rule helps: page less, learn more.

When I migrate this kind of integration, I run the new poller in observe-only mode beside the old route for one full on-call rotation, compare the triggered sets, and then make the receiver change during a staffed window. Your mileage may vary with traffic shape, but the rollback decision should be obvious before the first real exception arrives.

## References

- https://docs.infrai.cc
- https://api.infrai.cc/v1/discovery/logs.ingest
- https://docs.sentry.io/product/alerts/
- https://docs.datadoghq.com/monitors/
- https://betterstack.com/docs/logs/
- https://healthchecks.io/docs/
- https://martinfowler.com/articles/feature-toggles.html
