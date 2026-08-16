# Threshold rules, notifications and polling: what a free error tracking tier can prove

Use the threshold rules and webhook notifications inside an error tracking product for exactly one job — paging a human when the service as a whole degrades — and build a separate, deliberately boring path for anything that has to compare tenant cohorts. A free API tier in this category is normally polling-only, with a fixed menu of threshold rules, and that combination is adequate for a pager and close to worthless for an experiment readout. The deciding constraint is the denominator: an alert fires on a count of events inside a window, while a cohort comparison needs errors per session in each arm, evaluated at a time you picked before you looked.

That difference drives everything below.

The scenario I'm writing against is a customer support desk that has enabled an AI-assisted reply suggester for a treatment cohort of tenants and left the rest on the old flow. The question the platform team gets asked on Monday is never "did any errors happen" — it's "is the treatment cohort worse, and by enough that we should roll it back."

## The failure mode: alerts that track traffic, not harm

A threshold rule is usually shaped like "more than 50 events matching this filter in 5 minutes." In a multi-tenant support product that rule is a traffic detector wearing an alerting costume. One large tenant importing a backlog of 40,000 tickets trips it while every cohort behaves exactly as designed; a small tenant whose suggester errors on a third of its sessions never trips it, because a third of a small number is still a small number. Both outcomes are wrong for experiment analysis and both are perfectly correct for paging, which is the point — the rule was built to protect the service, not to protect a statistical conclusion.

There's a second noise source that catches teams out, and it's the grouping algorithm. Error trackers fold individual events into issues by fingerprinting the stack trace, and a deploy that changes line numbers or wraps an exception can split one long-running issue into three new ones. New issues look like new problems. A rule that alerts on "a new issue seen more than N times" will therefore light up on the exact day you shipped the experiment, for reasons that have nothing to do with the experiment.

Alert fatigue is a capacity problem, not a taste problem.

If on-call has to triage twenty cohort-flavored notifications a week, the team will mute the channel, and then the one alert that mattered arrives into a muted channel. I'd rather run three alert rules that everyone trusts than thirty that nobody reads, and that preference is what drives the split I'm arguing for: keep the pager rules coarse and few, move the fine-grained comparison somewhere it can be reviewed on a schedule instead of interrupting someone.

## Can error tracking alerts, webhooks, and threshold rules answer a per-cohort experiment question?

They can carry the signal. They cannot compute it, and the gap between those two verbs is where most of the practical limitations live.

Threshold rules evaluate counts of events inside a time window, scoped by whatever filters the product exposes — project, environment, release, and in most cases custom tags. Tag filtering is genuinely useful here, because if you tag every captured event with the cohort assignment you can at least scope a rule to one arm. What no such rule gives you is the denominator, a fixed evaluation time, or any control over how many comparisons you're running at once. Fire a rule per cohort per error type across eight cohorts and you're running dozens of implicit hypothesis tests a day, which manufactures a significant-looking result on a schedule.

Webhooks are the part worth keeping. A webhook is a transport: a signed HTTP POST to an endpoint you own, carrying the alert or the event, at which point your own code owns the arithmetic. Signature verification and replay protection are on you, and redelivery means your receiver has to be idempotent — the delivery is at-least-once, so treat the payload identifier as a primary key rather than assuming exactly-once.

| Signal path | Answers well | Doesn't answer | Ongoing cost |
| --- | --- | --- | --- |
| Built-in threshold rule → notification | Is total volume abnormal right now | Is one cohort worse per session | Rule upkeep, mute discipline |
| Webhook → receiver you own | Deliver events promptly to your own logic | Nothing by itself; it's transport | Signature checks, idempotency, a service to run |
| Scheduled polling of an aggregate query | Cohort rates at a chosen evaluation time | Sub-minute detection | Poll budget, checkpoint state |
| Counters in your own metrics pipeline | Rates with real denominators and SLO burn | Stack traces and grouping | A metrics backend to operate |

Buy-versus-build lands in a boring place: buy the capture, grouping and triage workflow, because reimplementing symbolication and dedup is a project nobody on a platform team should sign up for, and build the twenty lines of comparison arithmetic, because that part is small, testable, and needs to match how you define the experiment rather than how a vendor defines an issue.

## Living with a polling-only free API

Polling-only is a design constraint, not a defect, and it's a survivable one if you poll aggregates rather than events. Ask the API for a grouped count over a window — errors by cohort tag, last 15 minutes — instead of paging through raw events and counting them locally. One request per window beats ten thousand event records for the same answer, and it keeps you inside a rate limit you don't control.

Poll on a fixed cadence with jitter, checkpoint the last window boundary you successfully processed, and honor backpressure properly: a 429 with a `Retry-After` header means wait that long, not retry immediately with a shorter timeout. At a 60-second cadence a single query costs 1,440 requests per day, which is the kind of number worth putting in a capacity note before someone adds a second query per cohort and multiplies it by eight.

Retention is the constraint people notice last. Free tiers keep events for days or weeks rather than quarters, so an experiment that runs for six weeks will outlive its own raw data unless you snapshot the aggregate you actually compare. Write the daily cohort counts into your own store — a table with cohort, window start, sessions, errors is enough — and the analysis stops depending on someone else's retention policy.

One more thing if any of your instrumentation ships in a CLI or a desktop agent that support engineers run locally: check the `DO_NOT_TRACK` environment variable before sending anything home. It costs three lines and it's the difference between telemetry and surveillance.

## Tag the cohort, count the denominator, compare on a schedule

The implementation that survives contact with a real rollout has three parts: every captured error carries the cohort tag, every experiment session increments a denominator whether or not it errored, and a scheduled job compares the two arms with an interval rather than a point estimate.

```go
package experiment

import (
	"fmt"
	"math"
)

// Cohort is one arm of the rollout. Sessions is the denominator: every reply
// session that entered the experiment, including the ones that went fine.
type Cohort struct {
	Name     string
	Sessions int
	Errors   int
}

// wilson returns a 95% Wilson score interval for the error rate. The normal
// approximation is unusable at the rates we care about (a cohort with 3 errors
// in 400 sessions), so use the score interval instead.
func wilson(errors, sessions int) (lo, hi float64) {
	if sessions == 0 {
		return 0, 1
	}
	const z = 1.96
	n := float64(sessions)
	p := float64(errors) / n
	den := 1 + z*z/n
	center := (p + z*z/(2*n)) / den
	margin := z * math.Sqrt(p*(1-p)/n+z*z/(4*n*n)) / den
	return math.Max(0, center-margin), math.Min(1, center+margin)
}

// Compare evaluates one scheduled readout. It reports separated=true only when
// the treatment interval sits entirely above the control interval and both arms
// cleared the sample floor agreed on before the rollout started.
func Compare(control, treatment Cohort, minSessions int) (verdict string, separated bool) {
	if control.Sessions < minSessions || treatment.Sessions < minSessions {
		return fmt.Sprintf("hold: %s=%d %s=%d sessions, floor is %d",
			control.Name, control.Sessions, treatment.Name, treatment.Sessions, minSessions), false
	}
	cLo, cHi := wilson(control.Errors, control.Sessions)
	tLo, tHi := wilson(treatment.Errors, treatment.Sessions)
	switch {
	case tLo > cHi:
		return fmt.Sprintf("regression: %s [%.3f,%.3f] above %s [%.3f,%.3f]",
			treatment.Name, tLo, tHi, control.Name, cLo, cHi), true
	case tHi < cLo:
		return fmt.Sprintf("improvement: %s [%.3f,%.3f] below %s [%.3f,%.3f]",
			treatment.Name, tLo, tHi, control.Name, cLo, cHi), true
	default:
		return "inconclusive: intervals overlap, keep collecting", false
	}
}
```

The sample floor is doing quiet work in that function. Without it, the first hour of a rollout produces a cohort with 11 sessions and 2 errors, an 18% error rate, and an interval wide enough to drive a truck through — and someone will screenshot the 18% into a channel long before anyone reads the interval. Deciding the floor in advance, along with the evaluation cadence, is what stops the readout from becoming a peeking machine that reports whichever arm looks worse at the moment you refreshed.

The poller that feeds it stays deliberately dumb:

```go
func pollWindow(ctx context.Context, c *http.Client, endpoint string, start, end time.Time) (map[string]int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("group_by", "cohort")
	q.Set("from", start.UTC().Format(time.RFC3339))
	q.Set("to", end.UTC().Format(time.RFC3339))
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Authorization", "Bearer "+os.Getenv("ERROR_API_TOKEN"))

	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		wait, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		return nil, fmt.Errorf("rate limited, retry after %ds", wait)
	}
	var counts map[string]int
	return counts, json.NewDecoder(resp.Body).Decode(&counts)
}
```

Express the outcome as an SLO rather than a threshold if you can: a per-cohort objective of, say, 99.5% of reply sessions completing without a captured error gives you an error budget, and a burn-rate view tells you whether the treatment arm is spending its budget faster than control. Budgets survive traffic growth. Raw thresholds don't, which is why the rule you wrote at 1,000 sessions a day starts screaming at 10,000.

## Verification and rollback

Verify the path before you trust a readout from it. Inject a known error rate into a canary tenant — a deliberate failure on one in twenty synthetic sessions — and confirm the scheduled comparison reports a regression at roughly the rate you injected; if it reports nothing, your denominator is wrong or your sampling is silently discarding events. Then send a duplicate webhook delivery at the receiver and confirm the second one changes no state. Last, reconcile denominators against the application's own session count for the same window: a gap of more than a couple of percent means the instrumentation is missing sessions, and every rate you compute is inflated by exactly that gap.

Rollback should be one flag flip, not a deploy.

Leave the coarse pager rules alone while you roll back, because they're the thing telling you whether the rollback itself made anything worse, and resist the urge to widen sampling when the poll budget runs out — widen the interval instead, since a 5-minute window with complete data beats a 1-minute window with a quarter of the events. The catch with the approach in this piece is that it's a batch answer: if your requirement is sub-minute detection of a cohort-specific regression, scheduled comparison is not suitable and you'll want streaming aggregation with the cohort as a dimension, which is a bigger operational commitment than most support platforms need. Stick with the built-in threshold rules and skip all of this when there's one tenant class, no experiment, and a pager that already works — the machinery here earns its keep only once you're comparing arms and the answer changes what you ship.

I'm not certain any of this survives a truly low-traffic cohort, incidentally. Below a few hundred sessions per arm per week the intervals stay overlapped no matter how careful the arithmetic is, and the honest readout is "we can't tell yet" — which is still a better answer than a threshold rule that fired because one tenant imported a backlog.

## References

- Prometheus alerting rules: https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/
- Alertmanager routing, grouping and inhibition: https://prometheus.io/docs/alerting/latest/alertmanager/
- Google SRE Workbook, alerting on SLOs and burn rates: https://sre.google/workbook/alerting-on-slos/
- RFC 9110, HTTP Semantics (status codes and Retry-After): https://www.rfc-editor.org/rfc/rfc9110
- RFC 6585, Additional HTTP Status Codes (429 Too Many Requests): https://www.rfc-editor.org/rfc/rfc6585
- Standard Webhooks specification, signing and replay protection: https://www.standardwebhooks.com/
- Evan Miller, How Not To Run An A/B Test (the peeking problem): https://www.evanmiller.org/how-not-to-run-an-ab-test.html
- W3C Trace Context: https://www.w3.org/TR/trace-context/
- Console Do Not Track, the DO_NOT_TRACK convention for CLI telemetry: https://consoledonottrack.com/
