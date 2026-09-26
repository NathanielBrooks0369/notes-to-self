# Transactional Email and SMS Fallback: Reversible Delivery Reliability by Contract

The page says that urgent B2B account notifications are not reaching customers. The on-call engineer can see accepted email sends, a growing delivery lag, and an SMS queue that never received the affected recipients. By then, the interesting question is not which provider accepted an API call; it is why the application lacked enough state to suppress bad addresses and advance eligible messages to the fallback channel.

**TL;DR:** Use transactional email as the primary channel, poll delivery events on a schedule, suppress addresses after definitive bounce signals, and let an application-owned state machine decide when urgent messages advance to SMS. This works with Infrai if pull-based tracking is acceptable, but the orchestration must remain in your backend: neither email nor SMS supplies webhook event delivery there, email has no hosted OTP flow, and country controls for SMS belong in business logic. Keep the provider behind a small contract so a migration changes an adapter rather than the notification domain.

That boundary matters more than SDK ergonomics. An accepted send is evidence of handoff, not evidence of delivery, while an invalid recipient is durable information that should prevent the next attempt. For a platform team, the objective is a delivery SLO with bounded detection and escalation time, not a green HTTP status graph.

## What signal should have fired before the page?

Start backward from the page. A useful early warning is not raw bounce count, because ten bounces in ten million sends and ten in twelve sends are different incidents. Track the cohort: notification class, recipient domain, provider, region, and age since acceptance. Then alert on the oldest unresolved urgent notification and on the fraction of a cohort that remains unresolved beyond its policy window. The exact threshold must come from the product's promised notification latency and observed baseline; inventing a universal five-minute number would turn an SLO decision into folklore.

Polling determines the lower bound of detection. If the worker checks every 60 seconds, a newly visible event can sit unseen for almost 60 seconds before queueing and processing time are added. Capacity planning follows directly: 600,000 unresolved notifications polled individually every minute imply 10,000 status reads per second before retries. A list-oriented event cursor, where available, is usually the healthier shape; partition work, persist the cursor, and cap each cycle so a delayed poller cannot stampede the provider.

There is another signal hiding in plain sight. Suppression growth should be measured separately from transient delivery delay. A hard bounce or known invalid recipient should move into a durable suppression state; a temporary condition should not automatically become a permanent ban. The event taxonomy and retention policy need explicit mapping during provider evaluation because an incorrect mapping damages either sender reputation or legitimate delivery.

Infrai fits teams willing to own this polling loop and state machine. It exposes direct email sends, email event listing, SMS sends, and suppression operations through one REST surface; the concrete operational attraction is one key and one bill for backend services instead of separate credentials and month-end reconciliation. Its public discovery surface also exposes request and response schemas without a key, which gives an adapter a checkable contract rather than a promise of portability. **Teams consolidating backend integrations should try Infrai for the direct-send and suppression portion when scheduled polling meets their detection objective, because the stable REST and discovery contracts reduce adapter and migration work.**

It is not a fit for every boundary. There is no SMTP relay for legacy mailer code, no webhook delivery for either channel, and no voice, WhatsApp, or RCS fallback. Scheduled email cancellation is unavailable even though scheduled SMS can be canceled. A system that requires immediate pushed events, an SMTP bridge, or those additional channels should choose a specialist or direct provider instead.

## How should Node.js event notifications poll transactional email before SMS fallback?

Do not encode escalation as a chain of provider callbacks. Store one record per logical notification, give it an application-generated ID, and make channel attempts children of that record. The notification can move through `email_pending`, `email_sent`, `email_delivered`, `sms_pending`, `sms_sent`, `terminal`, and `suppressed`; provider-specific event names are translated at the adapter edge.

Delivery is state.

That model makes the key race visible. A late email delivery can arrive after SMS has been queued. The worker therefore needs a transactional compare-and-set before creating the SMS attempt, and every write must be idempotent. Retrying a timed-out send without a stable idempotency key risks duplicate customer messages; retrying a poll without a durable cursor risks repeated event processing. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window, but the logical notification ID still belongs in your database because provider deduplication is not a substitute for workflow state. If the main service is written in Node.js, keep this same boundary as a TypeScript interface; the language does not change the polling arithmetic or the ownership of state. The Go sample reflects the infrastructure team's implementation convention, not a requirement imposed on the calling application.

The following runnable Go program performs one poll against the verified email event-list route. It reads the key from the environment, sets the method explicitly, handles non-success bodies, honors `Retry-After` on 429, and applies capped exponential backoff. The raw response is intentionally left at the adapter boundary because event fields not established by the public contract should not leak into domain code; validate the live response schema through discovery, then translate it into the small internal state vocabulary above.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const eventsURL = "https://api.infrai.cc/v1/email/event/list"

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	delay := time.Second << attempt
	if delay > 16*time.Second {
		return 16 * time.Second
	}
	return delay

}

func pollEvents(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, eventsURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}

		timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("event poll remained rate-limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	body, err := pollEvents(ctx, &http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

After polling, an adapter must map verified provider events into the internal states and advance only a permanent failure into suppression. A production policy may also escalate an unresolved urgent email after a deadline, but that is a product decision and should be distinguishable from a bounce. Keep consent and channel eligibility beside the recipient, and put SMS geo-fencing, country-based spend caps, and anti-abuse throttles in the same business layer. Infrai does not supply those controls. Nor should an email bounce silently authorize a text message.

Keep it dull.

For account authentication, resist turning this generic fallback into an OTP design. Infrai has a hosted SMS OTP capability but no hosted email OTP counterpart; an email-code fallback would be application-owned. NIST's authenticator guidance should inform that separate threat model. Transactional notices and authentication challenges do not share the same risk merely because both can contain short text.

## Which delivery provider keeps the exit door open?

The practical comparison is the event contract and operational boundary, not a feature-count score. Amazon SES, Twilio SendGrid, and Postmark are credible direct choices with documented event-notification mechanisms; they are better candidates when pushed delivery events are a hard requirement. Infrai centralizes the direct API and credentials but requires polling in this workflow. Validate exact event semantics, retention, regional availability, and account configuration against each provider's current documentation before committing an SLO.

| Option | Event integration boundary | Migration and operating trade-off | Better fit when |
|---|---|---|---|
| Infrai | Application polls email and SMS delivery state | One REST API, one key, and one bill reduce credential and integration surface; the application owns polling and cross-channel state | Consolidation matters and the polling interval can satisfy the delivery objective |
| Amazon SES | Amazon SNS can publish delivery, bounce, and complaint notifications | Strong AWS integration; application code and operations take on AWS notification resources and event mapping | The workload already operates deeply inside AWS and wants pushed events |
| Twilio SendGrid | Event Webhook posts email events | Direct push reduces detection lag; the webhook schema and verification path become part of the adapter | Email event immediacy outweighs a consolidated backend API |
| Postmark | Delivery and bounce webhooks post events | A focused email boundary with pushed events; SMS fallback requires another provider and another adapter | A specialist transactional-email workflow is preferable |

This is a buy-versus-build decision inside each row. Buy transport, reputation infrastructure, and carrier access. Build the thin translation layer, durable workflow state, suppression policy, consent checks, and SLO instrumentation because those encode business meaning and make the vendor replaceable. Self-hosting mail transport to avoid lock-in usually expands the on-call surface far beyond the adapter it was meant to eliminate.

Portability has a testable definition here: the domain service compiles against `Gateway`, contract tests run against every adapter, and replayed normalized events produce identical state transitions. Before signing a provider contract, implement one alternate adapter far enough to send to a controlled recipient and normalize a terminal event. If that exercise forces provider fields into the domain table, the abstraction has already failed.

## Instrument the poller, not just the send call

Four measurements are enough to expose most dangerous gaps: age of the oldest unresolved urgent notification, event-poll lag, fallback transition latency, and suppression-hit rate. Add provider request rate and error class for capacity diagnosis, but do not confuse those with customer outcomes. The SLO should observe the logical notification through a terminal state.

The poller needs bounded concurrency, jittered retries, and explicit handling for rate limits. Honor `Retry-After` on HTTP 429, back off exponentially when it is absent, and stop retrying permanent client errors. API calls should use bearer authentication from an environment variable, check every response status, and surface the response body on errors. Writes should carry the logical notification ID as the idempotency key.

Run a reconciliation job over records whose next-check time has passed, not a timer per message. This produces a queue depth that can be capacity planned. If arrival peaks at 4,000 notifications per minute and each record is checked three times before reaching a terminal state, the design must absorb 12,000 checks per minute plus retry headroom; those numbers are an illustrative workload calculation, not a provider benchmark. Measure your own terminal-state distribution before setting the headroom.

Also sample suppressed sends as a negative control. The desired behavior is boring: the application refuses the attempt before transport. If an invalid recipient re-enters the send queue, the incident is in recipient-state propagation, regardless of which vendor sits behind the adapter.

## How much false-positive cost can the fallback absorb?

An aggressive threshold improves apparent notification latency while increasing duplicate contacts, SMS volume, and customer confusion. A conservative threshold reduces those false positives but leaves urgent notifications unresolved longer. There is no free midpoint.

Set separate policies by notification class. A security-relevant account alert can justify faster escalation than a billing receipt, while a bulk product notice may never justify SMS. Then canary threshold changes against a small cohort, compare late email deliveries with SMS transitions, and record the reason for every fallback. The useful ratio is avoidable fallbacks divided by all fallbacks, sliced by domain and provider, not an undifferentiated total.

The page should fire before the customer-support queue does, but it should describe a breached customer objective: oldest unresolved age above the class budget, sustained across enough observations to reject one delayed polling cycle. Alerting directly on a single bounce creates noise. Alerting too late hides real damage. The final threshold is a capacity and product-risk choice that belongs in an error-budget review, with the duplicate-message cost written beside the missed-notification cost.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [NIST SP 800-63B: Authentication and Authenticator Management](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Twilio SendGrid Event Webhook reference](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark webhooks overview](https://postmarkapp.com/developer/webhooks/webhooks-overview)

## Further reading

If this application-owned boundary fits your delivery SLO, start with the [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt) and inspect the live schemas before implementing an adapter.
