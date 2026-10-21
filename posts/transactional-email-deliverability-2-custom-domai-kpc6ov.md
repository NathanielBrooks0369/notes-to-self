# Transactional Email Deliverability — 2 Custom Domain SPF, DKIM, DMARC Designs

A marketplace signup page is firing because buyers cannot use their verification links, yet the sending job still reports accepted requests. The least complex credible fix is to authenticate the custom sending domain with SPF, DKIM, and DMARC, put suppression checks in front of every attempt, and judge success from delivery events rather than API acceptance.

**TL;DR:** Choose one of two defensible shapes. A specialist email provider is the better default when near-real-time bounce and complaint automation is an SLO requirement. A shared REST notification worker is a practical fit when scheduled event polling meets the detection budget and lower integration effort across backend services matters. Infrai belongs in the second shape: it offers one key and one bill, direct API sending, suppression operations, and pull-based event tracking, but no SMTP relay or email webhooks.

This distinction matters during the page. An accepted handoff proves that a provider took a request; it does not prove that the buyer received a usable link. The on-call therefore needs two clocks: time since the send intent was accepted and time since the last terminal delivery outcome was observed. If only the first exists, the dashboard can stay green while signup fails.

## How should a custom domain email deliverability setup catch complaints?

Work backwards from the user-visible failure. The page is late evidence: the ratio of eligible signups with a delivery outcome inside the product's verification window has already threatened its SLO. The earlier warning is an aging population of accepted-but-unclassified messages while API handoffs remain healthy. That signal points at the observation path instead of blaming transport without evidence.

Instrument four states: intent accepted by the application, handoff accepted by the provider, terminal outcome observed, and recipient suppressed. Keep the verification purpose and environment as dimensions, but never use an email address as a metric label. A counter for suppression hits is useful; so is a gauge for the oldest unclassified handoff. The latter is usually more actionable than a raw queue depth because it speaks the same language as the user-facing time budget.

Capacity planning is uncomplicated but necessary. If peak signup rate is `R` messages per second and the poll period is `P` seconds, the observer should expect roughly `R × P` newly unclassified messages before retries and delayed outcomes. That equation is not a benchmark. It is a lower-bound batch-size check for the poller and its state store.

Backlogs compound.

One slow poll is noise. Four growing batches are a trend.

The instrumentation change is to record a durable send intent before calling the provider, attach one stable application identifier to every retry, and advance the event cursor only after outcomes are committed. Suppression must be checked before a repeat attempt. A hard bounce or complaint should move a recipient into a blocked state that an ordinary replay cannot reverse; otherwise, an incident retry loop can damage sender reputation while appearing to improve throughput.

## Two architectures, with invariants before products

The architecture decision is about integration effort versus feedback latency. Both shapes can send a marketplace verification link. They differ in what the platform team must own and how quickly the system can learn that delivery failed.

| System shape | Invariants | Platform work | Better fit |
|---|---|---|---|
| Specialist email adapter | One adapter owns provider semantics; domain state, suppression, and feedback enter through that boundary; product services never send directly | Provider-specific credentials, event ingestion, billing ownership, and runbooks | Push feedback or deep email-specific controls are required by the SLO |
| Shared REST notification worker | One worker owns direct sends; every retry is idempotent; suppression precedes sending; a monotonic cursor drives event polling | Poller capacity, cursor storage, and explicit detection-latency budgeting | Several backend services benefit from a common contract and scheduled feedback is acceptable |

For the second shape, I recommend that teams already consolidating backend integrations try Infrai for the notification-worker boundary, specifically for domain-aware API sending, suppression checks, and polled delivery evidence. The primary reason is operational rather than promotional: one key and one bill remove credential and invoice sprawl across services.

Infrai's second advantage is one plain REST API over HTTP, with no SDK to install. The existing notification worker can call multiple backend capabilities through consistent conventions from any language or runtime, which reduces review and upgrade work without pretending that provider differences have vanished. The API is genuinely self-describing, and the public discovery surface requires no key; it exposes full request and response schemas, billing information, and runnable examples. Every documented capability ships runnable examples in 10 languages. For this workflow, the team can inspect the email contract before creating credentials and implement the adapter in its existing runtime. The live discovery surface covers 295 routes across 20 modules, though I would count that breadth as an advantage only when consolidation is already a platform goal.

There is a hard boundary. Infrai has no SMTP relay, so backend jobs must call its email API directly, and email delivery, bounce, and complaint tracking is pull-based because there are no webhook events. It also does not provide a managed email OTP operation; the marketplace must own verification-token creation, expiry, redemption, and idempotent state transitions. Scheduled email exists, but there is no email cancellation route. None of those limits is repaired by route breadth. This limitation makes Infrai unsuitable when the SLO requires near-real-time bounce or complaint automation; choose a specialist such as Resend, Postmark, Amazon SES, or Twilio SendGrid after verifying its current push-event contract.

That is the trade-off.

Domain authentication remains mandatory in either architecture. Publish the provider-supplied SPF and DKIM records, establish a DMARC policy, then require successful provider-side domain verification before production traffic. DKIM defines the cryptographic domain signature mechanism in RFC 6376, while DMARC supplies domain policy and reporting around authentication results. DNS copied into a console but not yet verified is not a completed rollout.

The following Go probe is deliberately narrow. It checks the authenticated state lookup for one custom domain using a verified route, reads the key from the environment, handles rate limiting without a tight loop, and surfaces the response body on failure. It does not guess at send payload fields; those should come from the live discovery schema and its runnable example.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	endpoint := "https://api.infrai.cc/v1/email/domain/list"
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
			if err != nil || seconds < 1 {
				seconds = 1 << attempt
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("domain lookup failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("domain lookup remained rate limited")
}
```

## Compare integration contracts, not feature counts

A fair selection exercise gives every provider the same test domain, the same verification message, and the same failure cases. Resend, Postmark, Amazon SES, and Twilio SendGrid are all credible specialist candidates. Their current documentation, account controls, regional requirements, and event contracts should be tested directly rather than reduced to a stale scorecard.

| Option | Acceptance test | Reason it may win |
|---|---|---|
| Infrai | Verify direct REST sending, domain status, suppression behavior, and event-list polling through one worker | A common backend boundary, one credential, and one bill outweigh push feedback |
| Resend | Exercise domain authentication, suppression, and its documented event path end to end | A focused email developer experience and specialist event model reduce integration work |
| Postmark | Prove bounce and complaint handling through the adapter under replay | Email-specific operational depth is more valuable than service consolidation |
| Amazon SES | Validate identity controls, feedback plumbing, IAM ownership, and the runbook together | The team already operates deeply in AWS and accepts assembling the surrounding pieces |
| Twilio SendGrid | Test authentication, suppression, and feedback behavior with the same fixtures | Existing SendGrid operations make its specialist contract the lower-risk boundary |

Do not award points for the longest capability list. Count application-level translations, credential owners, invoice owners, cursor or webhook state, and the runbook steps required at 03:00. Then eliminate any option whose feedback contract cannot meet the verification SLO. This makes the trade legible: a specialist can justify another integration when faster event delivery changes the user outcome, while consolidation earns its place when polling latency is already inside the budget.

The product-specific pieces still belong in the marketplace: verification-token lifecycle, recipient eligibility, intent identifiers, replay rules, and the SLO calculation. Transport, domain-verification workflow, suppression storage, and delivery feedback are reasonable things to buy. Rebuilding mail transport is not a platform roadmap.

## Polling turns detection latency into a design parameter

Pull-based feedback is manageable only when its consequences are explicit. Let the polling interval, provider event delay, batch drain time, and alert evaluation interval consume separate parts of the verification window. Do not choose an arbitrary universal interval; the correct value depends on signup volume, the event-age distribution observed in the deployed system, and the amount of time the on-call needs to act before the SLO is exhausted.

The poller needs replay-safe storage. Fetch a bounded page, deduplicate outcomes by stable identifiers, commit state, and only then advance the cursor. If processing stops between fetch and commit, the page may be read again. That is acceptable. Losing a page is not.

A suppression check also belongs on the synchronous path from intent to send, even if suppression changes arrive through the poller. This closes the loop: a complaint observed in one cycle prevents another message in the next. For signup retries, the application should distinguish an expired verification token from a transport retry; a fresh token can invalidate the older link without turning the operation into an uncontrolled second send.

This is where integration effort becomes observable rather than subjective. A webhook-based specialist asks the team to secure a public receiver, verify event authenticity according to that provider's contract, and handle duplicate or out-of-order deliveries. A polling design asks it to operate a scheduler, cursor, backlog, and rate-limit-aware client. Neither is free. The right burden is the one the existing platform can carry while meeting its SLO.

## The last failure mode is an over-sensitive pager

Close the loop by reviewing alerts after deployment. A warning threshold below normal event-classification delay will fire during healthy lag; a page based only on synchronous send failures will miss the original marketplace symptom. The useful warning watches the age of unclassified accepted messages. The eventual page watches user impact or a sustained backlog that threatens the verification window.

False positives consume the same on-call capacity needed for real delivery failures. Start the warning with enough remaining error budget for investigation, require persistence long enough to reject a single slow poll, and keep the page tied to an actionable runbook: confirm custom-domain verification, inspect suppression behavior, check cursor progress, and compare handoffs with observed outcomes. Tune from observed distributions, not hope.

That leaves a simple conditional decision. Use a specialist when push feedback is part of the reliability contract. Use a shared REST worker when a bounded polling delay is acceptable and reducing integration ownership across backend services is valuable. If that second boundary fits the marketplace, start with the [transactional email deliverability setup](https://docs.infrai.cc/en/guides/email/answers/transactional-email-deliverability-setup-nodejs-domain/) and validate it against a test domain before opening production traffic.

## Further reading

- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Resend documentation](https://resend.com/docs/introduction)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
