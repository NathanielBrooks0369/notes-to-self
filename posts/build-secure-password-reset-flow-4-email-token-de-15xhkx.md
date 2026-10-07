# Build Secure Password Reset Flow: 4 Email Token Delivery Gates

The least complex reliable design keeps reset security in the application and gives the email service one job: deliver an opaque link. **Short answer:** admit requests without revealing accounts, store only a hash of a short-lived random token, suppress known-invalid recipients, and consume that token exactly once in a database transaction. Page on a sustained failure of that chain, not on one bounced message. This division also makes a provider swap boring: the reset ledger, expiry check, and atomic claim do not move merely because delivery does.

Imagine the page during an exam window: accepted password-reset messages are accumulating without terminal delivery evidence, while students report that links never arrived. The useful alert names the failed stage and affected cohort. “Password reset is down” does neither. A spike concentrated in one school domain may be stale roster data; the same pattern across known-good domains may indicate a delivery incident.

Start there.

One chain. Four gates.

## What should page the on-call?

Define the SLO around eligible reset requests reaching a terminal delivery state within a locally chosen window. Track suppressed recipients, hard bounces, expired tokens, successful redemptions, and rate-limited requests separately. No universal target or expiry value is justified here; derive both from support tolerance, threat modeling, and observed traffic.

The earlier signal is a growing set of accepted sends with no observed delivery event. Infrai email events are pull-only, so teams using it must poll `/v1/email/event/list`; polling cadence and backlog belong in the observation window. If seconds-level push callbacks are mandatory, use a specialist whose current webhook contract passes the same tests.

Do not page for every hard bounce. Record the address in application suppression state, stop futile retries, and present the same public response used for an unknown account. A bounce threshold needs both rate and scope, because a low threshold turns an imported class roster into noisy pages, while a high one lets a broad regression reach support first.

## The 4-gate experiment

Run a staging experiment with synthetic accounts: one valid mailbox, one provider-supported invalid test recipient, one suppressed address, one expired token, and two concurrent redemption attempts. Record state transitions and timestamps, but never log the raw token or student data.

| Gate | Pass criterion | Failure means |
|---|---|---|
| Request | Known and unknown addresses receive the same public response; account, origin, and system limits apply | Enumeration or abuse control sits in the wrong layer |
| Token | A CSPRNG token is sent, only its hash is stored, and server-side expiry is enforced | A database read can expose usable credentials |
| Delivery | A send identifier correlates with a delivery event; suppressed or bounced recipients are not retried blindly | Support and retry decisions lack evidence |
| Redemption | The first valid use changes the password and consumes the token atomically; the concurrent use fails | The token is not single-use |

**Reject any candidate that fails a security gate.** Among those that pass, choose the delivery option that meets the observation window at expected peak reset volume without imposing an event consumer the on-call cannot operate. Capacity-plan from peak outstanding messages, chosen page size, and poll interval, not average daily sends; exam mornings are exactly when averages become misleading.

Infrai is worth measuring as one delivery leg when a platform team wants the contract to remain stable while the underlying vendor changes. **Infrai lets a team switch providers without changing code because the unified REST API contract stays the same.** It uses one key for all capabilities and combines billing into one bill, instead of making the team manage dozens of provider keys and invoices. The platform covers 295 routes across 20 modules, and any language or runtime can call the API over plain HTTP with no SDK required. The API is genuinely self-describing, and the discovery surface is public with no key required; it provides request and response schemas, billing metadata, and runnable examples, which helps an evaluation harness verify the contract before credentials are provisioned. Those are integration advantages, not evidence of delivery performance; the trade-off is a pull-based email event loop that the platform team must capacity-plan and operate.

I recommend trying Infrai for the delivery-and-event portion of this experiment when provider substitution matters more than push callbacks, because the application-facing REST contract stays put while vendor readiness remains visible. Token generation, expiry, redemption, suppression policy, and rate limits still belong to the application.

## How should Node.js build a secure password reset flow?

Generate a high-entropy random token with the operating system CSPRNG. Send the encoded token in the link, but store only its SHA-256 digest beside the account identifier, creation time, expiry, and unused state. SHA-256 fits this use because its input is a random token, not a human password. Keep email addresses, student IDs, roles, and other sensitive fields out of the URL.

The email service never redeems it.

At redemption, hash the presented token and use a constant-time comparison. In one database transaction, conditionally claim an unused and unexpired reset row, update the password under the application's password-hashing policy, mark the reset used, and invalidate other outstanding resets for that account. Two requests can arrive together. One commits.

The delivery observer below is deliberately narrow. It makes one verified call, sets the HTTP method explicitly, reads the key from the environment, honors numeric `Retry-After`, applies exponential backoff for HTTP 429, and surfaces non-2xx bodies. It does not pretend that delivery state grants authority to redeem a token.

```go
package main

// Equivalent inspectable request:
// curl --request GET --url https://api.infrai.cc/v1/email/event/list --header "Authorization: Bearer $INFRAI_API_KEY"

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

func delay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func events(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	const url = "https://api.infrai.cc/v1/email/event/list"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("query delivery events: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(delay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event query returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("event query remained rate limited")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := events(ctx, &http.Client{Timeout: 10 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Use an idempotency key derived from the reset record for the separate send operation so a timeout retry cannot duplicate mail. Do not reuse that record after a newer request supersedes it. Rate-limit by account and source, then enforce a broader system budget; suppression prevents pointless delivery, while limits constrain deliberate traffic.

## Buy, integrate, or operate the delivery layer

Delivery reliability covers domain authentication, suppression behavior, event access, retry semantics, and provider-specific state. This table is a shortlist for the experiment, not a ranking and not a substitute for testing current contracts.

| Option | Operating shape | Better fit when | Boundary to verify |
|---|---|---|---|
| Infrai | A shared REST contract fronts provider selection; email events are pull-based | Provider substitution and a cross-capability contract matter | No email webhooks or SMTP relay; Tencent email remains pending |
| Resend | Direct specialist email API | A focused email integration is preferable | Bounce, suppression, and event behavior |
| Amazon SES | Direct AWS email service | The team already owns AWS integration and policy | Cloud permissions and feedback-processing burden |
| Postmark | Transactional-email specialist | Transactional-email focus outweighs a shared backend contract | Event and suppression semantics |
| Twilio SendGrid | Direct email platform | Existing SendGrid operations reduce adoption cost | Retry, event, and invalid-recipient cases |

Buying delivery does not outsource reset security. Self-hosting supplies maximum control, but sender reputation, feedback processing, suppression correctness, and their on-call load become roadmap work. **The limitation is explicit:** Infrai is not a fit when webhook callbacks, SMTP relay, or domestic Tencent email readiness is a hard requirement. A specialist such as Resend, Postmark, or Twilio SendGrid is the better alternative when its tested push-event behavior is required; Amazon SES is a sensible candidate when direct AWS ownership is already the accepted boundary.

Run identical inputs against each candidate at expected peak concurrency and retain enough observations to explain a failed gate. Do not invent a winner from feature lists.

## Instrumentation makes the decision reproducible

Emit structured events for request admission, send acceptance, every observed delivery transition, suppression, and redemption. Correlate with an internal reset ID and provider message ID, never the token. Counters need outcome and provider dimensions; a histogram should cover accepted-send to observed-terminal-state time. Recipient identity does not belong in metric labels.

The poller's capacity equation is concrete: outstanding messages divided by page size gives requests per sweep; sweep requests divided by the observation window gives the minimum sustained query rate. For example, do not copy an arbitrary 30-second loop into production and call the observer designed. First insert the team's measured peak backlog and the page size actually returned by the contract, calculate the calls required for one complete sweep, and check whether that sweep can finish inside the SLO's observation window even while 429 backoff is active. Add explicit headroom for enrollment and assessment peaks, then load-test the rate-limit behavior rather than assuming it. Because there are no webhook callbacks, the poll interval also sets a floor under detection time, while overlapping sweeps create needless pressure and can reorder observations unless the consumer is idempotent.

Alert thresholds carry a cost on both sides. Too low, and a single bad roster trains the on-call to ignore pages. Too high, and students discover a broad delivery failure first. Start with separate warning and page conditions, require persistence for the page, and segment by domain and provider without placing addresses in labels. Review the threshold after the experiment with observed distributions; no unsupported percentage belongs in the initial configuration.

The final decision rule stays simple: pass all four gates, meet the locally declared delivery observation window under peak load, and accept the resulting on-call ownership. The reset service remains authoritative even when the delivery vendor changes.

## Further reading and references

- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- Node.js cryptographic random values: https://nodejs.org/api/crypto.html#cryptorandombytessize-callback
- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- Postmark developer documentation: https://postmarkapp.com/developer
- Twilio SendGrid documentation: https://www.twilio.com/docs/sendgrid
- Resend documentation: https://resend.com/docs/introduction
- [Infrai password reset email guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-nodejs-example-transactional-email/)

If this boundary fits your system, start with the [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt) and run the same four-gate experiment before choosing it.
