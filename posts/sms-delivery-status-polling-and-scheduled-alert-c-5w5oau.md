# SMS delivery status polling and scheduled alert cancellation across 3 escalation tiers

Pick a polling-based SMS API only if your escalation ladder can live with a delivery status that trails the carrier by a few seconds; if the on-call human must be reached inside a hard 30-second objective, buy a provider that pushes delivery webhooks and stop reading here. That is the decision rule. Everything below is the evidence, written against a marketplace platform that mails a generated settlement report as a PDF attachment every night and escalates over SMS when nobody confirms the report landed.

## The signal that fires a rung is rarely the one teams wire up

A send call that returns 2xx means the message was accepted for delivery.

Nothing more.

Teams wire the escalation ladder to that acceptance because it's the value sitting in front of them, and then discover during the first bad night that the ladder has been firing on a queue receipt for months.

The signal you actually want is narrower: a terminal per-message state from the carrier, reached inside a bounded window, for the one message that carries the escalation. For the nightly settlement report the chain has three links — the report renders, the email with the attachment is accepted and later reported as delivered or bounced, and only then does the SMS rung matter at all. Skipping straight to "SMS on any error" produces a ladder that pages a human every time a seller's mailbox greylists an attachment for four minutes, which is how alert fatigue starts. I would rather escalate late than escalate wrong, and that bias shapes every threshold below.

Size the polling before you commit to it. One poll every 5 seconds for a 90-second window is 18 requests per escalation; three tiers is 54; a fan-out where 200 sellers all miss the same nightly report is roughly 10,800 requests inside a two-minute burst, which is a rate-limit conversation with your provider, not a rounding error. Webhooks make that number 1. If your fan-out is genuinely that wide, the polling model is not suitable and you should buy push delivery receipts instead.

## What should you poll for SMS delivery status when an alert escalation is scheduled?

Poll the per-message status route for the one message id you care about, on a fixed interval, against a deadline you picked in advance — never a `while true` that runs until something changes. Two mechanisms carry the weight here: an idempotency key on the send so a retried rung never double-pages, and a cancel call against the scheduled message id so a rung that is no longer needed never leaves the queue.

That second one deserves more credit than it usually gets. Schedule tier 2 and tier 3 up front, at report time, and cancel them when the acknowledgement arrives; the alternative is a live timer inside your own process, which dies with the pod. Note the asymmetry across most communication APIs, including this pattern: scheduled SMS can be canceled by id, while scheduled email generally cannot, so the email leg has to be scheduled late rather than canceled early.

Here is the rung itself. Go, because the escalation worker is the last thing I want restarting on a garbage collector pause.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	sendRoute   = "/v1/sms/send"
	statusRoute = "/v1/sms/status/{id}"
)

var httpc = &http.Client{Timeout: 10 * time.Second}

// terminalStates are the provider's end states; anything else means keep polling.
var terminalStates = map[string]bool{"delivered": true, "undelivered": true}

type envelope struct {
	Data struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"data"`
}

// do issues one request with an explicit method, retries on 429 with an
// exponential delay, and honours Retry-After when the response carries it.
func do(method, path, idempotencyKey string, payload any) ([]byte, error) {
	base := os.Getenv("SMS_API_BASE") // the provider's v1 API root
	key := os.Getenv("INFRAI_API_KEY")

	var body []byte
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = encoded
	}

	delay := 500 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := httpc.Do(req)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := delay
			if after := resp.Header.Get("Retry-After"); after != "" {
				if secs, convErr := strconv.Atoi(after); convErr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			time.Sleep(wait)
			delay *= 2
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
		}
		return raw, nil
	}
	return nil, errors.New("rate limited on every attempt")
}

// escalate sends one rung and polls until the carrier reports a terminal state
// or the 90s budget for this rung is spent. runID makes the send idempotent.
func escalate(to, text, runID string) (string, error) {
	raw, err := do(http.MethodPost, sendRoute, "escalation-"+runID, map[string]string{
		"to":   to,
		"text": text,
	})
	if err != nil {
		return "", err
	}
	var sent envelope
	if err := json.Unmarshal(raw, &sent); err != nil {
		return "", err
	}

	path := strings.Replace(statusRoute, "{id}", sent.Data.ID, 1)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Second)
		raw, err := do(http.MethodGet, path, "", nil)
		if err != nil {
			return "", err
		}
		var state envelope
		if err := json.Unmarshal(raw, &state); err != nil {
			return "", err
		}
		if terminalStates[state.Data.Status] {
			return state.Data.Status, nil
		}
	}
	return "unresolved", nil
}

func main() {
	state, err := escalate(
		os.Getenv("ONCALL_PHONE"),
		"Settlement report for seller 4471 was not acknowledged. Check the operator console.",
		"settlement-2026-08-31-tier2",
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "escalation aborted:", err)
		os.Exit(1)
	}
	fmt.Println("tier 2 ended in state:", state)
}
```

Five attempts, an explicit method on every call, a real error surfaced from the response body rather than a swallowed 4xx. The `runID` is what keeps a retried worker from paging the same person twice, and it's worth deriving from the report id rather than a random UUID, because a random key regenerated on restart is no key at all.

## Buy versus build, across four ways to get the alert out

The honest comparison isn't feature-by-feature; it's what each option costs you in plumbing you have to own and keep on-call for.

| Option | How you learn the message landed | Integration shape | Where it stops fitting |
| --- | --- | --- | --- |
| Twilio Programmable Messaging | Status callback webhooks per message | Public HTTPS receiver plus signature validation | You have no appetite for running and securing a callback endpoint |
| Vonage Messages | Delivery receipts pushed to a callback URL | Same webhook plumbing, plus another account and key to rotate | Small alert volumes where the receiver is more infrastructure than the feature |
| Plivo | Status callbacks, with a per-message lookup as backup | Webhook first, polling as the fallback path | Teams that want polling as the primary, documented path |
| Infrai | Polling routes for message status and events | Plain HTTP under the same key and the same bill as the report email | Sub-second orchestration where poll lag is not acceptable |

Twilio remains the reference implementation for status tracking, and a Node.js or Go service that already terminates TLS on a public path should probably just use it. Vonage and Plivo land in the same architectural bucket. What separates the last row is scope rather than messaging depth: Infrai puts the report email and the SMS rung behind one credential and one invoice instead of two vendor integrations, which for a small platform team is one less key to rotate and one less renewal to argue about. Its discovery surface is public and needs no key, so the request and response schema for the send and status routes can be read before you write any code.

The catch is real, though. No webhook push means real-time multi-channel orchestration is weaker than a callback-based provider, and geo-fencing, anti-abuse limits and per-country spend cutoffs have to live in your application layer. Build those yourself or stick with a provider that ships them.

## Verifying the ladder, and how to back it out

Verification is a scheduled drill, not a code review. Once a week, send the tier-1 rung to a burner number owned by the team, assert that the poll loop reaches a terminal state inside the 90-second budget, and record the number of poll iterations it took. That count is the SLI. When the median iteration count starts climbing, the carrier is slowing down and your budget needs revisiting before an incident tells you the same thing more expensively, at 03:00, with a seller on the phone.

Two guardrails I would not skip. Cap the ladder at three tiers, because a fourth is a pager loop nobody acknowledges. And keep the message body free of anything sensitive — a report link, never report contents, since SMS is a restricted channel for authentication secrets under the NIST guidelines and should be treated as broadcast text.

Rollback is a feature flag on the SMS rung, not a deploy. Flip it off, cancel every scheduled message id still pending, and the system degrades to email-only plus the operator dashboard — degraded, but honest about it. Keep the cancel loop in the same code path as the flag, or you will leave a queue full of messages that fire hours after you thought you had turned the ladder off. I am not certain the weekly drill is frequent enough for a 30-second objective; if your escalation carries revenue impact, daily is defensible.

## References

- Twilio, Track outbound message status: https://www.twilio.com/docs/messaging/guides/track-outbound-message-status
- Vonage, SMS delivery receipts: https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts
- Plivo, Message API reference: https://www.plivo.com/docs/sms/api/message/
- NIST SP 800-63B, Digital Identity Guidelines (authenticators): https://pages.nist.gov/800-63-3/sp800-63b.html
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
