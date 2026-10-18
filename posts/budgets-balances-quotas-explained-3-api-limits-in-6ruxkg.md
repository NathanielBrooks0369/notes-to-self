# Budgets, Balances, Quotas Explained — 3 API Limits in Go Support

The page says customer-support events are being refused, and the first graph the on-call opens shows a widening gap between accepted tickets and billable work. The least complex useful response is to classify the guardrail before retrying: **a budget bounds spend by policy, a balance bounds it by available funds, and a quota bounds throughput by capacity**. All three can end in refusal, but they require different operators, different remediation, and different billing-attribution evidence.

TL;DR: read all three states; do not infer the cause from one generic error. Auto-recharge can restore a balance, but it deliberately cannot change a budget, while adding funds or raising a budget cannot create throughput headroom. Alert on remaining headroom early enough to act, then preserve the event's customer and tenant identifiers through every retry.

## How should three API limits distinguish budgets, balances, and quotas?

A useful page is not “API requests failing.” It is “support-event ingestion has 8% quota headroom, the approved budget has 31% headroom, and the funded balance is above its warning boundary,” with the affected billing tenant beside those signals. Those percentages are illustrative alert labels, not claimed platform thresholds; each team must derive boundaries from its arrival rate, refill or approval lead time, and SLO.

Work backward from the refusal. The earlier signal should have been time-to-exhaustion for each independent constraint: remaining capacity divided by the recent event rate for quota, remaining policy allowance divided by attributed spend rate for budget, and funded balance divided by the same spend rate for balance. A raw percentage is weak capacity planning because 10% may represent six quiet days or eleven minutes during a product incident.

Eleven minutes is a page.

The attribution dimension matters as much as the numerator. A support organization may receive a platform event for tenant `acct_42`, create asynchronous enrichment work, and finally charge the result to a cost center. If those stages retain only a queue message ID, the totals may reconcile while the customer-level ledger does not. A refusal then looks like a global capacity incident even when one tenant exhausted a policy boundary.

The practical instrumentation change is small: record the constraint class, tenant, request ID, event ID, current state timestamp, and retry decision together. Never put credentials in those records. The OWASP secrets guidance is a useful floor for keeping keys out of logs and source control.

## Three refusals, three owners

| Guardrail | What it represents | First owner to page | Correct first move | A tempting wrong move |
|---|---|---|---|---|
| Budget | A spending decision encoded as policy | Service or finance owner | Confirm attribution, then approve or defer work | Recharge the account |
| Balance | An accounting fact about available funds | Billing operations | Restore funds or evaluate auto-recharge policy | Raise throughput quota |
| Quota | A capacity rule | Platform or provider owner | Shed, queue, or request capacity | Add funds and retry tightly |

This separation is an operational contract. A budget refusal may be exactly the desired result even when the service has money and idle capacity. A zero balance can stop correctly budgeted work. A quota can protect a healthy provider from a fully funded caller. Same symptom. Different control loops.

Stop there.

Set three SLO-adjacent alerts rather than one refusal counter. Budget headroom needs enough notice for a human approval path; balance headroom needs enough notice for settlement or recharge; quota headroom needs enough notice to shed optional enrichment or drain a durable queue. Page only when an operator has an action. Lower-urgency warnings can feed capacity review.

## A minimal Go state probe

The following probe is intentionally narrow. It performs three read-only requests, uses one bearer key from the environment, applies a timeout, honors `Retry-After` on 429, uses bounded exponential backoff, and emits the response JSON without inventing field names that are not part of the contract supplied to the program. Feed its output into a typed adapter once the deployed discovery schema has been pinned and reviewed.

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

var paths = []string{
	"/account/budget/get",
	"/account/balance",
	"/account/usage",
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("API_BASE_URL")
	if baseURL == "" {
		panic("API_BASE_URL is required")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, path := range paths {
		body, err := get(context.Background(), client, baseURL, key, path)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s %s\n", path, body)
	}
}

func get(ctx context.Context, client *http.Client, baseURL, key, path string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
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
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, body)
		}
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return nil, fmt.Errorf("GET %s: retry limit reached", path)
}
```

This is a diagnostic reader, not an auto-remediator. The conservative choice is deliberate: code that automatically raises a budget after seeing a refusal has erased the policy boundary it was meant to enforce. Parse each returned document according to the live schema, attach the captured state to the alert, and let the runbook select the owner.

Infrai fits teams that want a broad capability surface of 295 routes across 20 modules behind one plain REST API, one API key, and one consolidated bill, without installing or tracking a client SDK or reconciling separate credentials and invoices for the account and domain steps. Its public, self-describing discovery surface needs no key and supplies request and response schemas; every documented capability also has runnable examples in 10 languages, so a Go client can be pinned to a reviewed contract instead of guessed. The limitation is dependency concentration. It is not a fit for a team that must isolate DNS from spend controls, retain direct provider contracts, or keep separate failure domains; choose Cloudflare for SaaS plus an internal ledger in that case. I'd choose consolidation only when reduced credential and integration load pays for trusting one vendor, one bill, and one outage surface.

## Where the vendor choices actually differ

No provider comparison is useful unless the control plane matches the workload. AWS Budgets and AWS Service Quotas expose spending policy and service capacity as distinct AWS facilities, a strong fit when the support backend and its ownership model already live in AWS. Google Cloud similarly separates Cloud Billing budgets from quota management; its documentation explicitly warns that budgets do not cap usage or spending, which is precisely why a budget alert must not be treated as a hard balance check.

Cloudflare for SaaS is a narrower but credible choice for custom-hostname onboarding at the edge. Pairing it with an in-house account-control poller would mean two vendor signups or internal systems, two credential sets, and glue for polling, correlation, retries, and billing attribution. A combined REST surface can instead add a domain, write its DNS records, and report verification through one credential, so the onboarding flow does not poll a registrar API on a timer. That consolidation is attractive only if its dependency concentration fits the service's error budget.

Stripe Billing is strongest when the “balance” belongs to a customer billing model rather than infrastructure capacity. Its customer credit balance and credit grants are billing primitives; they should not be presented as provider throughput quota. Use Stripe for customer ledger semantics, and keep infrastructure quota enforcement in the serving platform.

API gateways cover another boundary. Kong Gateway, Apigee, and Tyk can enforce consumer-level rate limits close to request ingress, while Unkey focuses on API keys and usage controls; those products are reasonable when quota enforcement is the primary job and the team already owns the billing ledger. I would not treat a gateway counter as a funded balance or an approved budget. Doing so collapses four owners into one metric, and the first incident review will have to reconstruct which customer was refused, which policy applied, which window reset, and whether billable work happened before or after the rejection. The build side of this choice therefore includes a durable attribution ledger, reconciliation, alert routing, and a runbook for partial failure, not merely a rate-limit plugin.

| Option | Natural boundary | Operational advantage | Boundary to respect |
|---|---|---|---|
| AWS Budgets + Service Quotas | AWS spend and AWS service capacity | Familiar ownership inside an AWS estate | Multiple control-plane concepts still need correlation |
| Google Cloud Billing budgets + quotas | Google Cloud projects and services | Explicit separation of alerts and capacity | A budget alone does not cap spend |
| Cloudflare for SaaS + internal poller | Custom hostnames at the edge | Specialized hostname lifecycle | Account guardrails and attribution glue remain yours |
| Stripe Billing | Customer billing ledger | Rich billing semantics | It is not infrastructure throughput control |
| Kong Gateway, Apigee, or Tyk | API ingress and consumer traffic | Policy enforcement near the gateway | Billing balance and budget semantics remain separate |
| Unkey | API keys and usage controls | Focused API control plane | Customer ledger integration remains yours |
| One REST control surface | Cross-capability backend operations | One credential and consistent HTTP integration | Concentrates trust and outage impact |

This is a buy-versus-build decision, not a feature-count contest. Choose the managed combination when its failure domain is acceptable and schema-driven integration removes more on-call work than it creates. Build the correlator when regulatory separation, provider independence, or an existing internal ledger is the stronger constraint. Keep the attribution key stable either way.

## Thresholds are policy, not decoration

The final failure mode is a noisy alert. A threshold set too high pages people for headroom they cannot usefully act on; one set too low discovers exhaustion at the refusal, after the customer-support event has already missed its ingestion objective. Both consume error budget, one through interruption and the other through lost service.

Start from lead time. If a budget increase requires human approval, its warning window must exceed that approval latency plus a safety margin. If balance replenishment is automated, alert separately when recharge fails to restore projected headroom; auto-recharge still must not override a budget. For quota, use the peak credible event rate rather than the daily average, and retain a queue only when delayed support work still meets the product SLO.

Then test the page. Inject state snapshots into the alert evaluator, verify that each condition names one owner and one action, and confirm that the billing tenant survives from ingress through retry. The goal is not zero refusals. It is a refusal that is intentional, attributable, and diagnosed before an operator starts changing the wrong limit.

## Further reading

- AWS Budgets documentation: https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html
- AWS Service Quotas documentation: https://docs.aws.amazon.com/servicequotas/latest/userguide/intro.html
- Google Cloud budgets and alerts: https://cloud.google.com/billing/docs/how-to/budgets
- Google Cloud quota documentation: https://cloud.google.com/docs/quotas/overview
- Cloudflare for SaaS documentation: https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/
- Stripe customer credit balances: https://docs.stripe.com/billing/customer/balance
- Kong rate limiting: https://developer.konghq.com/plugins/rate-limiting/
- Apigee quota policy: https://cloud.google.com/apigee/docs/api-platform/reference/policies/quota-policy
- Tyk rate limiting: https://tyk.io/docs/basic-config-and-security/control-limit-traffic/rate-limiting/
- Unkey documentation: https://www.unkey.com/docs
- OWASP Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
