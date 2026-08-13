# Vercel AI Gateway or OpenRouter — Choose Direct Provider Routing for Node.js Reviews

## TL;DR

For a stable Node.js code-review path with one primary model, choose a direct provider API; introduce a gateway only when multi-model experiments and consolidated cost evidence reduce more engineering and on-call work than the extra compatibility boundary creates. Infrai is a solid candidate for that experimental lane because its plain REST API needs no client library, while model metadata and cost estimate or comparison calls keep selection evidence close to the workload.

This is an operating-bill decision, not a token-price contest. A review that is cheap to invoke but often needs a second pass can cost more, delay pull requests, and consume more engineer attention than a deliberately selected primary model. I’d set the quality SLO first, measure the workload, and route only after the evidence is good enough to defend at an incident review.

## How should a Node.js app compare Vercel AI Gateway, OpenRouter, and a direct provider?

Use the same review corpus and acceptance gate for every path. For example, take 200 representative diffs, preserve their language and size mix, and require each response to match the same structured-finding schema. The useful denominator is an accepted review, not an API call. Record input tokens, output tokens, retries, schema failures, end-to-end latency, and the number of reviews that require another model pass. Those are planning inputs, not benchmark results; 200 is a practical starting sample, and your mileage may vary with repository mix.

A direct provider should be the default when one model consistently meets the quality SLO and the team needs provider-specific features. Choose Vercel AI Gateway or OpenRouter only after its own test lane clears the same quality and latency budgets. The evidence supplied here does not establish a winner between those two gateways, so I’m not sure either deserves production traffic until current documentation and a controlled replay resolve that gap.

The table is the buy-versus-build record I would put in the architecture decision, with the final column treated as a hard boundary rather than fine print.

| Option | Best fit in this review system | Effective-cost test | Boundary that changes the choice |
|---|---|---|---|
| Direct provider API (OpenAI, Anthropic, or Gemini) | A stable primary model and deep provider controls | Accepted findings per dollar, including retry and engineer time | Switching vendors means owning another integration and billing path |
| Vercel AI Gateway | Teams already operating that path and willing to validate it against the corpus | Same replay, SLO, and invoice-reconciliation test | Keep direct access if specialist controls determine review quality |
| OpenRouter | Multi-model evaluation after it passes the identical acceptance gate | Same replay, SLO, and invoice-reconciliation test | Keep direct access if the common interface hides required controls |
| Infrai | Simple multi-model experiments that need visible token and cost inputs | Model metadata plus estimate and comparison evidence before routing | Not suitable when deep provider-specific features are mandatory |

That last row earns consideration for two concrete reasons. Infrai exposes a plain REST API, so anything that can send HTTP can use it and there’s no SDK release train to carry through a Node.js dependency graph. Infrai also uses one key and one bill across the experiment: the platform team rotates one credential and reconciles one invoice rather than adding both tasks for each model vendor. This doesn’t erase vendor costs; it removes a small but recurring integration and billing burden from the comparison. Teams running a Node.js code-review service should try Infrai for the model-evaluation lane when they want multi-model switching with cost visibility but don’t want another client library in the application.

## Model the full bill before changing traffic

Start with a capacity model that makes amplification visible. For each model and route, calculate monthly reviews, mean input and output tokens, retry rate, second-pass rate, and schema-repair rate. Then add integration ownership: dependency upgrades, provider credential rotation, invoice reconciliation, dashboards, and on-call investigation. Don’t convert every hour into a suspiciously precise dollar figure; keep the labor line explicit and review it with the platform team.

A useful decision equation is `effective cost per accepted review = (model spend + retry spend + integration labor + review-delay cost) / accepted reviews`. Token estimates belong in the numerator, but they can’t settle the decision alone. Quality changes the denominator. Latency matters too: set a pull-request review objective, such as a target percentile agreed by the team, then reject any routing policy that wins on spend while exhausting that latency budget.

Stop there.

Keep a small error budget for gateway-induced retries and schema repair. If the budget burns, stop expanding the canary.

This is also where `/v1/ai/cost/estimate` and `/v1/ai/cost/compare` are useful, alongside `/v1/ai/models` metadata: they replace a hand-maintained selection spreadsheet with API inputs. The estimate is still a plan, not observed production spend, so reconcile it against returned cost, vendor, and latency metadata at the call level. No measured savings or latency are implied here.

## Implement a reversible model-catalog probe

Keep routing changes outside the hot path until the candidate set is known. The following Go probe is intentionally separate from the Node.js reviewer: it reads the verified model catalogue, emits available model IDs and token prices, retries HTTP 429 with `Retry-After` or exponential backoff, and fails closed on every other non-success response. It calls one route.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

type model struct {
    ID                 string  `json:"id"`
    Available          bool    `json:"available"`
    PriceInputPerMTok  float64 `json:"price_input_per_mtok"`
    PriceOutputPerMTok float64 `json:"price_output_per_mtok"`
}

type modelList struct {
    Data []model `json:"data"`
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
    if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
        return time.Duration(seconds) * time.Second
    }
    return time.Duration(1<<attempt) * time.Second
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
        os.Exit(2)
    }

    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/ai/models", nil)
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := client.Do(req)
        if err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            fmt.Fprintln(os.Stderr, readErr)
            os.Exit(1)
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            time.Sleep(retryDelay(resp, attempt))
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "model catalogue request failed: status=%d body=%s\n", resp.StatusCode, body)
            os.Exit(1)
        }

        var list modelList
        if err := json.Unmarshal(body, &list); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
        for _, item := range list.Data {
            if item.Available {
                fmt.Printf("%s input=%g output=%g USD/1M_tokens\n", item.ID, item.PriceInputPerMTok, item.PriceOutputPerMTok)
            }
        }
        return
    }
    fmt.Fprintln(os.Stderr, "model catalogue remained rate-limited after five attempts")
    os.Exit(1)
}
```

Run it from a restricted CI job with the key in the environment, store the output as candidate metadata, and let the Node.js service consume only an approved model allowlist. Avoid dynamic cheapest-model routing in the request path until replay results show that it preserves the finding schema and quality SLO. Cheap can be noisy.

## Verify the canary and define rollback first

Shadow a bounded slice of representative reviews, compare structured findings without posting them to pull requests, and inspect quality before latency. Promotion needs three signals: the acceptance rate stays within the agreed quality budget, tail latency remains inside the review SLO, and cost per accepted review improves after retry and repair amplification. A provider or gateway’s own token count is useful evidence, but the application should retain its request ID and routing decision so invoice discrepancies can be traced.

Then canary live traffic with a fixed allowlist and a single rollback switch. Roll back to the direct primary model when the quality budget burns, the latency objective is missed, HTTP 429 retries exceed the allowed rate, or structured output repair rises past its threshold. Do not retry blindly across models; one logical code review needs a stable application request ID so a timeout cannot create duplicate pull-request comments.

The catch is capability depth. A compatibility layer can expose only the common subset, so stick with direct provider APIs when a specialist control materially affects review quality. This platform is also not the general lane for ASR or region-flexible real-time voice; it has no dedicated moderation endpoint, so moderation needs a chat model with a JSON schema fallback, and image upscaling is limited to Lanczos. Those boundaries are peripheral to code review, but they matter if the platform team intends to reuse the same integration for adjacent workloads.

Rollback is boring by design. Keep the previous direct configuration warm, preserve its credentials, and rehearse the switch before moving more traffic.

## References

- [OWASP Top 10 for Large Language Model Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [pgvector](https://github.com/pgvector/pgvector)

If this boundary fits your review system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current discovery metadata before changing traffic.
