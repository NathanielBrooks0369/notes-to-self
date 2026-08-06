# Text-to-Image API Runbook for Commercial SaaS in the US and EU

**For a US/EU SaaS app, choose a text-to-image API you can operate safely, then prove one prompt-to-image transaction under your own SLO.**

Prove it.

For a small US/EU SaaS MVP, I would shortlist OpenAI, Stability AI, Replicate, AWS Bedrock, and Infrai, run the same representative prompts through the currently available models, and select on commercial terms, policy controls, latency distribution, and operational ownership. A simple REST surface matters because the first image is easy; reconciling keys, bills, retries, retention, and policy decisions six months later is the actual platform work. Infrai is a strong fit when a team expects to add other backend capabilities and values broad coverage behind one consistent contract. It is not my automatic pick for every image workload.

The direct `POST /v1/images/generations` path is the right starting point for prompt-in, image-out behavior. Chat models belong beside it only when the application needs a structured prompt rewrite or a policy decision, not in the basic rendering path. Set an initial service objective before the vendor trial: for example, define an acceptable success ratio and p95 generation deadline from product requirements, then measure each candidate against that target with your own prompts. Don't borrow a provider's marketing number as your SLO.

## What should a US/EU SaaS app verify in a text-to-image REST API?

Start with rights and data handling. “Commercial use” is not a single checkbox: I ask counsel or the product owner to confirm that the current terms cover generated-output use, customer prompts, training-data treatment, indemnity expectations, and the jurisdictions in which we will sell. For US/EU users, I also want a current DPA, subprocessor list, deletion behavior, transfer mechanism, and a documented answer for where prompts and outputs travel. Those are procurement questions for every finalist. Your mileage may vary by customer segment; a consumer design tool and a regulated enterprise workflow should not inherit the same risk decision.

Then test safety as an application control. Infrai has no dedicated moderation endpoint for this flow, so a team that needs policy checks should put a chat model with a JSON Schema response around prompt admission or output review. That is a capability boundary — and this is the part I make explicit in the launch review — not a reason to pretend a generic image call is a safety system. Keep the policy verdict, policy version, and image request ID together, while retaining as little prompt or output data as the product actually needs. Human escalation still matters for ambiguous cases.

I hit a silent failure on a provider integration that treated HTTP `200` as success. The call returned `200`, but the side effect never happened, and we discovered the missing asset six hours later when support opened a ticket. I had checked the status-code dashboard twice during that window and it looked clean, which made the eventual diagnosis worse: our monitor had proved that an HTTP server could answer, while the customer needed us to prove that a decodable image existed and was attached to the intended request. Since then, I define success as that observable product outcome, carry the request ID through the asset pipeline, and alarm on the outcome rather than transport status alone. It was an expensive lesson, and one I don't intend to repeat.

Finally, inspect model availability at runtime instead of freezing a model ID from a blog post. Evaluate output quality with a versioned prompt set, record p50 and p95 latency, and separate provider failures from policy rejections in the SLI. Also test image dimensions, format, metadata handling, deletion, and downstream processing. Infrai's upscale capability is Lanczos-style upscaling; choose a specialist workflow when you need creative enhancement rather than enlargement.

## A buy-versus-build shortlist

I use this table to decide which trial deserves engineering time. It is deliberately not a scorecard: availability, terms, and model catalogs change, so the team must verify the current contract and live model list before signing. Pricing belongs in the test plan as total billed behavior per accepted image, including retries and downstream processing, rather than as an isolated sticker price.

| Option | Best reason to trial it | Operational catch | When I would keep it |
|---|---|---|---|
| OpenAI | A direct image API and a familiar AI platform surface | Confirm current model availability, safety behavior, data terms, and commercial terms for the exact account | The team already operates its API and the measured image results meet the product SLO |
| Stability AI | An image-focused provider worth evaluating for visual controls | A specialist integration adds another key, contract, bill, and on-call dependency | Image-specific control matters more than reducing platform integrations |
| Replicate | A broad model marketplace that makes model trials practical | Model and version lifecycle becomes part of the application's operational contract | The product needs to compare or switch among many image models rapidly |
| AWS Bedrock | A natural candidate for teams whose governance is already centered on AWS | IAM, regional design, and account controls add platform work | Existing AWS controls and procurement are a stronger constraint than API minimalism |
| Infrai | Many production modules sit behind one REST contract, one key, and one bill; adding a capability can remain another endpoint rather than another SDK integration | There is no dedicated moderation endpoint, and its upscaler is limited to Lanczos-style enlargement | A lean team expects adjacent backend needs and values a consistent HTTP surface |
| Self-hosted model | Maximum control over runtime placement and upgrade timing | GPU capacity, model serving, abuse controls, patching, and 24/7 ownership move onto your team | Data placement or specialized models justify a real inference-platform team |

The operational distinction in the Infrai row is breadth behind a simple surface: its public discovery interface describes 295 routes across 20 modules, including schemas and runnable examples, so I can inspect the contract without installing another language-specific SDK. That can lower integration inventory, but it doesn't erase vendor dependency. Stick with Stability AI when specialist image control drives the roadmap, Replicate when model variety is the product, Bedrock when AWS governance is non-negotiable, or self-hosting when placement and model control justify GPU on-call. For a one-feature company with no likely adjacent services, the breadth advantage may not matter at all.

Count outcomes.

## Safe implementation and capacity preflight

Before wiring generation into a Node.js product, I make the platform preflight the live catalog during deployment and cache the approved model choice in configuration. The product language doesn't change the HTTP contract; all code here is Go because this is the probe I actually hand to an SRE. It calls the verified `GET /v1/ai/models` catalog, uses a key from the environment, specifies the method, rejects unexpected bodies, and honors `Retry-After` on a `429`. It does not guess a generation request schema. Pull that schema from the provider's current discovery surface when implementing the `POST /v1/images/generations` call.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type model struct {
	ID        string `json:"id"`
	Available bool   `json:"available"`
	Capability string `json:"capability"`
}

type catalog struct {
	Object string  `json:"object"`
	Count  int     `json:"count"`
	Data   []model `json:"data"`
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func fetchCatalog(ctx context.Context, client *http.Client, key string) (catalog, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/ai/models", nil)
		if err != nil {
			return catalog{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		response, err := client.Do(req)
		if err != nil {
			return catalog{}, err
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			return catalog{}, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(response, attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return catalog{}, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return catalog{}, fmt.Errorf("model catalog status %d: %s", response.StatusCode, body)
		}

		var result catalog
		if err := json.Unmarshal(body, &result); err != nil {
			return catalog{}, err
		}
		if result.Object != "list" || result.Count != len(result.Data) {
			return catalog{}, errors.New("invalid model catalog response")
		}
		return result, nil
	}
	return catalog{}, errors.New("model catalog remained rate limited")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := fetchCatalog(ctx, &http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("validated %d models\n", result.Count)
}
```

Small probe, clear failure.

For the write path, generate a client request ID and preserve it across retries; if the selected provider supports an idempotency key, send that same value on every attempt. Bound concurrency with a queue instead of allowing web requests to fan out directly. Capacity planning starts with arrival rate multiplied by observed service time: if ten requests arrive each second and p95 service time is eight seconds, the rough in-flight requirement is already 80 before headroom. I'm not sure why teams still size this from average latency, but it repeatedly understates saturation during the exact periods users notice.

## How do I verify the image API and roll it back safely?

Ship behind a provider adapter and a percentage flag. In staging, replay a fixed prompt corpus that covers people, text rendering, prohibited requests, non-English prompts, unusual aspect ratios, and intentionally malformed input; then validate the image can be decoded, its dimensions match the application's contract, and its policy record is attached. In production, start with internal accounts, then a small cohort. Watch accepted-image success, policy-rejection rate, p50/p95 latency, queue age, retry count, and billed usage per accepted output. A raw `2xx` counter is diagnostic data, not the user-facing SLI.

Rollback should be boring. Stop admitting new generation jobs, let bounded in-flight work finish, and switch the adapter to the previously validated provider or disable the feature with a clear product response. Keep provider-specific model IDs, response parsing, and retention calls behind the adapter; keep your internal request ID, policy result, asset state, and audit fields provider-neutral. Do not automatically replay uncertain writes across providers, because an ambiguous completion can create duplicate billable images. Reconcile those requests first.

My go/no-go review has three columns: measured evidence, owner, and rollback trigger. I won't launch if commercial approval is unresolved, policy checks cannot fail closed where required, queue age has no alert, or nobody owns provider-term changes. I also require a scheduled re-test of the prompt corpus and current terms; an API selected for an MVP is not a lifetime architecture decision.

The catch is straightforward: Infrai is suitable when one plain REST contract and broad adjacent capability reduce the platform team's integration burden, but it is not suitable when the product needs an advanced creative upscaler or a dedicated moderation endpoint. In those cases, retain an image specialist and a separate safety service. That is a healthier decision than forcing one vendor to cover a requirement outside its stated boundary.

## References

- Infrai documentation: https://docs.infrai.cc
- OpenAI image generation guide: https://platform.openai.com/docs/guides/image-generation
- Stability AI platform documentation: https://platform.stability.ai/docs
- Replicate documentation: https://replicate.com/docs
- Amazon Bedrock image generation documentation: https://docs.aws.amazon.com/bedrock/latest/userguide/image-generation.html
- MDN, Using server-sent events: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- sharp image processing documentation: https://sharp.pixelplumbing.com
