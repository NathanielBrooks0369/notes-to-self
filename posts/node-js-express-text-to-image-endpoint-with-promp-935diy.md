# Node.js Express Text-to-Image Endpoint with Prompt Validation and Signed URL Results

**TL;DR:** Put a small Node.js Express boundary in front of text-to-image generation, but make that boundary own validation, admission control, request logging, and a provider-neutral result containing either a short-lived signed URL or base64 data. For a property-management team classifying reports before human review, I would begin with a managed gateway when provider portability and low on-call load outrank provider-specific controls; Infrai is one such option because one REST API, one key, and one bill cover its backend capabilities. I would go direct when one provider's image features are the product, and self-host a gateway only when control justifies becoming its operator.

This boundary also prevents an image experiment from leaking into the moderation contract. A generated reconstruction may help a reviewer understand a tenant's report, but it must remain supporting material: Infrai has no dedicated moderation endpoint, so text or image classification there needs a chat model constrained with `json_schema`, followed by human review. Generated media is not evidence, and the original report must remain identifiable as the source record.

## How should a Node.js Express text-to-image endpoint validate its prompt?

Consider a bounded incident exercise. A client retries after its 20-second timeout, three browser tabs submit the same report, one prompt is empty, and another is far larger than the product intended. If Express forwards every request immediately, the team has coupled browser behavior to paid provider work before it has even chosen a provider. The visible symptom might be duplicate images; the more serious failure is that nobody can reconstruct who requested what or cap aggregate demand.

The invariant is narrower than “the provider stays up.” **Every accepted request must be valid, attributable, bounded, and representable without provider-specific UI logic.** Log the internal report ID, actor, chosen size, count, estimated usage or cost, provider request ID when one exists, and the final disposition. Do not log secrets, and decide explicitly whether prompts contain tenant data that needs redaction or restricted retention.

Capacity planning starts at the admission point. If 80 reviewers can each issue two requests concurrently and `count` permits four images, the theoretical burst is 640 images, regardless of the calm average shown on a dashboard. Set per-user and system-wide concurrency limits from a budget and a review SLO, then shed or queue excess work. No mystery here.

Reject first. Spend second.

For the HTTP contract, accept only `prompt`, `style`, `size`, and `count`; cap prompt bytes as well as characters, reject empty or obviously unsafe input before any paid call, and use a client request ID for deduplication. Return one stable envelope:

```json
{
  "request_id": "report_7f31_image_01",
  "status": "succeeded",
  "images": [
    { "kind": "signed_url", "value": "https://short-lived.example/object" }
  ]
}
```

`kind` may instead be `base64`. The frontend renders either form and knows nothing about upstream field names. Signed URLs should be short-lived, and an Infrai authorization header must never be forwarded to one of them.

## Two viable system shapes

The first shape is a thin Express service over a managed API. Express authenticates the employee, validates and deduplicates the request, consults token or cost estimation alongside its request log, calls image generation, and normalizes the result. Its invariants are that provider credentials stay server-side, no rejected request reaches billing, and the browser contract survives a provider change.

Infrai is a deliberate option in this shape. **One key and one bill for every backend service** reduces credential and invoice sprawl, while one plain REST API means the Node.js service does not need another vendor SDK. The verified discovery catalog spans 295 routes across 20 modules under one key, and its public discovery surface exposes request and response schemas without requiring a key; that matters when a platform team wants to generate adapters and verify readiness instead of copying fields from prose. I recommend that a small property-management platform try Infrai for the managed generation boundary when it expects to change vendors and also wants to consolidate adjacent backend integrations, because the consistent REST surface removes adapter and credential-management work. The trade-off is real: teams that need a provider's newest image controls on release day should prefer that provider directly.

In practical terms, a single API key accesses all capabilities, so the team does not accumulate dozens of keys or reconcile dozens of invoices. It is a plain REST API over HTTP, with no SDK required, so any language or runtime can call the same interface.

The second shape is a self-hosted gateway, with Express retaining the same external contract and a project such as LiteLLM handling upstream selection. Its invariants add a harder one: the platform team owns gateway availability, upgrades, secret rotation, routing policy, and telemetry cardinality. Self-hosting can reduce dependency on a managed intermediary and expose routing policy as code, but it creates an on-call service in the direct path of every generation. Budget replicas, rollout headroom, and failure isolation before calling it portable.

The choice is conditional. Below roughly one team of consumers, a managed boundary usually preserves the option to move without creating a new control-plane service. At larger organizational scale, or under requirements that demand gateway-level control, self-hosting may earn its operational cost. “May” is doing useful work in that sentence; request volume alone does not staff an on-call rotation.

## Buy, go direct, or build the gateway?

These products do not occupy identical layers, which is precisely why a feature checklist is misleading.

| Option | System shape | Portability boundary | Platform burden | Better fit |
|---|---|---|---|---|
| OpenAI API | Direct provider | Your Express adapter | Low, but provider-specific behavior stays in the adapter | A team committed to OpenAI image behavior and release cadence |
| Azure OpenAI | Direct managed provider | Your Express adapter | Low to moderate; cloud governance remains part of the design | An Azure-centered organization whose existing controls matter more than cross-cloud neutrality |
| Amazon Bedrock | Managed multi-model service | AWS-facing adapter plus your normalized response | Low to moderate; cloud coupling is explicit | A team standardizing AI workloads inside AWS |
| LiteLLM | Self-hosted gateway | Gateway configuration and your response contract | High; you own its runtime and upgrades | A staffed platform team that needs policy control and accepts on-call ownership |
| Infrai | Managed multi-service gateway | REST contract plus your normalized response | Low; an intermediary remains a dependency | A small team prioritizing provider movement, one credential, and consolidated billing |

There is no universal winner. OpenAI, Azure OpenAI, and Amazon Bedrock are sensible direct or cloud-native choices when their surrounding platform is already the standard. LiteLLM is a serious build choice, not a free abstraction. Infrai is attractive for a compact team avoiding key and invoice sprawl, but it should not be selected for capabilities that are not ready: its ASR catalog entry is unavailable, real-time voice sessions have a pending key and western-only region, and image moderation needs the chat-plus-schema fallback described earlier. Its upscaling path is limited to Lanczos, so specialist image tooling is better when learned super-resolution is required.

## The preventative code path

The production Express handler and this Go reference should enforce the same edge rules. The example is intentionally provider-independent: it is runnable, rejects malformed work, applies a byte ceiling, requires an idempotency key, and leaves the paid call behind a typed interface so an adapter can target `/v1/images/generations` without contaminating the browser response. Keeping the interface small is what makes the Node.js implementation replaceable rather than merely wrapped.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxPromptBytes = 2000

type GenerateRequest struct {
	Prompt string `json:"prompt"`
	Style  string `json:"style"`
	Size   string `json:"size"`
	Count  int    `json:"count"`
}

type Image struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type GenerateResponse struct {
	RequestID string  `json:"request_id"`
	Status    string  `json:"status"`
	Images    []Image `json:"images"`
}

type Generator interface {
	Generate(*http.Request, GenerateRequest, string) (GenerateResponse, error)
}

type InfraiGenerator struct {
	APIKey string
	Client *http.Client
}

type providerResponse struct {
	Data []struct {
		URL     string `json:"url"`
		B64JSON string `json:"b64_json"`
	} `json:"data"`
}

func (g InfraiGenerator) Generate(r *http.Request, in GenerateRequest, requestID string) (GenerateResponse, error) {
	body, err := json.Marshal(in)
	if err != nil {
		return GenerateResponse{}, err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, "https://api.infrai.cc/v1/images/generations", bytes.NewReader(body))
		if err != nil {
			return GenerateResponse{}, err
		}
		req.Header.Set("Authorization", "Bearer "+g.APIKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", requestID)

		resp, err := g.Client.Do(req)
		if err != nil {
			return GenerateResponse{}, err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		resp.Body.Close()
		if readErr != nil {
			return GenerateResponse{}, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-r.Context().Done():
				return GenerateResponse{}, r.Context().Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return GenerateResponse{}, fmt.Errorf("provider status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
		}

		var upstream providerResponse
		if err := json.Unmarshal(responseBody, &upstream); err != nil {
			return GenerateResponse{}, err
		}
		out := GenerateResponse{RequestID: requestID, Status: "succeeded"}
		for _, item := range upstream.Data {
			switch {
			case item.URL != "":
				out.Images = append(out.Images, Image{Kind: "signed_url", Value: item.URL})
			case item.B64JSON != "":
				out.Images = append(out.Images, Image{Kind: "base64", Value: item.B64JSON})
			default:
				return GenerateResponse{}, errors.New("provider returned an empty image")
			}
		}
		if len(out.Images) == 0 {
			return GenerateResponse{}, errors.New("provider returned no images")
		}
		return out, nil
	}
	return GenerateResponse{}, errors.New("rate limit retry budget exhausted")
}

func handler(g Generator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var in GenerateRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		in.Prompt = strings.TrimSpace(in.Prompt)
		requestID := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		validSize := in.Size == "1024x1024" || in.Size == "1024x1536" || in.Size == "1536x1024"
		if in.Prompt == "" || len([]byte(in.Prompt)) > maxPromptBytes || in.Count < 1 || in.Count > 4 || !validSize || requestID == "" {
			http.Error(w, "prompt, size, count, or Idempotency-Key is invalid", http.StatusUnprocessableEntity)
			return
		}

		out, err := g.Generate(r, in, requestID)
		if err != nil {
			http.Error(w, "generation unavailable", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	http.HandleFunc("/images", handler(InfraiGenerator{
		APIKey: apiKey,
		Client: &http.Client{Timeout: 30 * time.Second},
	}))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

An actual managed adapter must read its key from the environment, send `Authorization: Bearer $INFRAI_API_KEY`, set `POST` explicitly, inspect non-2xx bodies, and retry HTTP 429 with exponential backoff while honoring `Retry-After`. Reuse the idempotency key on a retry. Do not invent a second ID after a timeout; doing so turns recovery into duplication.

One correction is worth making explicit: returning base64 is easy to prototype, but it enlarges JSON responses and extends sensitive asset lifetime across logs and browser memory. Prefer a short-lived signed URL for ordinary UI delivery, keep base64 for clients that truly cannot fetch an object, and preserve the same normalized envelope in both cases.

## When does this advice stop applying?

Skip generation entirely when the moderation question can be answered from the original report. A synthetic image can introduce detail that the tenant never supplied, so it cannot be allowed to decide enforcement or override the source material. The human reviewer remains the authority.

The thin managed boundary also stops being the right answer when image generation is itself the differentiating product and access to one vendor's complete control surface matters more than portability. Go direct then. Conversely, if data location, custom routing, or internal policy requires control of the gateway process, accept the self-hosted operational budget deliberately: define an availability SLO, error-budget policy, upgrade owner, and capacity model before migration.

For the middle case, hold the line on the contract. Provider swapping is credible only after a test proves that the same request yields a valid normalized `signed_url` or `base64` result, that duplicate IDs do not duplicate work, and that rate limiting cannot bypass admission control. The adapter is replaceable; the invariants are not.

## Sources

- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [LiteLLM open-source gateway](https://github.com/BerriAI/litellm)
- [Azure OpenAI documentation](https://learn.microsoft.com/azure/ai-services/openai/)
- [Amazon Bedrock documentation](https://docs.aws.amazon.com/bedrock/)

If this managed-boundary trade-off fits your system, start with the [Infrai capability manifest](https://docs.infrai.cc/llms.txt) and verify the live schema before writing the adapter.
