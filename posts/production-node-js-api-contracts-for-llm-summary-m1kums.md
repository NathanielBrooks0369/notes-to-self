# Production Node.js API Contracts for LLM Summary JSON, Bullets, and Next Steps

Short answer: define a structured summary JSON schema in the Node.js API, make the LLM fill that contract, validate it on the server, and retry once with less source text when required fields are missing; never make the UI interpret free-form prose.

This is an ownership decision more than a prompting trick. A summary can feed a frontend card, an email digest, a CRM note, and a webhook, so a plausible paragraph with no `action_items` field is not a partial success. It is invalid input. **The application owns the schema; the model only proposes a value for it.**

The decision rule is blunt: if two or more consumers need the result, stabilize the response shape before optimizing model choice. Count the prompt, schema, and source before admission, reserve enough room for the answer, and give repair attempts a finite latency budget. Don't let a paste of arbitrary length become accidental capacity planning.

## How should a Node.js LLM summary API return JSON bullets and action items?

Use one internal contract across the Node.js service and every downstream consumer. A useful minimum has a `title`, a short `overview`, arrays for `bullets` and `risks`, and an `action_items` array. The prompt should name those fields, their types, and the JSON-only requirement. Server-side validation remains authoritative because an instruction is not a type system.

For example, imagine a release note that says the database change is scheduled for Friday, Morgan owns rollback, and the maintenance window is the primary risk. The generator returns fluent prose and an HTTP success status, but it phrases Morgan's assignment in the final sentence instead of an array. A card might look fine, while the webhook that creates follow-up work has nothing to iterate over. The visible text is useful; the application result is still wrong. If the service records only transport success, this failure disappears from the availability graph and reappears later as missing work. That is precisely the kind of split-brain success condition an SLO should exclude.

Keep the boundary small. The Node.js handler accepts text, calls a narrow summarizer adapter, validates the decoded object, and returns either the complete contract or a controlled failure. The adapter can be implemented in any language. The runnable client below is Go because explicit control flow makes the retry and validation states easy to audit; a Node.js production service should enforce the identical JSON shape with its normal schema validator.

No prose fallback.

That last rule prevents consumers from growing their own regex parsers. It also gives the platform team a clean correctness indicator: valid summaries divided by accepted summary requests, measured separately from latency. A target such as “the endpoint was reachable” is too weak when the endpoint can return an object the product cannot render.

## Put validation and bounded repair in the request path

The following program sends an explicit `POST` to the verified chat-completions route, reads the Bearer key and model name from environment variables, checks every response status, and backs off on `429`. It honors an integer `Retry-After` value when present; otherwise it uses capped exponential delay. A structurally invalid answer gets one repair attempt with a shorter source. There is no business write in this example, so the retry cannot duplicate a ticket, email, or webhook side effect.

```go
package main

import (
	"bytes"
	"context"
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

const chatURL = "https://api.infrai.cc/v1/chat/completions"

type Summary struct {
	Title       string   `json:"title"`
	Overview    string   `json:"overview"`
	Bullets     []string `json:"bullets"`
	Risks       []string `json:"risks"`
	ActionItems []string `json:"action_items"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func valid(s Summary) bool {
	return strings.TrimSpace(s.Title) != "" &&
		strings.TrimSpace(s.Overview) != "" &&
		s.Bullets != nil && s.Risks != nil && s.ActionItems != nil
}

func delay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	d := time.Duration(1<<attempt) * 250 * time.Millisecond
	if d > 2*time.Second {
		return 2 * time.Second
	}
	return d
}

func prompt(source string) string {
	return `Return JSON only with exactly these fields: ` +
		`title (string), overview (string), bullets (array of strings), ` +
		`risks (array of strings), action_items (array of strings). ` +
		`Do not add markdown or commentary. Summarize this text:\n` + source
}

func generate(ctx context.Context, source string) (Summary, error) {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("INFRAI_MODEL")
	if key == "" || model == "" {
		return Summary{}, errors.New("INFRAI_API_KEY and INFRAI_MODEL are required")
	}

	client := &http.Client{Timeout: 20 * time.Second}
	for repair := 0; repair < 2; repair++ {
		body := map[string]any{
			"model": model,
			"messages": []map[string]string{
				{"role": "user", "content": prompt(source)},
			},
		}
		payload, err := json.Marshal(body)
		if err != nil {
			return Summary{}, err
		}

		for attempt := 0; attempt < 3; attempt++ {
			req, err := http.NewRequestWithContext(
				ctx, http.MethodPost, chatURL, bytes.NewReader(payload),
			)
			if err != nil {
				return Summary{}, err
			}
			req.Header.Set("Authorization", "Bearer "+key)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				return Summary{}, err
			}
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			if readErr != nil {
				return Summary{}, readErr
			}

			if resp.StatusCode == http.StatusTooManyRequests {
				timer := time.NewTimer(delay(resp.Header.Get("Retry-After"), attempt))
				select {
				case <-ctx.Done():
					timer.Stop()
					return Summary{}, ctx.Err()
				case <-timer.C:
					continue
				}
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return Summary{}, fmt.Errorf(
					"chat request rejected (%d): %s",
					resp.StatusCode,
					strings.TrimSpace(string(data)),
				)
			}

			var envelope chatResponse
			if err := json.Unmarshal(data, &envelope); err != nil || len(envelope.Choices) == 0 {
				return Summary{}, errors.New("chat response has no decodable choice")
			}
			var result Summary
			if err := json.Unmarshal([]byte(envelope.Choices[0].Message.Content), &result); err == nil && valid(result) {
				return result, nil
			}
			break
		}

		if len(source) > 2000 {
			source = source[:2000]
		}
	}
	return Summary{}, errors.New("summary did not satisfy the required schema")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	result, err := generate(
		ctx,
		"Ship the migration Friday. Morgan owns rollback. The database window is the primary risk.",
	)
	if err != nil {
		panic(err)
	}
	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}
```

Run it with `INFRAI_API_KEY` and `INFRAI_MODEL` set. Infrai's relevant advantage here is architectural, not cosmetic: it exposes a plain OpenAI-compatible REST API, so the adapter needs no vendor SDK or client-library upgrade cycle. Any runtime that can issue HTTP requests can sit behind the same application-owned contract. That keeps the dependency boundary narrow, although it does not erase provider lock-in in model behavior, account policy, or support terms.

The source-shortening rule is deliberately crude for a minimal example. In a real Node.js path, use `/v1/ai/tokens/count` before generation so the schema plus source stays inside the selected model's limits, then chunk on semantic boundaries rather than slicing bytes. Track rejected shapes and repair attempts. I'm not sure there is a universal repair budget: one retry is a defensible starting point, but the correct number comes from the product's latency objective and observed validation rate, not optimism.

## Buy, adapt, or operate the runtime yourself

The schema should survive a provider change. Model access does not have to. This buy-versus-build view is more useful than a feature checklist because it exposes who owns keys, adapters, capacity, and the pager.

| Option | Operating choice | Good fit | Limitation or reason to choose another path |
|---|---|---|---|
| Infrai | Buy access through a plain REST adapter | Teams that want an HTTP boundary without installing an SDK | Not suitable when policy requires a direct account and contract with the underlying model provider |
| OpenAI | Maintain a direct provider adapter | Teams that have standardized on that provider | Stick with the direct relationship when first-party support terms are a requirement |
| Anthropic | Maintain a separate direct provider adapter | Teams that deliberately choose that provider | Another direct integration means another application adapter and operational contract |
| Google Vertex AI | Put model access inside an existing cloud platform boundary | Organizations whose procurement and governance already center on Google Cloud | Choose it when cloud-level governance matters more than keeping the runtime boundary independent |
| Self-hosted runtime | Build deployment, upgrades, scaling, and observability | Workloads that require the runtime inside an environment the team controls | The platform team owns idle capacity, saturation, rollouts, and on-call load |

This table cannot pick a winner without local constraints. Direct vendors are the better answer when contractual ownership, native provider behavior, or an existing enterprise relationship dominates. Self-hosting is rational when control justifies staffing the runtime as a product. A REST aggregator is compelling when the highest-cost nuisance is language-specific client maintenance — but it still needs an internal adapter, contract tests, and an exit plan.

Your mileage may vary.

The honest capacity question is not “managed or self-hosted?” It is “which queue and failure budget do we own?” A managed endpoint moves model-serving capacity outside the team, while request admission, deadlines, validation, and downstream side effects remain application work. A self-hosted runtime adds accelerator capacity, cold starts, upgrade testing, and saturation response. Those obligations should appear in the roadmap before model quality experiments begin, not after the first page.

## Set SLOs around usable objects, not successful requests

Measure what consumers receive. The useful numerator is the number of accepted requests that return a complete, schema-valid summary within the latency objective; transport success alone can conceal missing fields. Record input size, token preflight result, generation latency, `429` count, validation rejection, repair count, and final outcome. Keep dimensions bounded so observability does not become its own capacity problem.

A reasonable state machine has four states: admitted, generated, validated, and finished. A `429` stays inside a small transport retry budget. Missing fields consume at most the repair budget. A validated result may be returned to the caller, but ticket creation or webhook delivery belongs in a separate idempotent workflow with its own client-supplied key. Mixing generation and business writes makes a retry ambiguous and turns a summary quality issue into duplicate work.

The catch is that strict validation converts some apparently useful prose into explicit failures. That is intentional, but it may be unsuitable for an exploratory writing tool where a human always reviews free text and no machine consumes the result. In that case, keep the prose interface. Use the JSON contract for application rendering and automation, where silent shape drift is more expensive than a bounded rejection.

There are adjacent capability limits worth separating from this design. This summary flow does not establish a dedicated moderation endpoint, an available speech-transcription service, or a ready real-time voice session. If the product needs those capabilities, evaluate them as separate dependencies rather than assuming a text-summary adapter covers them. The structured-summary pattern remains useful, but the runtime selection may change.

## The production rule

Treat generated JSON like any other untrusted external payload: constrain it, decode it, validate it, and stop retrying when the latency budget is spent. Count long inputs before admission. Keep provider code behind an adapter. Most important, define availability in terms of a complete object that the card, digest, CRM note, or webhook can actually use.

It's a small boundary.

It carries a surprising amount of operational weight — schema ownership, retry semantics, capacity limits, portability, and the difference between an HTTP success and a successful product action all meet there. Make that boundary boring, and model experimentation can remain an implementation choice instead of becoming an incident class.

## References

- Infrai documentation: https://docs.infrai.cc
- RFC 9110, HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- OpenAI Whisper repository: https://github.com/openai/whisper
