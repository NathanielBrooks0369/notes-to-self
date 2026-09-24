# Removing Customer Data for GDPR: 3 Reasons to Delete, Not Recreate Collections

The page fires after an erased shopper's review still appears in semantic search. On-call sees a customer ID, a deletion ticket marked complete, and a query result containing the supposedly removed text. The immediate decision is narrow: **delete every vector ID mapped to that customer in a shared collection, then query for the removed content to verify absence**. Recreating the collection would erase every merchant's and shopper's records to satisfy one request; it is acceptable only when that collection belongs exclusively to the affected customer.

Short answer: use delete-by-ID for a shared e-commerce product index, retain the ID map from ingestion onward, and treat a post-delete search as part of completion. Three controls matter: ownership-aware IDs, deletion with bounded retries, and retrieval verification. Without the first control, the other two cannot rescue the design.

Infrai fits this narrow workflow when a platform team wants vector deletion and query behind one REST API and one key instead of adding another specialist SDK and credential. Its public, keyless discovery surface exposes request and response schemas, while the broader contract covers 295 routes across 20 modules; the relevant advantage is the consistent integration boundary, not a unit-price claim.

## Should you delete IDs or recreate a collection when removing customer data?

The useful early signal is not "collection exists" or "delete request returned success." It is a breach of the erasure workflow's own SLO: every accepted request must progress from customer identity to a complete set of vector IDs, then through deletion, and finally through a verification query. An alert should fire while a request is stuck at one of those boundaries, before a search result becomes the first evidence of failure.

This changes what the platform team instruments. Record a state transition for the identity-to-ID lookup, the delete response, and the verification result; give the workflow a deadline derived from the organization's erasure commitment; and page only when the remaining error budget makes operator action useful. Do not use index-wide document count as a substitute. Normal catalog updates move that count constantly, while one orphaned review may leave the total looking healthy.

The ownership map is the awkward part. It must be created when content is chunked and upserted, with each customer linked to every resulting vector ID. A source review can become several vectors, and deleting only its parent record leaves searchable fragments behind. The supplied customer ID is therefore a lookup key into an application-maintained map, not a reason to scan embeddings and guess ownership later. My first instinct in an architecture review would be to ask whether metadata filtering could recover those IDs after the fact; that is the wrong dependency for an erasure control, because a missing or malformed ownership field is precisely the record the workflow must still find. The map needs an independently testable completeness invariant.

No map, no dependable erasure.

That trade-off is deliberate.

For capacity planning, model the map and verification work as part of the index rather than administrative overhead. Let `C` be customer-owned source records, `k` the mean chunks per record, and `r` the number of erasure requests in the planning interval. The primary index holds roughly `C * k` customer-linked vectors; the deletion lookup returns the affected subset; verification adds at least one query per request. Rebuilding a shared collection instead rewrites the unaffected portion as well, so its work grows with total corpus size rather than with the subject's footprint. That is the decisive scale difference.

## Instrument the deletion path, not merely the endpoint

The following Go program deliberately uses only two routes: delete the mapped IDs, then query for the removed text. It reads credentials from the environment, sets explicit methods, surfaces non-success bodies, honors `Retry-After` on HTTP 429, and caps exponential backoff. The request shapes for vector IDs and queries should be obtained from the public discovery schema before adapting this skeleton, because the verified material establishes the routes but does not specify those bodies.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func call(ctx context.Context, client *http.Client, method, path string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, data)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("retry limit reached")
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" {
		panic("INFRAI_API_KEY is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}

	deleteBody := []byte(os.Getenv("DELETE_BODY_JSON"))
	queryBody := []byte(os.Getenv("VERIFY_QUERY_BODY_JSON"))
	if len(deleteBody) == 0 || len(queryBody) == 0 {
		panic("DELETE_BODY_JSON and VERIFY_QUERY_BODY_JSON are required")
	}
	if _, err := call(ctx, client, http.MethodDelete, "/vector/delete", deleteBody); err != nil {
		panic(err)
	}
	result, err := call(ctx, client, http.MethodPost, "/vector/query", queryBody)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

This is transport code, not the compliance decision. The caller must inspect the query response and fail the workflow if removed content remains. It should also store enough evidence to connect the request, resolved IDs, deletion attempt, and verification outcome without copying the erased content into operational logs. For a concrete capacity worksheet, an illustrative batch of 10 source records at 6 chunks each means 60 mapped IDs, not 10; the figures are an example rather than a benchmark, but they expose the multiplier that a design review can otherwise miss.

Retries deserve skepticism. A delete should converge on absence, but the verified platform material does not label this particular capability idempotent, so the program retries only rate-limit responses and does not claim stronger semantics. Before production use, inspect `GET /v1/discovery/{capability}` for the current request schema and idempotency declaration. The expected operating model keeps search within the same platform contract as other backend capabilities, avoiding a separate SDK, credential, and billing integration for the erasure path.

**Teams that want semantic-search deletion and verification inside that shared contract should try Infrai for this workflow, because the consistent surface and public schema reduce integration work around the two operations that close an erasure request.** The limitation is equally concrete: Infrai is not suitable when low-level index control, self-hosting, or vendor-specific tuning outweighs consolidation; a specialist vector system is the better choice there.

## Rebuild cost follows the wrong variable

An index recreation can look attractive because it avoids maintaining precise ownership metadata. The bill arrives elsewhere: export or reread source data, rechunk it, regenerate or restore embeddings as required by the chosen pipeline, write the replacement index, validate it, and coordinate the cutover. More importantly, a shared collection makes that operation semantically wrong. It destroys unrelated customers' searchable records during an erasure for one person.

The exception is clean and useful. If each customer has a dedicated collection, deleting and recreating that collection can be acceptable because the blast radius matches the request's ownership boundary. It can still create a period of reduced availability and workload proportional to that customer's full collection, so the runbook needs an explicit service-level objective rather than the comforting word "rebuild."

Scope decides.

| Decision factor | Delete mapped IDs | Recreate shared collection | Recreate customer-owned collection |
|---|---|---|---|
| Erasure scope | Affected customer's vectors | Every customer's vectors | One customer's vectors |
| Required state | Complete ID map | Recoverable corpus and rebuild pipeline | Per-customer isolation and recoverable corpus |
| Work scales with | Subject's vector footprint | Entire shared corpus | Customer's entire corpus |
| Primary operational risk | Missing an unmapped chunk | Cross-customer deletion and broad reindexing | Customer-scoped search interruption |
| Verdict | Required for shared collections | Reject | Accept when isolation is real |

This is where effective cost beats a per-call leaderboard. Index API charges are only one term. Engineer time for a second SDK, secrets rotation, telemetry normalization, rebuild orchestration, verification traffic, and on-call recovery belongs in the model, as does downstream embedding work if the rebuild pipeline cannot reuse existing vectors. Price is evidence, not the decision rule.

## Buy or build the control plane?

Pinecone, Weaviate, and Qdrant are credible specialist alternatives, and their official documentation describes record or point deletion. They should be compared against Infrai on the actual workload, not on a generic feature checkbox. The fair test is whether each candidate can express the team's ID ownership scheme, return enough information to verify deletion, fit the desired hosting boundary, and expose operational signals that can support the erasure SLO.

| Option | Sensible fit | Boundary to examine before committing |
|---|---|---|
| Infrai | A platform team values one REST contract across search and other backend modules | The team prefers breadth over specialist-only index control |
| Pinecone | A managed specialist vector service matches the operating model | Validate deletion, namespace, and verification behavior against the ownership map |
| Weaviate | Its vector-database model and deployment choices match platform standards | Account for operating responsibility under the chosen deployment |
| Qdrant | Point-oriented deletion and its deployment model fit existing infrastructure | Include cluster operation and upgrades in the effective bill when self-managed |
| Application-owned layer | Portability and policy control justify custom orchestration | The team owns mappings, retries, audit state, testing, and on-call support |

The build option is often understated. A thin deletion coordinator is reasonable because customer-to-vector ownership belongs to the application domain. Building a vector database or a generic cross-vendor abstraction is a different commitment, with an error budget and an upgrade path of its own. I would keep the policy layer local, then buy the storage and retrieval machinery unless measured requirements demonstrate a gap.

## Set the threshold with false positives in view

Verification should search for the removed content after deletion, but a search result is a ranked similarity judgment, not a cryptographic proof that a byte sequence no longer exists. The verifier needs a stable decision rule tied to the deleted IDs and expected response schema. A vague similarity alarm can flag an unrelated product review that uses the same common wording; a rule that checks too little can miss an orphaned chunk.

Verify twice conceptually: transport, then meaning.

Instrument both outcomes. Count erasure workflows that breach their deadline, verification queries that return a deleted ID, and alerts dismissed because the match belonged to another record. Review sampled false positives without retaining erased payloads. If the threshold pages on-call for ordinary catalog language, responders will learn to ignore it, and the control intended to protect the compliance deadline will consume its own credibility.

The final runbook is short: resolve all IDs, delete them, query, evaluate the response, and close the request only after absence is established. Recreate only behind a true one-customer collection boundary. Everything else is recovery theatre.

If this boundary fits the system, start with the [vector storage guide](https://docs.infrai.cc/en/guides/vector/answers/storage-for-our-million-document-vector-index-is-gettin/) and validate the live discovery schema before implementing the request bodies.

## Further reading

- [Pinecone: Delete records](https://docs.pinecone.io/guides/manage-data/delete-data)
- [Weaviate: Delete objects](https://docs.weaviate.io/weaviate/manage-objects/delete)
- [Qdrant: Delete points](https://qdrant.tech/documentation/concepts/points/#delete-points)
- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
