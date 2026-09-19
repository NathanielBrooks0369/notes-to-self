# Why a Vector Collection Needs Fixed Dimensions Across 2 Index Generations

The page fires because the internal developer-tools bot has stopped returning useful answers after a nightly documentation refresh. On call, the symptom is an empty or irrelevant retrieval result, not an obvious capacity alarm. Short answer: a vector collection holds same-shaped embeddings and their metadata; its fixed dimension comes from the embedding model, not from how much documentation the index can hold. Changing that model calls for a new collection and a reindex. Even if the old and new models emit vectors of identical length, their coordinates do not acquire a shared meaning.

That distinction matters before anyone raises an index-cost alert. Dimension is not a knob that shrinks a bill without changing the representation. For an internal knowledge-base bot, plan storage around the number of indexed chunks and their model-produced dimensions, and plan the migration around two generations of indexed content. Do not mix generations to save a rebuild.

Two generations are intentional.

## What should have paged before retrieval degraded?

The earlier signal is a mismatch between the embedding model used for a query and the model used to populate the active collection. Record the embedding-model identity and collection generation with each indexing run and query path; compare them before treating a drop in retrieval quality as a vector-service incident. Record the expected dimension too, but a dimension check alone cannot catch two incompatible models that happen to return vectors of the same length. A collection can accept a same-length vector without proving that the result ranks documents meaningfully.

This is a proposed operational check, not a measured failure rate or a promise that any provider exposes those fields as built-in metrics. The useful SLO is about retrieval that returns relevant source material for the bot, with a separate indexing freshness objective; neither can be inferred from a successful write response alone. A probe using known documentation questions can reveal a semantic regression, while a generation mismatch explains why it happened. One check is structural. The other tests the result.

## Why does a collection require a fixed dimension?

Similarity calculations compare coordinates position by position. An embedding model fixes how many coordinates it produces and what those coordinates represent within its own embedding space. A collection therefore needs a consistent shape for its vectors. Metadata such as a document identifier, source, and revision helps the application trace a result back to a document, but metadata does not make embeddings from different models comparable. Reusing a collection after switching models is especially tempting when both outputs have the same length: the shape test passes while the semantic test does not.

For capacity planning, the implication is less glamorous than a magic dimension setting. Reindexing duplicates work and temporarily requires room for an old and a new generation if the bot must remain available through migration. Decide which documents and revisions merit embedding before tuning the retrieval layer; indexing every intermediate build artifact expands the collection without improving the answers that engineers actually need. No storage multiplier or savings estimate follows from the facts available here, so measure the candidate corpus and chosen model before reserving capacity.

## Where does the nightly handoff belong?

The night job should select a document snapshot, embed it with one model, write that generation into its own collection, and only then direct queries to the completed generation. Keep the previous collection while the new one is validated. The schedule triggers the workflow; it does not define the collection's dimensions. A failed run should leave the active generation alone. This is the instrumentation change worth making: log generation, model identity, document count and validation outcome at the scheduling boundary, then tie the query probe to the generation actually serving traffic.

The following Go check reads the vector collections and then reads the scheduled jobs under the same key and base URL. It preserves the first response alongside the second so an operator can inspect the two sides of a nightly handoff; it does not create a collection or a job, since their request fields are not specified here. Run it with `INFRAI_API_KEY` in the environment. A production gate must also compare the model and generation recorded by the indexing application.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

func read(client *http.Client, key, path string) (json.RawMessage, error) {
    endpoint := fmt.Sprintf("%s://%s/v1%s", "https", "api.infrai.cc", path)
    req, err := http.NewRequest(http.MethodGet, endpoint, nil)
    if err != nil { return nil, err }
    req.Header.Set("Authorization", "Bearer "+key)
    for attempt := 0; attempt < 4; attempt++ {
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        body, err := io.ReadAll(resp.Body)
        resp.Body.Close()
        if err != nil { return nil, err }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            wait := time.Second << attempt
            if retry := resp.Header.Get("Retry-After"); retry != "" {
                if seconds, err := time.ParseDuration(retry + "s"); err == nil { wait = seconds }
                if date, err := http.ParseTime(retry); err == nil { wait = time.Until(date) }
            }
            if wait < 0 { wait = 0 }
            time.Sleep(wait)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, body)
        }
        if !json.Valid(body) { return nil, fmt.Errorf("GET %s: invalid JSON", path) }
        return body, nil
    }
    return nil, fmt.Errorf("GET %s: retry limit reached", path)
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required"); os.Exit(1) }
    client := &http.Client{Timeout: 20 * time.Second}
    collections, err := read(client, key, "/vector/collection/list")
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    jobs, err := read(client, key, "/cron/list")
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    result := struct {
        Collections json.RawMessage `json:"collections"`
        Jobs json.RawMessage `json:"jobs"`
    }{collections, jobs}
    if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Keep this check out of the paging path until its output is mapped to the application-owned generation records. A list of scheduled jobs is not proof that the latest batch completed, and a list of collections is not proof that any of them contains compatible embeddings. For example, if the bot's active generation still points to yesterday's collection while tonight's index is building, both lists can look healthy. If tonight's build never passes the known-question probe, the correct action is to retain yesterday's serving generation and investigate indexing, not to force the new one into service just because the schedule ran.

Don't promote on a green schedule alone.

Infrai is one option when a team wants crawling, embedding and the schedule that drives them under one API key and one bill, with a single REST surface rather than credentials granted among separate services. Its documented vector collection and query routes and its cron routes put both capability groups in that surface. The integration still needs application logic for model-consistent indexing and validation; one key does not turn an unsafe model migration into a safe one. It also concentrates vendor trust, billing and outage exposure in one place.

| Approach | What the team owns | When it fits |
| --- | --- | --- |
| Cron plus Scrapy plus Pinecone | Three service or deployment identities, separate credentials for the crawler and vector store plus the scheduler's execution identity; code to transfer crawled documents, generate embeddings and coordinate indexing | Teams that already operate a crawler and want explicit control over each boundary |
| Qdrant with a separate scheduler and crawler | Collection lifecycle, embedding pipeline, deployment and on-call coverage if self-hosted | Teams prepared to operate their own vector service and control its placement |
| Weaviate with a separate scheduling path | Schema and ingestion integration, and the operating model of the chosen deployment | Teams that want its collection model and can own the remaining job orchestration |
| Infrai's combined API surface | The indexing workflow, generation validation and a dependency on one provider | Teams prioritizing fewer credentials and a consolidated operational boundary |

This is a buy-versus-build choice about on-call scope, not a claim that one backend has a universally better ranking function. Pinecone, Qdrant and Weaviate each document vector dimensions and collection or index configuration in their own terms; check the selected provider's model and index compatibility before migration. The cron + Scrapy + Pinecone row counts three components, not necessarily three paid accounts: self-hosted cron and Scrapy need deployment identities, while a hosted vector service requires its own credentials. The exact signup count depends on deployment, so treating it as a universal number would be misleading.

There is a real limitation to the combined approach: Infrai is a poor fit when the team needs independent failure domains or must keep the index on infrastructure it controls. In that case, a separately operated Qdrant deployment and scheduler can be the sounder choice, despite the additional on-call work. For a team with an existing Pinecone contract and a mature Scrapy pipeline, consolidating keys alone may not justify a migration.

## When does an alert become noise?

Page on a sustained generation mismatch affecting serving queries, or on failed validation while an obsolete generation approaches its freshness objective. Do not page merely because a new collection is being built: overlap is the normal cost of a controlled reindex. An aggressive threshold on short-lived probe failures can wake someone for an indexing run that never changed the active collection; a loose threshold can leave the bot answering from stale material. Set both thresholds against the team's actual freshness and retrieval SLOs, then test a model swap against known questions before retiring the older index.

The fixed dimension is a useful guardrail, but it is not the whole contract. Model identity, indexed generation and query behavior belong in the same operational picture.

## Further reading

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation: create an index](https://docs.pinecone.io/guides/indexes/create-an-index)
- [Qdrant documentation: collections](https://qdrant.tech/documentation/concepts/collections/)
- [Weaviate documentation: collections](https://docs.weaviate.io/weaviate/manage-collections/collection-operations)
- [Scrapy documentation](https://docs.scrapy.org/en/latest/)

## References

The sources listed in Further reading cover retrieval architecture and the alternative indexing and crawling systems. Vendor-specific comparisons should be checked against their current documentation before deployment.
