# Image Processing API Observability: Clinical Crop Quality for Small SaaS Teams

TL;DR: For a small healthtech SaaS producing several aspect ratios from one clinical image, the simplest defensible design is one immutable original, a small allowlist of named derivatives, and an asynchronous quality check before those derivatives become visible. Page on the rate of bad crop sets, not raw transformation errors. Bandwidth belongs in the decision, but a cheap response is worthless when the crop removes the region a reviewer needs.

The page arrives as `clinical_crop_set_quality_burn`: valid requests are completing, yet too many newly published image sets fail the quality policy. The on-call sees the affected tenant count, the derivative recipe revision, input-format distribution, and two links: one to sampled redacted metadata and one to the deployment that changed the recipe. No patient image appears in the alert. The immediate action is to stop publishing the suspect revision and keep serving the last accepted set while queued originals are reprocessed.

That page is deliberately late in the pipeline. Working backward reveals the signal that should have fired earlier: the canary's accepted-set ratio moved before the user-facing error-budget burn did. The important engineering choice is where quality becomes measurable, reversible, and bounded.

## What should the on-call learn from one page?

A transport-success counter cannot answer whether a smart crop is useful. HTTP success says that bytes came back. It does not say that every required ratio was produced, that dimensions match the recipe, or that the crop retained the intended subject. Treat the output as a set with one verdict; publishing three acceptable derivatives and one damaged derivative creates inconsistent review behavior and makes rollback harder.

The first dashboard should separate four stages: original accepted, transform completed, crop set accepted, and crop set published. Then split failures by recipe revision and broad input format, while keeping tenant and image identifiers out of metric labels. High-cardinality identity belongs in access-controlled logs or traces. This is a capacity-planning issue as much as an observability issue: cardinality that grows with uploads can turn the telemetry path into a second production workload.

A useful page carries a numerator and denominator. `rejected crop sets / evaluated crop sets` is actionable; a count of rejected images rises naturally with traffic. It also needs a minimum-volume guard so one rejected canary does not wake someone during a quiet period. Set the actual page threshold from the service SLO and observed baseline, not from a borrowed percentage.

No crop, no publish.

Short alert text is a feature.

## Instrument the acceptance boundary

Keep the transformation adapter boring. It receives a named recipe, obtains derivatives, validates structural properties, calls the approved subject-retention evaluator, and emits one bounded event. The evaluator may change; the event contract should not. This focused Go example shows the instrumentation boundary without pretending that pixel dimensions prove semantic crop quality.

```go
package crops

import (
    "context"
    "fmt"
)

type Derivative struct {
    Name          string
    Width, Height int
    Bytes         int64
}

type Result struct {
    Recipe   string
    Outputs  []Derivative
    Accepted bool
    Reason   string
}

type Recorder interface {
    ObserveCropSet(ctx context.Context, recipe, outcome, reason string, bytes int64)
}

type Publisher interface {
    Publish(ctx context.Context, outputs []Derivative) error
}

func PublishAccepted(ctx context.Context, r Result, rec Recorder, pub Publisher) error {
    var total int64
    for _, output := range r.Outputs {
        total += output.Bytes
    }

    outcome := "accepted"
    if !r.Accepted {
        outcome = "rejected"
    }
    rec.ObserveCropSet(ctx, r.Recipe, outcome, r.Reason, total)

    if !r.Accepted {
        return fmt.Errorf("crop set rejected: %s", r.Reason)
    }
    return pub.Publish(ctx, r.Outputs)
}
```

`recipe`, `outcome`, and `reason` must come from finite registries. Do not put a filename, URL, free-form exception, or patient identifier in those fields. Record total output bytes because quality versus bandwidth is the governing trade-off: the same canary corpus can show whether a recipe change preserves acceptance while increasing transfer size.

Structural checks come first because they are deterministic: expected member count, exact target dimensions, decodability, and an allowed output format. Semantic acceptance follows, using the same approved evaluator and frozen corpus for every candidate. MDN's image-format guide is a useful reference for browser format characteristics, but format selection still needs testing against the clients the service actually supports.

Run that corpus before deployment, again on a small canary, and after any recipe or evaluator revision. Store the original immutably so a rejected set can be regenerated. Cache derivatives by original identity plus recipe revision; otherwise a changed crop policy can collide with old cached output even though both requests look superficially valid.

## What should a small SaaS test in an alternative image processing API?

Cloudinary, imgix, and ImageKit may all appear on a shortlist because they are the candidates in this selection question. The available evidence does not establish a factual feature or price winner among them, so a responsible evaluation runs the same acceptance contract against each candidate and against a self-operated path. Do not award points for a feature that the application cannot observe.

| Decision | Managed transformation path | Self-operated transformation path | Evidence to collect |
|---|---|---|---|
| Crop quality | Provider behavior is behind an adapter | Team owns the evaluator and runtime | Accepted-set rate on the frozen corpus |
| Bandwidth | Delivery behavior is part of the service boundary | Team owns encoding, cache, and egress design | Bytes per accepted crop set by client class |
| On-call load | External dependency still needs failure policy | Runtime, patching, and capacity stay with the team | Pages, operator time, and recovery steps during a trial |
| Lock-in | URL semantics can leak into stored data or clients | Internal recipe semantics can become accidental API | Time to replay originals through a second adapter |
| Change safety | Provider and local recipe changes need canaries | Dependency and configuration changes need canaries | Rollback result and cache-key correctness |

This is the buy-versus-build line I care about: ownership should follow the failure mode the team is staffed to carry. A managed path can reduce machinery under direct control, but it does not remove responsibility for crop acceptance, privacy boundaries, rollback, or an SLO. A self-operated path offers control and portability at the price of capacity forecasts, patch work, and a larger on-call surface. The trade-off is explicit: fewer runtime duties can justify less direct control only if the adapter, original-image retention, and acceptance gate preserve an exit path. I would require one engineer unfamiliar with the candidate to replay the frozen corpus through a second adapter; a design that needs hidden URL rules or manual cache repair has failed that portability check before procurement begins. Neither column wins by definition.

For a small service, constrain the trial before discussing contracts. Use one adapter interface, one frozen and appropriately governed corpus, the same named ratios, the same client compatibility matrix, and the same observation window. Compare total bytes only for accepted sets. Comparing bytes for damaged crops rewards the wrong system.

Stop there.

Do not let price become the headline metric. First reject any option that cannot meet the quality policy, data-handling constraints, or recovery objective. For the survivors, model total operational cost from measured request volume, stored originals and derivatives, delivery bytes, cache behavior, engineering time, and expected on-call work. Published unit prices can change; the workload model and ownership questions endure.

## Close the loop without training the pager to lie

Deploy a recipe revision dark, evaluate it on the frozen corpus, and then canary it on bounded production traffic without exposing rejected output. Promotion requires both structural validity and subject retention. If the canary degrades, stop that revision; because originals and previous accepted sets remain available, recovery does not depend on reconstructing lost inputs.

The earlier warning should be a ticket or deployment gate when the canary moves beyond its learned baseline but the user-facing SLO is intact. The page is reserved for sustained error-budget burn with enough evaluated volume to be meaningful. That division matters. If both signals page, operators will learn that one of them is usually noise and will hesitate when the real crop-quality alert arrives.

Thresholds have a cost on both sides. Too loose, and reviewers encounter bad framing before the service reacts. Too tight, and natural variation in a small denominator repeatedly interrupts the on-call, consumes the very operator capacity that a managed service was supposed to preserve, and encourages broad muting. Backtest candidate thresholds against historical aggregate events, document the expected page rate, and review the threshold when volume or the SLO changes.

The final selection is the option that passes the same crop-set acceptance policy with tolerable bandwidth and an operational burden the team can actually own. Keep that sentence free of vendor names. The pager should defend the clinical workflow, not a procurement decision.

## Further reading

References:

- MDN, Image file type and format guide: https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
