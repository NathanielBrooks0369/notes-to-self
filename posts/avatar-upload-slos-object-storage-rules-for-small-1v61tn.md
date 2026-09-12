# Avatar Upload SLOs: Object Storage Rules for Small Images and Multipart

Short answer: a beginner handling small avatar files should start with one bounded object upload, keep the object key in the application database, and add multipart only when measured large-file throughput or interruption data says that retransmission is missing the upload SLO.

The default here is deliberately narrower than “always use multipart for object storage.” This business SaaS workflow stores training artifacts and accepts avatars and other files. The avatar is the small-file case; the training artifact is the capacity-planning case. Mixing their policies is how a harmless profile feature becomes an operational obligation.

## The incident lesson: retention starts with ownership

The failure mode I plan for is not a dramatic outage. It is a half-finished upload that has no owner, no expiry decision, and no corresponding application record. A user replaces an avatar while a large training artifact is still transferring. The browser disappears. The application has recorded intent, but storage has not recorded a complete object. Later, a cleanup job sees bytes and cannot tell whether they are abandoned work or a delayed completion.

The invariant is simple: every durable upload state needs an owner, a timeout, and a deletion path. A completed object should have an application record that names its key, retention class, content type, and creator. An in-progress transfer should have a session identifier, an expiry, and a status that can be reconciled without guessing from a bucket listing. That separation makes a retention policy reproducible: the database says why an object exists, while object storage holds the bytes.

Small avatars rarely justify preserving partial work. Retrying the whole bounded file is easier to observe and easier to explain to support. Large training artifacts are different. If a multi-gigabyte transfer repeatedly fails near the end, retransmitting the complete object can consume the user's time budget and the platform's egress budget. The decision should follow observed file-size percentiles, interrupted-transfer rate, concurrency, and the completion SLO, not a universal threshold copied from another system.

Three words: measure the tail.

## The retention record owns deletion

Yes. Define the record before selecting the transfer protocol. A training artifact may need a longer retention class, an audit trail, and a deletion approval path; an avatar may be replaced frequently and retained only while the account exists. Those are governance decisions. Multipart cannot supply them.

Keep the state machine small.

## How should a beginner evaluate multipart for avatar uploads?

Start with four measurements: accepted file-size distribution, upload duration by network class, retry rate, and the proportion of sessions abandoned before completion. Record them by artifact type. Avatar images and training artifacts do not belong in one histogram, because their failure costs and retention rules differ. I would also record the number of active upload sessions and the age of incomplete sessions; those are the inputs to a capacity forecast and to a cleanup alert.

I am not sure a fixed byte cutoff can be correct for every browser, mobile network, and SLO. Your mileage may vary. A threshold becomes defensible only after the team can say, in plain terms, “above this size, a whole-file retry causes the completion target to fail often enough to pay for multipart state.” Until then, one request is the least complex design that meets the requirement.

Multipart is a transport and lifecycle choice. It normally introduces create, upload-part, complete, and abort transitions, along with part identifiers and reconciliation. Those transitions need idempotency rules. Completion must update the application record only after storage confirms the finished object. Abort must be safe to repeat. An expiry process must distinguish a genuinely active session from abandoned work. If the team cannot name who owns each transition during an on-call handoff, the feature isn't ready for production.

## The preventative path: one record, two transport choices

Use the same application contract for both classes of object, but allow different transports behind it. The application accepts a declared artifact type, validates size and media type, allocates an object key, and creates an upload record. A small avatar can use a single PUT or a presigned single-object request. A large training artifact can use multipart when the measured SLO requires resumability. The user-facing workflow should not expose storage-specific state as if it were business state.

The key should be opaque enough that changing a filename does not change ownership. A generated key tied to the upload record is safer than using a user-supplied filename as identity. The record then supports replacement, retention review, and erasure without searching object metadata. For GDPR deletion requests, the application can locate every object associated with the subject and issue deletion work from an authoritative list; Article 17 is a policy requirement, not a reason to rely on a storage listing operation.

Here is the small-file decision boundary in Go. It intentionally keeps policy separate from any provider client, so changing the transport does not change the retention record.

```go
package upload

import "fmt"

type Kind string

const (
	Avatar   Kind = "avatar"
	Training Kind = "training-artifact"
)

type Plan struct {
	Key       string
	Multipart bool
	Retention string
}

func PlanUpload(id string, kind Kind, size int64, measuredLimit int64) (Plan, error) {
	if id == "" || size <= 0 || measuredLimit <= 0 {
		return Plan{}, fmt.Errorf("upload identity and positive limits are required")
	}

	return Plan{
		Key:       fmt.Sprintf("uploads/%s", id),
		Multipart: kind == Training && size > measuredLimit,
		Retention: string(kind),
	}, nil
}
```

The example is a policy sketch, not a claim that one number fits every deployment. In production, `measuredLimit` should come from a reviewed configuration and the retention value should map to an explicit policy table. A cleanup worker can then expire abandoned sessions, while a separate retention worker handles completed objects. Do not make one job infer both states from object age.

## The storage boundary has a cost

The relevant comparison is the boundary the platform team must operate, not a leaderboard of storage products. A direct integration can fit an organization whose identity, networking, lifecycle, and support practices already cover the chosen storage control plane. Self-hosting can fit a team that can staff capacity, upgrades, repair, and recovery. A managed abstraction can reduce the number of credentials and client contracts the application carries, but it cannot remove the need to understand retention, recovery, data residency, or multipart cleanup.

| Approach | Good fit | Trade-off to accept |
|---|---|---|
| Storage boundary | What must be true | Cost in engineering ownership |
|---|---|---|
| Direct integration | Identity, lifecycle, and audit controls already exist | Provider-shaped APIs remain in the application boundary. |
| Self-hosted object storage | Capacity, upgrades, repair, and recovery have staffed owners | Durability, replication, and on-call load stay with the team. |
| Managed abstraction | The extra dependency and its capability map are documented | Portability and retention guarantees still need verification. |
| Database-backed small files | Objects are genuinely tiny and low-volume | Database growth, backups, and large-file throughput become constraints. |

For this scenario, the deciding evidence is throughput and ownership. A direct or self-hosted option can carry large artifact movement only if the team can meet its durability and recovery SLOs. A managed layer can reduce integration work when a single REST contract and one credential boundary are useful, but that convenience is not a substitute for checking multipart semantics or retention controls. The catch is that a boundary is not suitable when it cannot express the deletion, audit, and recovery guarantees the training data requires. Stick with the simpler single-upload path when the file distribution and failure data do not justify another state machine.

## Capacity benchmarks belong in the rollout plan

Instrument intent, bytes transferred, completed uploads, aborted uploads, retries, duration, and incomplete-session age. Alert on the user-visible completion SLO and on the growth rate of abandoned state separately. A healthy completion rate can coexist with a cleanup failure, and a tidy bucket can coexist with users timing out at the browser. Those are different symptoms.

Test the transitions. Send an avatar twice and verify that the old key is deleted only after the new object is confirmed. Interrupt a large artifact after several parts, resume it, and then abandon it; verify that expiry removes temporary state without deleting a completed object. Run deletion from the application record and verify that retries are idempotent. These tests matter more than demonstrating that a happy-path upload works once.

My decision rule is therefore modest: single upload for bounded avatar files, multipart for large training artifacts only after throughput and interruption measurements show a gap, and an explicit retention record for both. That keeps the design aligned with the SLO instead of with the protocol's vocabulary. I've found that this wording also gives reviewers a useful question: which state are we adding, and what deletes it?

## Further reading

- AWS S3 documentation on presigned uploads: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- GDPR Article 17, right to erasure: https://gdpr-info.eu/art-17-gdpr/

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://gdpr-info.eu/art-17-gdpr/
