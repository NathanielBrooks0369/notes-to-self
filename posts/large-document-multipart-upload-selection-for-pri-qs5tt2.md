# Large Document Multipart Upload Selection for Private Object Storage

Short answer: send 100 MB PDFs and 1 GB ZIP archives directly to private object storage, use multipart upload when a whole-file retry would be costly, and enforce an expiry for every incomplete upload.

The choice is about recovery and ownership, not a fashionable size cutoff. A 100 MB PDF can be a single request on a stable connection; a 1 GB ZIP should usually be resumable. The moment an application server proxies those bytes, its memory, connection lifetime, rolling deploys, and autoscaling become part of the document's durability story.

No magic threshold.

## What should Node.js select for 100 MB PDFs, 1 GB ZIPs, multipart upload, and private object storage?

For a service that stores user documents, the practical paths are an application-proxied request, a direct single-object upload, and a direct multipart upload. Node.js should authorize the operation and issue narrowly scoped upload instructions. It should rarely be the data path for a large document.

Multipart is appropriate when a full re-transfer would be expensive to the user or interruption is common. Its value is retry granularity: a failed part can be retried without repeating completed parts. In the S3 multipart model, an upload can contain up to 10,000 parts, and all parts except the final part must meet the documented minimum size. Those constraints turn part size into capacity planning. Pick a fixed part size that leaves room for expected growth, set bounded concurrency, and test it under the deadline your SLO can afford.

A single-object request remains sensible for smaller documents when the added state, extra requests, and cleanup duty of multipart create more operational work than they remove. **Use a threshold derived from observed retry cost and client failure rates, then revisit it as file distributions change.** PDF versus ZIP is a weak signal; a mobile connection, a corporate proxy, and an export job with a hard deadline are stronger ones.

| Path | Where bytes travel | Recovery unit | Operational obligation | Appropriate condition |
|---|---|---|---|---|
| App-proxied upload | Application process | Entire request | Drain connections during deploys; size the tier for concurrent bodies | Inline inspection is required before storage |
| Direct single-object upload | Client to private storage | Entire object | Restrict credentials and verify completion | Small files and reliable clients |
| Direct multipart upload | Client to private storage | One part | Track upload state and expire incomplete uploads | Large or interruption-prone transfers |
| Self-hosted storage | Client or platform network | Depends on implementation | Operate capacity, durability, upgrades, and on-call response | Residency or control requirements justify it |

The catch is that multipart is not a free reliability switch. It adds an upload identifier, part ordering, finalization, and incomplete-upload lifecycle. Stick with a single-object path when documents are small, clients are reliable, and the team cannot staff those controls. Use a quarantined private location followed by promotion when content must be scanned or structurally validated before it is readable; direct upload alone does not answer that policy requirement.

## The incident lesson: unfinished uploads need an owner and a deadline

The failure pattern worth designing against is ordinary. A browser uploads several parts, the user closes the tab or the connection changes, and no process remains to issue cleanup. Completed parts are not a completed object, but they are still state that must be governed.

A client-side abort is useful, yet it cannot cover every interrupted process, device sleep, or cancelled request. Cleanup therefore belongs to storage policy rather than a best-effort handler in Node.js. Configure the private bucket to abort incomplete multipart uploads after a period aligned with the supported resume window. The period should be longer than a legitimate pause but short enough to bound retained partial data. Alert on the count and age of active uploads; storage-byte graphs are slow signals, while old incomplete uploads name the control failure directly.

Consider the sequence behind a seemingly harmless retry: the control plane creates a session for `private/account/document.zip`, records its expiry, and grants the client authority only for that session's object and duration; the client sends parts and retains the identifiers needed to resume; the finalization request checks the recorded subject and expected key before treating the object as available. If the client disappears before finalization, there is no document record to publish, while the bucket lifecycle performs eventual cleanup. If the client resumes before expiry, it can continue from confirmed part state rather than making the application tier reconstruct a 1 GB request. These are separate transitions, and keeping them separate is what prevents an interrupted transfer from becoming either an accidental public object or permanent unowned storage.

Short, but consequential.

There is a second ownership question. The authorization service should bind each upload to an authenticated subject, an expected object key, a content-type policy, a maximum size, and an expiry. On completion, record the object only after storage confirms the final object, then make it available through an authorization check rather than a public URL. Private storage is a permission model carried through the whole flow, not a bucket checkbox.

A team can test this without inventing a production outage. Start an upload, interrupt it at different boundaries, retry a single part, retry the entire upload, wait past the expiry, and verify that the object is either finalized with expected metadata or absent. Run the same cases while draining an application instance. The result gives the platform team an actionable SLO: a completed document must be retrievable only by its authorized owner, and abandoned transfer state must disappear within its declared retention window.

## A small Go control-plane example

The data plane differs across object-storage implementations, but the policy boundary should stay narrow. This example models the application-side decision: it creates an expiring upload session, refuses paths outside the private prefix, and never accepts the document body through the API process. The storage adapter is an interface, so the workflow remains testable and portable.

```go
package intake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const multipartThreshold = int64(100 * 1024 * 1024)

type UploadMode string

const (
	SinglePart UploadMode = "single"
	Multipart   UploadMode = "multipart"
)

type Session struct {
	ID        string
	ObjectKey string
	Mode      UploadMode
	ExpiresAt time.Time
}

type StorageControl interface {
	CreateUpload(ctx context.Context, key string, mode UploadMode, expiresAt time.Time) (string, error)
}

func Start(ctx context.Context, control StorageControl, accountID, name string, size int64, now time.Time) (Session, error) {
	if accountID == "" || name == "" || size < 0 {
		return Session{}, errors.New("invalid upload request")
	}

	key := fmt.Sprintf("private/%s/%s", accountID, name)
	if !strings.HasPrefix(key, "private/") {
		return Session{}, errors.New("object key is outside the private prefix")
	}

	mode := SinglePart
	if size >= multipartThreshold {
		mode = Multipart
	}

	expiresAt := now.Add(30 * time.Minute)
	id, err := control.CreateUpload(ctx, key, mode, expiresAt)
	if err != nil {
		return Session{}, fmt.Errorf("create upload session: %w", err)
	}

	return Session{ID: id, ObjectKey: key, Mode: mode, ExpiresAt: expiresAt}, nil
}
```

The threshold is intentionally a configuration candidate, not a universal fact. A service with stable wired clients can set it higher; a field workflow with intermittent connections may set it lower. Measure completion time, retry counts, abandoned-session age, and authorization failures separately. Don't fold them into a single success metric, because a fast transfer that later exposes a document to the wrong principal is still a failed system.

## Capacity, security, and the buy-versus-build boundary

Capacity planning starts with the concurrent transfer count, selected part size, and maximum in-flight parts per client. Those values bound the network and request-rate pressure placed on storage, while the app tier mainly handles short authorization and completion requests. Set explicit limits before a large export arrives: maximum object size, maximum active sessions per account, session TTL, and a tenant-aware quota. Treat each limit as an admission-control rule, then publish it to the client rather than letting an upload discover it after consuming bandwidth.

For private objects, least-privilege credentials should permit only the expected operation, key scope, and lifetime. Completion is a state transition, so make it idempotent: a retry should return the recorded terminal result rather than create a second document record. Store immutable metadata needed for later access decisions, and log the actor, object identifier, state transition, and request correlation identifier. Avoid logging document names when they contain sensitive data.

Self-hosting can be appropriate when residency, isolation, or predictable high utilization outweigh the operating burden. The trade-off is not a storage feature checklist; it is the commitment to capacity replacement, replication testing, key management, incident response, and upgrade work. A managed object store can reduce that burden, but it does not remove application ownership of authorization, lifecycle policy, auditability, or recovery testing.

## A release checklist that catches quiet failures

Before enabling large private uploads, exercise the design as a set of failure cases rather than a happy-path demo:

- Verify a user cannot write outside their private prefix or read another user's object.
- Interrupt a multipart transfer and confirm the retry path preserves completed parts only within the supported session lifetime.
- Confirm the lifecycle policy removes incomplete work after the declared retention window.
- Drain or restart the authorization service during an upload and verify that direct data transfer is unaffected.
- Measure completion latency and abandoned-session age against separate SLOs.

This is not a recommendation for a particular storage provider. It is a way to make 100 MB PDFs and 1 GB ZIPs boring: select retry boundaries deliberately, keep document bytes away from the general API tier, and give every unfinished upload a verifiable owner and deadline.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html
- https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager
- https://developers.cloudflare.com/r2/
- https://developer.mozilla.org/en-US/docs/Web/API/Blob/slice
- https://www.fedramp.gov/
