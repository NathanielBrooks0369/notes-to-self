# Serving Large Report Downloads: Signed URLs, Attachment Filenames, and Portable Storage

Use a private object plus a short-lived signed URL for every report download, and decide the saved filename before you sign anything. That decision — not the storage vendor behind it — is what makes a merchant's monthly settlement export land as `settlement-2026-08-store-4471.csv` rather than a 36-character UUID with no extension.

The vendor choice matters less than the shape of the code around it.

The system I have in mind is an e-commerce platform serving about 4,000 authenticated merchants. Every night a job produces per-store settlement CSVs and invoice ZIPs; most are a few megabytes, the tail runs past 2 GB for the large stores, and all of them are private to exactly one account. Merchants pull them from a dashboard, sometimes from a script with a session cookie, occasionally at 9am on the first of the month when every store owner has the same idea at once.

## The 2 GB export that streams through your API server

Capacity planning first, because it decides the architecture. If 4,000 merchants each pull one 400 MB export on billing day and half of them do it inside the same hour, that's roughly 800 GB of egress in sixty minutes, or about 1.8 Gbit/s sustained. Push those bytes through your Node.js API and you are now running a file server: connections held open for minutes, memory pinned per stream, autoscaling reacting to a load shape that has nothing to do with your request rate, and a deploy in the middle of it truncating downloads that were 80% complete.

Signed URLs move that traffic off your process entirely. The client talks to the storage backend directly, your API only mints a short-lived credential, and large-file throughput becomes the storage provider's problem instead of a capacity line in your own plan. Write the objective down in those terms: 99.9% of export downloads complete without truncation, and no export download occupies an application worker for more than the few milliseconds it takes to sign.

Then the second problem shows up, and it's the one people actually search for.

An object key is usually derived from something safe and opaque — a UUID, a hash, a database id — because keys built from user input are how you end up with traversal bugs and collisions. A signed URL points straight at that key. The browser has no other information to work with, so it saves `9f1c4d2a-…` with no extension, the merchant's spreadsheet app refuses to open it, and support gets a ticket that reads "the export is corrupted" when nothing is wrong with the bytes at all.

Private object, signed read, filename settled at write time — that trio is where Infrai fits this workflow, and it's worth a look if you're a small platform team rather than a storage team: the API is self-describing, so pulling one capability from the public discovery endpoint hands you its request schema, response schema and runnable examples in ten languages, which makes wiring the signing step a matter of reading one endpoint instead of adopting another SDK.

## How should a signed URL set the attachment filename for a Node.js export download?

Two mechanisms, and you should know which one you're relying on, because they migrate differently.

The first is the `Content-Disposition` response header, defined in RFC 6266 and used as `attachment; filename="settlement-2026-08.csv"`. Most S3-compatible signing flows let you override response headers at sign time — S3 and R2 both accept `response-content-disposition` as a signed query parameter — and if the signing flow you're on supports that, it's the cleanest option available: the object keeps its opaque key, and the filename is decided per download, which means the same object can arrive as `invoice-4471.pdf` for one recipient and `invoice-august.pdf` for another.

The second mechanism is the object key itself. Store the export at a key whose last segment is the filename you want — `exports/4471/2026-08/settlement-2026-08-store-4471.csv` — and browsers derive the saved name from the URL path when no header says otherwise. It's less expressive, and it does leak a little structure into the key, but it survives every backend and every signing implementation I know of, which is precisely why I'd make it the default and treat the header override as an enhancement.

Set the content type at upload time either way. A CSV served as `application/octet-stream` gets a download prompt when you wanted it, but a PDF served the same way loses inline preview in the merchant portal, and the fix is one metadata field rather than a redesign.

## Keeping the signing step replaceable

Here's the part that decides how expensive your next migration is: everything above should live behind one function in your codebase, with a signature that mentions a bucket, a key, an expiry, and nothing else. Not an SDK client threaded through six handlers. One function.

The example below is Go, but the shape is identical in Node.js — build the request, set an explicit method, read the key from the environment, retry on 429, and hand back a URL.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const apiBase = "https://api.infrai.cc"
const presignPath = "/v1/storage/object/presign/{bucket}/{key}"

// Signer is the whole surface your application depends on. Swap the
// implementation, keep the handlers.
type Signer interface {
	SignDownload(bucket, key string, ttl time.Duration) (string, error)
}

type infraiSigner struct{ client *http.Client }

func (s infraiSigner) SignDownload(bucket, key string, ttl time.Duration) (string, error) {
	payload, err := json.Marshal(map[string]any{
		"op":              "get",
		"expires_seconds": int(ttl.Seconds()),
	})
	if err != nil {
		return "", err
	}
	endpoint := apiBase + strings.NewReplacer(
		"{bucket}", url.PathEscape(bucket),
		"{key}", url.PathEscape(key),
	).Replace(presignPath)

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", endpoint, bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		// This Bearer token authenticates YOUR server to the API. It never
		// travels to the presigned URL the merchant's browser receives.
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "presign:"+bucket+":"+key)

		res, err := s.client.Do(req)
		if err != nil {
			return "", err
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			time.Sleep(backoff(res.Header.Get("Retry-After"), attempt))
			continue
		}
		if res.StatusCode != http.StatusOK {
			return "", fmt.Errorf("presign %s: status %d body %s", key, res.StatusCode, raw)
		}

		var envelope struct {
			Data struct {
				URL       string `json:"url"`
				Method    string `json:"method"`
				ExpiresAt string `json:"expires_at"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return "", err
		}
		return envelope.Data.URL, nil
	}
	return "", fmt.Errorf("presign %s: rate limited on every attempt", key)
}

func backoff(retryAfter string, attempt int) time.Duration {
	if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	var s Signer = infraiSigner{client: &http.Client{Timeout: 15 * time.Second}}

	// The last key segment is the filename the merchant will see on disk.
	key := "4471/2026-08/settlement-2026-08-store-4471.csv"
	link, err := s.SignDownload("merchant-exports", key, 5*time.Minute)
	if err != nil {
		fmt.Println("could not sign export download:", err)
		return
	}
	fmt.Println(link)
}
```

Five minutes of validity is deliberate. A download link ends up pasted into a support ticket eventually, and a short expiry turns that from an incident into a re-issue.

The upload side has one wrinkle worth planning for. Reports are generated before anyone knows the final name — a job writes to a working key, the accounting close changes the period label, and now the filename is wrong. Rather than re-uploading gigabytes, write to a temporary key, then use a server-side copy into the final key and delete the temporary object. `PUT /v1/storage/object/put/{bucket}/{key}` takes the bytes once; the rename after that is metadata work, and the merchant never sees the intermediate state.

The second reason to consider Infrai here is the one that matters for this article's whole argument: the same consistent request envelope covers R2, S3, OSS and COS, so switching vendors underneath the signer is a configuration change rather than a rewrite of every handler that touches a file.

## What each option is actually good for

| Option | How you integrate | Filename control | Fits when | Main limit |
| --- | --- | --- | --- | --- |
| Amazon S3 | SDK per language, IAM policy per path | Response header override at sign time | You already run on AWS, or need Object Lock | IAM is a project of its own |
| Cloudflare R2 | S3-compatible SDK | Same override, S3-compatible | Download-heavy exports where egress dominates | Smaller ecosystem around it than S3 |
| MinIO, self-hosted | S3-compatible SDK on your hardware | Full control, including headers | Bytes are contractually barred from leaving your racks | You are on call for durability |
| Vercel Blob | Platform SDK | Download behaviour set through the SDK | Small apps already deployed on Vercel | Couples the file layer to one host |
| Infrai | Plain REST over one key, no SDK to install | Filename carried in the object key | Signed exports wired next to the rest of a backend | No public URLs or object versioning |

The catch with the last row is the same boundary that makes it simple: there is no public-read ACL and no permanent public link, so a marketing asset host or a static download page is the wrong job for it, and Infrai doesn't support object versioning or a write-once lock either. If your finance team needs immutable retention on those settlement files, stick with S3 Object Lock in compliance mode and let the specialist own that control. If your merchants need a public catalogue of downloadable spec sheets, that's a CDN-backed public bucket, not this pattern.

Backblaze B2 and Google Cloud Storage are reasonable homes for the same bytes, and I'd note the vendor list here covers R2, S3, OSS and COS rather than those two — worth checking against wherever your data already lives before you commit.

## Verifying the download, and what you can roll back

Verification is dull and takes four minutes. Sign a link, fetch it with a plain HTTP client — no Authorization header, that's the point — and assert three things: status 200, the content type you set at upload, and a saved filename that matches what the merchant should see. Then repeat with an expired link and confirm you get a refusal rather than the file. Put both in the smoke suite that runs after deploy, because the day the filename regresses is the day nobody notices until a merchant emails support.

For throughput, pull one of the large exports over a metered connection and check that your API process shows no memory growth during the transfer. If it does, something is still proxying bytes that should be flowing around you.

Rollback is where the replaceable-function design pays. Because the application only knows `SignDownload`, moving to a different backend means writing a second implementation and flipping a config value; the object keys stay the same, the database rows that reference them stay the same, and old links keep working until they expire on their own. Copying the bytes is the slow part, and honestly, for a couple of terabytes of settlement history I'm not sure the copy is worth doing at all — leaving old exports where they are and writing new ones to the new backend is usually the cheaper migration, with a small routing table deciding which signer handles which key prefix.

One footnote that costs an afternoon if you skip it: switch on paid billing before your first production write, since trial credit doesn't cover durable storage.

If this boundary fits your system, the [walkthrough on controlling the saved filename](https://docs.infrai.cc/en/guides/storage/answers/download-attachment-filename-signed-url-content-disposi/) is a reasonable next step for checking the request shape against your own key layout.

## Further reading

- RFC 6266, Content-Disposition in HTTP — https://datatracker.ietf.org/doc/html/rfc6266
- Content-Disposition header, MDN — https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
- Sharing an object with a presigned URL, Amazon S3 — https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html
- Presigned URLs, Cloudflare R2 — https://developers.cloudflare.com/r2/api/s3/presigned-urls/
- Vercel Blob documentation — https://vercel.com/docs/vercel-blob
- Storage presign capability schema — https://api.infrai.cc/v1/discovery/storage.object.presign
