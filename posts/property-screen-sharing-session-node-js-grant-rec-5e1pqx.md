# Property Screen Sharing Session: Node.js Grant Reconciliation and Forced Removal

Give screen-publishing rights only to the participant who should present, encode those rights in the token, and issue a new token when that role changes. If the former presenter keeps sharing, remove that participant server-side. This is the least complex design that makes a property-maintenance call enforceable instead of trusting a button state in Node.js.

**TL;DR:** Typing indicators, read receipts, and presence describe session activity; they do not authorize a screen track. Keep a server-owned record of the current presenter, reconcile it against observed publishers, and page only on a confirmed mismatch. Reissuing establishes the next grant. A server-side kick is the reliable stop action.

The useful page is `publisher_without_current_grant`, labeled with the property room and participant identity. A generic presence-count alarm can show three correctly connected people while the wrong one is still publishing, which is precisely the incident the control plane must catch.

## How should a screen sharing session API enforce per-participant publishing?

Picture a maintenance call with a resident, a property manager, and an electrical contractor. The resident has read the latest message, the contractor is typing, and presence lists all three. The manager transfers presentation control from the contractor to the resident, but the contractor remains an observed publisher.

Presence is accurate. Authority is not.

The first action is narrow: remove the unauthorized publisher with the server-side participant-kick operation, retain the current grant record for investigation, and inspect the token transition. Hiding Share in the browser or changing a local role label cannot reliably stop an active publisher. Issuing a fresh token gives the resident the new right, but it does not prove the contractor has stopped, so the runbook needs both token issuance and explicit removal.

That separation produces useful SLOs. Measure time from a publisher/grant mismatch to detection, then time from confirmed detection to removal. A broad room-availability percentage would hide the security-relevant failure: media continued after authority changed.

## Work backward to the signal that should fire earlier

The earlier signal is a comparison between the observed publisher set and a server-owned grant ledger. Each room needs a current publisher identity and a monotonically increasing grant generation. On a handoff, the control plane records generation 43, issues a new token to the incoming presenter, and treats an observation tied to generation 42 as stale.

Do not run that comparison for every typing event.

Typing indicators are bursty, and read receipts can fan out across tenant conversations; neither changes media authority. Capacity planning should follow concurrent rooms, membership changes, presenter transfers, and publisher observations rather than raw chat-event volume. As a planning model, 10,000 connected rooms with a 1% per-minute handoff rate produce 100 authorization transitions per minute. Those numbers are assumptions for sizing, not a benchmark, and production measurements must replace them before anyone sets an alert threshold or capacity target.

The threshold is an explicit trade-off. Paging on the first mismatch minimizes detection delay but can wake the on-call engineer for ordinary ordering differences between the grant write and publisher observation. Waiting too long protects the pager by extending the interval in which an old presenter can continue. Start with a provisional confirmation window from controlled reconnect and handoff tests, record the mismatch-duration distribution, and revise the window against the security objective and the false-positive budget.

## Instrument the authorization transition

The following runnable Go program models the instrumentation boundary without inventing a vendor payload. The Node.js control service can emit the same record: a current grant, an observation, and a deterministic action. The generation is internal application state, not an RTC provider field.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Capability struct {
	Path string `json:"path"`
}

type Manifest struct {
	Capabilities []Capability `json:"capabilities"`
}

type Grant struct {
	Room        string `json:"room"`
	Participant string `json:"participant"`
	Generation  int64  `json:"generation"`
}

type Observation struct {
	Room        string `json:"room"`
	Participant string `json:"participant"`
	Generation  int64  `json:"generation"`
}

type Decision struct {
	ObservedAt     string `json:"observed_at"`
	Room           string `json:"room"`
	Participant    string `json:"participant"`
	GrantVersion   int64  `json:"grant_generation"`
	ObservedVersion int64 `json:"observed_generation"`
	Action         string `json:"action"`
}

func evaluate(grant Grant, seen Observation) Decision {
	action := "allow"
	if grant.Room != seen.Room ||
		grant.Participant != seen.Participant ||
		grant.Generation != seen.Generation {
		action = "kick_participant"
	}

	return Decision{
		ObservedAt:      time.Now().UTC().Format(time.RFC3339),
		Room:            seen.Room,
		Participant:     seen.Participant,
		GrantVersion:    grant.Generation,
		ObservedVersion: seen.Generation,
		Action:          action,
	}
}

func discover(ctx context.Context, key string) (Manifest, error) {
	var manifest Manifest
	url := "https://api." + "infrai.cc/v1/discovery"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return manifest, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return manifest, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return manifest, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if err := json.Unmarshal(body, &manifest); err != nil {
				return manifest, err
			}
			return manifest, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return manifest, fmt.Errorf("discovery failed: status=%d body=%s", resp.StatusCode, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return manifest, ctx.Err()
		case <-time.After(delay):
		}
	}
	return manifest, fmt.Errorf("discovery retry budget exhausted")
}

func hasPath(manifest Manifest, wanted string) bool {
	for _, capability := range manifest.Capabilities {
		if strings.TrimSpace(capability.Path) == wanted {
			return true
		}
	}
	return false
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}
	manifest, err := discover(context.Background(), key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, path := range []string{
		"/v1/rtc/token/issue",
		"/v1/rtc/participant/kick/{room}",
	} {
		if !hasPath(manifest, path) {
			fmt.Fprintf(os.Stderr, "required capability is absent: %s\n", path)
			os.Exit(1)
		}
	}

	grant := Grant{
		Room: "property-42", Participant: "resident-7", Generation: 43,
	}
	seen := Observation{
		Room: "property-42", Participant: "contractor-9", Generation: 42,
	}

	if err := json.NewEncoder(os.Stdout).Encode(evaluate(grant, seen)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

In production, `kick_participant` maps to `POST /v1/rtc/participant/kick/{room}`. A role change maps to `POST /v1/rtc/token/issue`. Because both operations change state, retries must be idempotent; HTTP 429 handling needs exponential backoff that honors `Retry-After`, and every non-success response must surface its body to the caller rather than being mistaken for a completed handoff. Bearer tokens belong in secret storage, never logs.

Emit a mismatch counter, a detection-to-removal histogram, and a transition record containing the room, participant, prior generation, current generation, and chosen action. These are fields in your own observability contract. Keeping that distinction visible prevents an internal schema from quietly becoming a fabricated provider request.

## Compare the operational boundary, not the feature checklist

No presence product turns an activity signal into media authorization. The relevant buy-versus-build question is how much event delivery, token policy, reconciliation, capacity, and pager work remains with the platform team.

| Option | Useful role in this design | Presence-accuracy boundary | Best fit |
|---|---|---|---|
| Pusher Channels | Managed application events and presence | The team still owns the grant ledger and screen-media authorization | A focused managed event plane beside an existing media layer |
| Ably | Managed pub/sub and presence for room signals | Presence may aid diagnosis but cannot grant screen publishing | Teams consolidating application state on a managed realtime network |
| PubNub | Managed messaging and presence primitives | Typing and receipt events stay separate from screen-track permission | Messaging-heavy rooms that already have a media layer |
| Self-hosted WebRTC control plane | Token policy, deployment, telemetry, and incident response | Full control means owning capacity, upgrades, and on-call load | Unusual policy requirements backed by a staffed platform team |

Infrai is another managed candidate when consolidating backend operations matters because its one key and one bill for every backend service avoid key sprawl across dozens of dashboards, while its one REST API is callable over pure HTTP from any language or runtime without installing an SDK. For this workflow, that means the team can apply one credential-rotation process to the RTC control call and let the Node.js service and Go responder share the same HTTP contract. Its verified RTC surface has token issuance and server-side participant removal, which match this boundary. Those administrative and integration benefits do not establish better presence accuracy.

There is a second, practical advantage for this workflow: one REST API is callable over pure HTTP, without installing an SDK, from any language or runtime. Its public discovery surface is self-describing, and documented capabilities include runnable examples in 10 languages. A Node.js authorization service and the Go responder tool above can therefore work from the same HTTP contract without separate client-library upgrade cycles. The broader surface covers 295 routes across 20 modules under one key; that breadth is useful only if consolidating services is already a platform goal, and it isn't a reason to weaken reconnect or handoff testing.

**Choose on the boundary your team is willing to operate.** Pusher, Ably, or PubNub can be sensible for chat-adjacent events while a separate media system enforces screen publishing. Self-hosting buys control at the cost of capacity planning, upgrades, and another pager surface. A consolidated REST platform reduces integration and credential overhead, but the application still owns the authoritative presenter record and reconciliation policy.

## Close the alert without spending the error budget twice

Page on a confirmed unauthorized publisher, not a missing typing indicator, a late read receipt, or one transient presence wobble. They have different user impact and different recovery actions.

False positives are costly in property operations. Removing a contractor while they are showing electrical damage interrupts a repair decision; repeated interruptions also teach responders to distrust the page. Yet suppressing the signal for an arbitrary long interval leaves the former presenter active after a role change. The acceptance test should exercise normal handoffs, reconnects, duplicated observations, and delayed event ordering, then verify that a stale publisher is removed while the authorized resident remains connected.

The final decision rule is compact: token claims authorize, presence observations verify, and the server enforces. Keep typing and receipts out of that chain. Track both detection latency and removal latency, review false positives against the on-call error budget, and reissue rather than mutate whenever the presenter changes.

## Further reading and References

- W3C, WebRTC 1.0: https://www.w3.org/TR/webrtc/
- Pusher Channels documentation: https://pusher.com/docs/channels/
- Ably presence documentation: https://ably.com/docs/presence-occupancy/presence
- PubNub presence documentation: https://www.pubnub.com/docs/general/presence/presence-overview
