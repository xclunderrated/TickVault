# Reliable downloads and rate-limit prevention plan

Date: 2026-10-12. Status: client controls implemented in local 0.3.0 after the
[review](REVIEW.md). The deterministic gates pass, including minimum supported
dependencies. The one-GET live access gate still received HTTP 429; provider
access and a confirmed quota remain open. See [validation](VALIDATION.md).

| Stage | Current result |
| --- | --- |
| Integrity/configuration/tool floors | R1/R6/R7/R8/R9 repaired and tested |
| Shared coordinator/headers | Persistent pacing, rolling budgets, cooldowns, one recovery probe and bounded streaming implemented |
| Hour ownership/jobs/status | Shared validated receipts, heartbeat leases, resumable jobs and failure JSON implemented |
| Deterministic gates | Passing offline process, transport, cancellation, source identity, storage and CLI tests |
| Live payload gate | One GET received 429; cooldown persisted, no further GETs |
| Optional delivery routes | Evaluated in [delivery options](DELIVERY_OPTIONS.md); paid/undocumented routes remain gated |

## Goal and limits

Prevent avoidable throttling, obey observed provider restrictions, keep completed
data durable, and turn ordinary interruptions into resumable job states. A server,
network, operating system or disk can still fail. No client can guarantee that an
external provider never sends 429, particularly without a verified quota contract.

For a fully cached and validated range, the firm guarantee is narrower and useful:
reading it makes zero provider requests and therefore cannot receive a provider
rate-limit response. Incomplete ranges must remain visibly incomplete.

HTTP 429 does not specify how the provider identifies a client or counts requests.
IP changes may help an IP-scoped limit, but cannot establish that an account,
endpoint or whole-service limit permits the next request.
[RFC 6585](https://www.rfc-editor.org/rfc/rfc6585#section-4).

## 1. Fix integrity and access prerequisites

- Repair export lost updates and atomic `fail` publication (R1/R7) before allowing
  competing exports. Acquire destination ownership before taking a snapshot and
  retain it through publication; coordinate readers on Windows. SQLite may use a
  transaction in the destination to avoid replacing an entire database snapshot.
- Recover valid raw files regardless of prior no-data metadata (R6). Distinguish
  absent-at-last-check, failed, pending and confirmed downloaded outcomes.
- Make settings and logger changes atomic (R8). Validate paths before replacing
  the working configuration. Optional logging failure must not destroy the vault.
- Align tool/dependency floors and CI with actual supported configurations (R9).
- Preserve the recorded live-access uncertainty. Verify one aligned complete
  historical hour, one known instrument, one GET, zero automatic retries, after
  any applicable cooldown. Stop on 429 or denial. This is an access canary, not
  a search for the provider's breaking point.
- Record sanitized status, Date, Retry-After, response origin/proxy indicators,
  payload format and tick count. Identify the actual network route. No credentials,
  proxy passwords or unbounded response bodies belong in diagnostics.
- Obtain a published or provider-confirmed quota for the chosen delivery method,
  including whether other apps sharing this account/network consume that quota.

The implemented defaults enforce those client controls. `canary` performs at
most one governed GET with zero retries. Uncached multi-hour work requires a
source-specific validated payload; cached ranges remain available during a
cooldown. The initial ceilings are conservative client policy, not provider
approval of large downloads.

## 2. One shared, persistent provider coordinator

Introduce a coordinator shared by symbols, library calls, CLI processes and vault
directories. A dedicated local owner or transactional shared state can implement
it; run-local controllers must not be the authority for provider restrictions.

The coordinator owns:

- A request-start rate, short/long rolling-window budgets and an aggregate
  in-flight cap. Every real attempt consumes admission, including retries, 404s,
  probes and proxy routes. Budget accounting must not refund sent requests.
- A burst limit of one initially. Proposed pilot settings are one request in
  flight and one request start every two seconds. These are conservative design
  settings, not a documented Dukascopy quota or a zero-429 guarantee. Activation
  depends on the access gate and any stricter confirmed provider policy.
- Durable next-request time, server cooldown deadline, recent throttle history,
  and circuit state. Restarting an app, changing symbols, selecting another vault,
  or switching a route must not silently reset an observed restriction.
- A stable provider/account/declared-network-group identity. Where quota scope is
  unknown, all configured routes share the conservative provider budget. Other
  applications need to participate, or their traffic remains an external risk.
- Immutable configuration per job, avoiding global settings changes during a run.

Adaptive behavior may reduce rate/capacity after trouble. It must not raise the
budget merely because responses are fast. Increases require a confirmed policy
and controlled evidence, while remaining within that policy's ceiling.

## 3. Respect headers before reading bodies

Fix R4 by receiving response headers through streaming access. Persist a 429 or
503 Retry-After deadline immediately; a slow or broken error body must not change
that into an ordinary network retry. Close error responses without reading an
unlimited body. Read successful bodies with configurable size/time bounds and
retain BI5 record/decompression validation.

Parse both seconds and HTTP dates, accounting for server Date and clock skew.
Wait at least the provider's deadline. Add only nonnegative jitter afterward to
avoid synchronized recovery bursts. Persist a long delay even if the current
call's time budget expires.

When Retry-After is absent or invalid, enter a durable conservative cooldown and
open the circuit instead of starting an immediate retry wave. A proposed initial
fallback is 15 minutes; it is a client recovery policy, not a known server limit.
After a deadline, admit exactly one recovery probe. Another throttle extends the
pause and reduces the budget. Authentication/permission denial requires a clear
actionable state, not repeated route changes.

## 4. Download each requested hour once

Fix R5 using one pending operation per provider/symbol/hour. Overlapping callers
subscribe to that result. Across processes, use leases with expiry, owner identity
and recovery so a crash does not strand a job. Recheck validated cache completion
after claiming an hour and before consuming a request admission.

Publish the validated raw file first, then persist its metadata receipt. Recover
valid files after crashes from absent, failed and prior no-data states. One
subscriber cancelling should release its interest while preserving other readers.

The implementation permits one active owner per source/symbol/hour. Cancelling
a waiter does not cancel the owner. If the owner exits before publishing a
validated receipt, a remaining caller may take ownership and fetch again under
the same budget. A process crash cannot guarantee exactly one lifetime GET of
an unfinished hour; completed receipts avoid repeat requests.

Improve negative caching using verified instrument coverage and calendars, where
the source supplies them. Do not assume all symbols close on weekends or that
every historical 404 proves a permanently empty hour. Recent publication delays
and older confirmed immutable coverage need different recheck policies.

No verified per-instrument calendar/coverage contract was available for this
route. Current negative results expire after one day or an explicit recheck.
Instrument-specific permanent negative caching remains deferred to that evidence.

## 5. Make common failures resumable states

Add a managed-job API with explicit states:
`queued`, `running`, `waiting_for_provider`, `waiting_for_network`,
`paused`, `complete`, and `needs_action`.

A normal 429 or temporary outage retains progress, releases resources, stores the
next permissible attempt and returns/updates a waiting state. It does not erase
successful hours, spin indefinitely or claim a complete range. Preserve the old
strict API's error contract where callers depend on it.

CLI JSON must always report the job state, completeness, completed/pending/failed
counts, retry reason and next eligible time, including actual failed runs (R10).
Returning success for accepted deferred work must be distinguishable from success
for a complete range. Corrupt data, invalid credentials and permanent storage
failures remain visible; hiding them cannot make the output trustworthy.

## 6. Creative ways to reduce dependence on the hourly feed

### Local mirror and planned coverage

Maintain a user-owned validated archive. Serve charting, backtests and exports
from it. Download only missing coverage, prioritize the active range, and schedule
background preparation within the same budget. After requested history is cached,
interactive use consumes no provider quota.

### Bulk delivery

[Dukascopy documents a Requester Pays S3 export](https://www.dukascopy.com/wiki/en/development/data-export/)
requiring AWS credentials, eu-west-1 and acceptance of requester charges. Verify
coverage, object layout, daily timebase, price/volume units and costs before
implementing a separate adapter. Where verified daily files replace hourly files,
they can reduce request count substantially. Do not treat the S3 protocol as an
hourly URL substitution, guarantee unthrottled service, or initiate paid transfers
without the user's billing authorization.

### Explicitly permitted route fallback

Support stable user-supplied routes if the provider's policy permits them. Keep
per-route health/cooldown state and a shared provider/account budget. Fallback can
recover a broken connection. A 429 permits another route only when a verified
policy establishes a genuinely independent allowance; otherwise the provider
cooldown still governs it. A route swap is never evidence that a quota reset.

### Independently authorized sources

For continuity requirements, evaluate an independently authorized second source
or delivery contract. Keep provider identity, exact bid/ask behavior, timestamps,
coverage and scaling attached to every dataset. Do not silently combine another
feed with Dukascopy ticks or label it as Dukascopy data. Compare a bounded real
sample before claiming equivalence.

### Verify protocol changes before adapting

The reported Jetta endpoint uses JSON rather than the legacy hourly BI5 layout.
Study the current vendor or maintained-library contract and prove pagination,
units and date bounds with a bounded permitted sample before selecting it. A
historical memory note or changed URL alone is not a current integration proof.

## 7. Acceptance gates

Use a fake clock and controlled transports for deterministic admission tests.
Use temporary synthetic vaults for failure/concurrency checks. Live checks remain
small and stop after a throttled response.

| Gate | Required evidence |
| --- | --- |
| Provider access | One real validated payload through the selected authorized route and protocol |
| Request pacing | All attempts respect spacing, burst, rolling-window and aggregate limits |
| Restart safety | No request before a stored one-hour cooldown after process restart |
| Shared control | Concurrent symbols, separate CLI processes/vaults and proxy routes share the budget |
| Deduplication | Many subscribers to the same hour cause one request, including crash/lease recovery |
| Header handling | A 429 with a stalled body preserves Retry-After and causes no early retry |
| Recovery probing | Exactly one probe is admitted after an open-circuit deadline |
| Export integrity | Competing symbols/appends/fail publications cannot lose accepted data |
| Metadata recovery | Valid published files recover from absent, failed and no-data metadata |
| Failure durability | Disk-full/permissions/cancellation/crashes preserve prior outputs and pending work |
| Machine status | JSON is valid and includes state/completeness even when downloads are deferred/failed |
| Offline operation | A complete cached range causes zero network requests |
| Compatibility | Current and minimum supported dependencies pass appropriate platform checks |

## Recommended implementation order

1. Repair data integrity and configuration publication (R1/R6/R7/R8), and fix tool floors.
2. Build the shared durable coordinator, pacing and immediate header classification (R2/R3/R4).
3. Add hour ownership/deduplication, durable jobs and structured outcomes (R5/R10).
4. Run the deterministic gates, then the single permitted real-payload access gate.
5. Evaluate optional bulk delivery, authorized route fallback and a second source
   according to verified coverage, policy and cost.

No stage is complete solely because the previous suite passes. The stronger
guarantees above need their own evidence, and provider availability remains an
external dependency after the client controls are implemented.
