# TickVault reliability review

Date: 2026-10-12. Original scope: the local 0.2.0 working tree in `D:\TickVault`,
including core models/settings, HTTP/download supervision, metadata, readers,
SQLite/HDF5 storage, public API, CLI, examples/notebook, dependencies and CI.
The findings below preserve the pre-fix evidence. Local 0.3.0 implements all ten
repairs; their verification is recorded below and in [validation](VALIDATION.md).

Reviewers used controlled HTTP transports and temporary synthetic archives.
No Dukascopy datafeed requests, IP changes, or proxy changes were made. The
previous 148 passing tests and 91% coverage remain the recorded baseline; these
new reproductions show scenarios outside that suite's existing coverage.

## Repair status in 0.3.0

| Finding | Implemented repair | Regression evidence |
| --- | --- | --- |
| R1 | Process destination locks cover snapshot through publication and streamed readers | `test_storage.py`, `test_publication.py` |
| R2 | Shared SQLite admission persists cooldowns, capacity, starts and recovery probes | `test_provider_coordinator.py`, `test_reliability_integration.py` |
| R3 | Start pacing, rolling minute/hour budgets, no budget refund | `test_provider_coordinator.py` |
| R4 | Streaming header classification persists restrictions before reading or closing error bodies | `test_fetcher.py` |
| R5 | Heartbeat-protected hourly ownership and immutable shared validated receipts | `test_provider_coordinator.py`, `test_reliability_integration.py` |
| R6 | Valid nonempty orphan files take precedence over older no-data rows | `test_metadata.py` |
| R7 | CSV destination ownership and atomic hard-link publication for fail mode | `test_cli.py`, `test_publication.py` |
| R8 | Prepare logger resources before publishing configuration; running operations use snapshots | `test_config_atomic.py`, `test_reliability_integration.py` |
| R9 | Compatible dependency/tool floors, fully pinned minimum CI environment | `.github/constraints-min.txt`, local minimum suite |
| R10 | JSON failures retain partial reports; managed jobs expose explicit waiting/action states | `test_managed_jobs.py`, `test_reliability_integration.py` |
| R11 | Shared export destination checks protect provider state, sidecars and shared raw cache | `test_review_hardening.py` |
| R12 | New jobs persist absolute vault and state paths before their first asynchronous write | `test_review_hardening.py` |
| R13 | Bounded cache scans, decompression and metadata commits run on worker threads; cancellation waits for owned disk work | `test_review_hardening.py`, `test_downloader.py` |

Integration review also fixed source identity: each vault is pinned to a feed,
and source-specific access proof is required before uncached bulk requests.
Access checks preserve status/counts for corrupt, oversized and unexpected HTTP
responses. A failed cooldown persistence write remains a visible storage error.
These repairs do not establish live feed availability or a vendor quota.

## Original verified findings

### R1 — P1: Concurrent exports can silently lose successful writes

Locations: [SQLite snapshot](../tick_vault/read/storage.py#L119), publication at
line 152; HDF5 snapshot at lines 246–247 and publication at line 283.

Both exporters copy the current destination, modify their own temporary copy,
and replace the entire destination. No lock covers snapshot through publication.
Interleaved exports of EURUSD and GBPUSD both returned one row written. The
final store contained EURUSD=1, GBPUSD=0, USDJPY=1: one successful export vanished.
This was reproduced independently for SQLite and HDF5.

Required fix: coordinate destination ownership across processes for the entire
operation, not just replacement. Consider updating SQLite in one transaction in
the destination instead of replacing a snapshot. Coordinate readers too so
Windows file sharing does not turn publication into a permission failure.

### R2 — P1: Cooldown and capacity are confined to one download invocation

Locations: [controller construction](../tick_vault/download/api.py#L131),
[initial cooldown](../tick_vault/download/concurrency.py#L27).

Every call constructs its own controller. A failed call discards the deadline.
Two sequential mocked calls receiving `Retry-After: 3600` made requests only
0.0266 seconds apart. Two concurrent symbols with one worker each produced two
simultaneous requests. Restarting a CLI also has no persisted cooldown to load.

Required fix: one durable provider budget shared by calls, symbols, proxies,
processes and vault directories. Preserve server deadlines after job failure.

### R3 — P2: Concurrency does not limit requests per second

Location: [capacity admission](../tick_vault/download/concurrency.py#L40).

There is no request-start interval, rolling-window budget or burst budget.
One worker initiated 24 mocked missing-hour requests in 0.015 seconds. Reducing
worker count alone therefore cannot prevent a request-rate limit.

Required fix: admission pacing for every network attempt, including retries,
empty-hour checks and access probes, independently of the concurrency limit.

### R4 — P2: A stalled 429 response body hides its rate-limit headers

Location: [buffered request](../tick_vault/download/fetcher.py#L101), followed by
status handling at lines 116–120.

The HTTP client reads the entire response before status inspection. A mocked
429 with `Retry-After: 3600` and a stalled body became a `RetryableError`. Two
requests occurred 0.0273 seconds apart; the final cause lost the cooldown.

Required fix: inspect status/headers using streaming response access, persist a
429 deadline immediately, close error bodies promptly, and bound successful body
size and read time. Diagnostic body capture must not delay respecting headers.

### R5 — P2: Overlapping jobs fetch the same hour twice

Location: [run-local scheduling](../tick_vault/download/api.py#L237).

Two concurrent calls for the same symbol/hour both discover unfinished work.
The reproduction issued two GETs for one unique URL.

Required fix: share pending results inside a process and use expiring ownership
leases across processes. Recheck the cache after acquiring ownership. Cancellation
of one subscriber must not stop work that another subscriber still needs.

### R6 — P2: Prior no-data status hides a valid recovered file

Location: [no-data precedence](../tick_vault/metadata.py#L263), versus the recovery
branch at line 284.

Record no-data, then save a valid one-tick file to model a successful retry that
stopped before metadata commit. Resume produced no pending chunks, metadata
remained no-data, and strict reading returned zero rows despite that valid file.

Required fix: reconcile a valid nonempty local file before applying no-data
expiry. Preserve empty-hour age so revalidation does not postpone its expiry.

### R7 — P2: CSV fail policy has a publication race

Locations: [existence check](../tick_vault/cli.py#L217) and unconditional
replacement at line 257.

With `--if-exists fail`, a reader created the destination after the existence
check. The CLI returned success and replaced that file.

Required fix: atomic publication that cannot replace an existing destination,
or destination ownership that all writers respect. Include competing external
file creation in the acceptance checks, not just two TickVault writers.

### R8 — P2: Failed configuration reload can leave invalid settings active

Locations: [configuration mutation](../tick_vault/core/config.py#L104) before
logger setup at line 108; old handlers removed in logger.py lines 30–32 before
the new file handler can be created at lines 37–38.

Passing an existing file as `base_directory` raised `FileExistsError`, but CONFIG
had already changed to that unusable location. The previous logging setup can
also already have been removed. The reproduction restored its original settings.

Required fix: prepare and validate directories/handlers before committing the
settings, or restore both configuration and handlers on setup failure.

### R9 — P2: Declared minimum development tools are incompatible

Locations: [tool dependencies](../pyproject.toml#L176), pytest config at line 122,
Black target versions at line 72.

pytest-asyncio 0.23 is allowed, but `asyncio_default_fixture_loop_scope` was
introduced in 0.24. With strict configuration, the permitted older version fails
collection. Black 24.1 is allowed, but its target enum ends at Python 3.12 while
this project asks it to recognize Python 3.13 and 3.14.

Evidence: [pytest-asyncio changelog](https://pytest-asyncio.readthedocs.io/en/stable/reference/changelog.html),
[Black 24.1 source](https://raw.githubusercontent.com/psf/black/24.1.0/src/black/mode.py).
Required fix: raise tool floors to versions supporting the actual configuration
and run a compatible minimum-dependency CI job alongside current dependencies.

### R10 — P3: JSON mode produces no JSON for failed downloads

Locations: [download invocation](../tick_vault/cli.py#L339), generic error handler
at lines 401–403; the CLI guide promises a final report at line 21.

A mocked 429 with no retries returned exit 1, empty stdout and plain stderr.
Automation receives none of the advertised failed/completed counts.

Required fix: a structured complete/stopped/deferred/failed result, with partial
progress and retry deadline. Preserve sensible exit codes and clearly distinguish
an accepted deferred job from a completed data range.

## What is already useful

The existing code validates BI5 payloads, publishes raw chunks atomically, uses
bounded queues/ordered decoding, preserves confirmed receipts on ordinary
cancellation, migrates legacy metadata additively, and has meaningful offline
regressions. Same-run retries reacquire capacity and share one cooldown across
that run's proxy clients. These controls should be retained.

The original review found no additional task-lifetime defect in its inspected
cases. The follow-up below extends that coverage; neither review is an error-free
guarantee.

## Follow-up review of the 0.3.0 implementation

The follow-up ran entirely offline, starting from the 250-test baseline. Nineteen
additional cases cover the following fixes. Real HTTPX transports are now denied
by the test suite; synthetic MockTransport responses remain available.

### R11 — P1: Exports could target provider coordination files

The export checks protected vault metadata and raw downloads but omitted the
provider database, its journals and the shared receipt cache. In particular,
CSV replacement could overwrite those files, losing cooldowns/jobs or corrupting
cached data. A shared destination validator now rejects these targets before
creating directories, locks or temporary files, across CSV, SQLite and HDF5.
Tests cover all three formats and both configured and default provider locations.

### R12 — P1: Relative job paths changed meaning after restart

A queued job stored `vault` as a relative path. Resuming from another working
directory selected a different archive, missed existing cached hours and could
attempt unnecessary downloads. New jobs now resolve the vault and provider state
paths before persistence. A two-minute synthetic query queued in one directory
and resumed in another completes entirely from the original cache with HTTP
disabled. Existing previously saved relative paths have no recorded original
working directory; resume those from their original directory.

### R13 — P1: Disk validation and metadata commits blocked the event loop

Cache discovery, metadata writes and payload decompression ran synchronously
inside async tasks. Slow disk or SQLite lock waits prevented cancellation and
heartbeat tasks from running, risking expired leases and replacement requests.
Discovery now scans at most eight hours per worker-thread operation; each
SQLite connection is created and closed by its owning thread. Payload validation
and metadata commits also run off the event loop. Cancellation finishes owned
disk work and acknowledges successfully committed queue entries before exiting.
Controlled thread gates verify loop responsiveness and durable cancellation,
while the existing end-to-end shutdown and concurrency tests remain passing.

The GitHub Pages project page is a dependency-free single-page export under
docs/page/. Its sections and internal links were checked offline. It contains no
external fonts, scripts, image hosts or data requests. The Pages workflow builds
the page artifact after the repository owner enables Pages for the repository.

## Provider evidence and limits

The previous single legacy request returned 429 without Retry-After; it did not
produce a verified market-data payload. The original probe was described as a
local Python command. The earlier suggestion that this machine necessarily has
a different network from the execution environment was unsupported. Record the
actual route and response origin before attributing the limit to an IP or service.

HTTP does not define one mandatory identity for rate-limit counting. The server
may count by resource, authenticated identity, server, or a wider service.
Changing IP therefore cannot guarantee acceptance.
[RFC 6585, section 4](https://www.rfc-editor.org/rfc/rfc6585#section-4).

See [the implementation plan](RELIABILITY_PLAN.md) for the prevention and recovery
design, access gates, optional permitted route fallback, and acceptance criteria.
