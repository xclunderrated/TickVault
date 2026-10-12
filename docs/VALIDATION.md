# Validation record

Work performed in `D:\TickVault` on 2026-10-12, on local branch
`fix/complete-roadmap`. Based on the upstream `main` checkout obtained that day.
Upstream commit: `d12cd8223a989cfce5f72b01be0120bb77899ef2`.
This is an unreleased local version 0.3.0. No upstream issue, pull request,
repository branch, or PyPI release has been changed.

## Scope and evidence

- [Issue #3](https://github.com/keyhankamyar/TickVault/issues/3) reports a stalled
  sub-hour XAUUSD download and asserts a replacement feed host. The repaired
  pipeline is tested with the same 21:30–21:40 interaction, mocked success,
  throttling, transport deadlines and fatal worker/writer failures. The host
  migration assertion is not independently verified.
- [Issue #1](https://github.com/keyhankamyar/TickVault/issues/1) supplies a 0.001
  pipet scale for USATECHIDXUSD and USA500IDXUSD. Both registry entries and
  decoding behavior are covered by regression tests.
- The open [PR #2](https://github.com/keyhankamyar/TickVault/pull/2) concerns a
  separate daily reference-data source. It is not a fix for the hourly feed and
  was not merged into this work.
- All twelve original roadmap items have local implementations. The README
  maps each original item to its public API or implementation package.

## Service access gate

A single bounded GET of
`https://datafeed.dukascopy.com/datafeed/EURUSD/2024/00/15/12h_ticks.bi5`
returned **HTTP 429**, with no Retry-After header. Further provider requests
were stopped. This proves neither a permanent feed outage nor successful tick
retrieval from this environment. Successful live payload validation remains open.

After the 0.3.0 offline gates passed, the explicit quota-governed canary made one
GET for `EURUSD/2024/00/02/12h_ticks.bi5` and received **429**. No retry or route
change followed. The response had Content-Type `application/json` and no Date,
Retry-After, Server or Via header. Those fields do not identify the component
that produced the response. The default HTTPX route includes any configured
environment proxy; no credentials were printed or changed.

The shared state persisted a deadline of `2026-10-12T19:23:04.063328Z`, opened
the circuit, kept one request start, released all leases, and reduced the start
rate from one per two seconds to one per four seconds. A local provider-status
query confirmed the cooldown and `access_verified_at=null` without HTTP.
The sanitized canary result is retained locally in
`artifacts/live-canary-2026-10-12.json` (excluded from source distributions).
No tick-data file was downloaded. After the user specified a limited internet
allowance, remaining validation used local synthetic archives and offline package
operations only. The hourly protocol cannot transfer just two minutes of an
uncached hour; exact minute bounds apply to subsequent local reading.

The library keeps the legacy hourly endpoint as its default and permits explicit
HTTPS hourly endpoint configuration. It honors rate-limit delays across workers
and proxies. It does not automatically switch hosts when access is throttled.

[Dukascopy's current export documentation](https://www.dukascopy.com/wiki/en/development/data-export/)
describes an AWS Requester Pays daily export requiring credentials and billing.
Its format/timebase differs from the hourly archive. Paid S3 access is outside
the original roadmap, is not implemented here, and was not used for validation.

## Regression checks

The test suite uses deterministic BI5 fixtures and HTTP mock transports. No
default test contacts Dukascopy or another provider. An autouse fixture blocks
real HTTPX transports; provider responses must use offline mock transports.

| Area | Acceptance evidence |
| --- | --- |
| HTTP | Typed errors, finite retries/timeouts, numeric/date Retry-After, zero delays, excessive delay budget, URL credential redaction |
| Downloads | Same-hour coverage, live call-time cutoff, shared clients/queue, adaptive latency/error response, shared provider cooldown |
| Shutdown | Child failure propagation, stop events, external cancellation, durable receipts under queue backpressure, metadata writer failure |
| Resume | Legacy migration, separate no-data/failed states, expiry, missing/corrupt file repair, recoverable complete files |
| Concurrent access | WAL metadata permits a streaming reader to remain open while completed downloads commit |
| Shared admission | Fake-clock pacing/window ceilings; restart-safe one-hour cooldown; independent-process startup/admission; exactly one recovery probe; jitter only extends deadlines |
| Hour ownership | Concurrent calls/vaults reuse one validated receipt; independent processes publish one digest-addressed artifact; stale ownership cannot publish; heartbeat/cancel recovery |
| Source identity | Vault pins reject endpoint changes; unbound legacy bytes cannot be reassigned to custom feeds; access proofs are endpoint-specific |
| Managed jobs | Durable progress, frozen settings, lease-token guarded updates, cached work during cooldown, waiting/action/pause states, no credential persistence |
| Follow-up review | Absolute job paths survive changed working directories; every export protects provider state and raw receipts; gated disk work leaves the event loop responsive and completes safely on cancellation |
| Binary | LZMA/record integrity, hourly offsets/order, finite nonnegative volumes, int64 volume overflow, atomic write failure preservation |
| Reader | Exact UTC/microsecond bounds, empty schema, confirmed empty-hour coverage, gaps, corrupt/missing files, bounded thread/process work |
| Storage | SQLite index, duplicate timestamps, precise indexed range queries, optional HDF5, failure preservation and append semantics |
| Export concurrency | Independent-process symbol writes and appends preserve both successes; reader/writer coordination on Windows; atomic CSV fail publication resists competing creation |
| CLI/API | stdout JSON/CSV, stderr diagnostics, exit codes, Ctrl-C, convenience function and real local pipeline integration |
| Tutorial | Offline example and all notebook code cells, including installed optional HDF5; live tutorial cell disabled |

Tests use synthetic prices solely to verify software behavior. They are not
market-data samples and are never inserted into a user's real vault.

## Reproduce

```powershell
.\.venv\Scripts\python -m pytest
.\.venv\Scripts\python -m ruff check tick_vault tests examples
.\.venv\Scripts\python -m black --check tick_vault tests examples
.\.venv\Scripts\python -m mypy tick_vault
.\.venv\Scripts\python -m build
.\.venv\Scripts\tick-vault --help
```

The repository also configures CI on Windows/Linux with Python 3.11–3.14. CI
configuration does not imply those remote jobs have run. Local results and
environment versions are recorded below after the final verification pass.

## Local verification results

- Windows, Python 3.13.15: all 269 regression tests passed; 91% statement coverage.
- Windows, Python 3.11.15 with the pinned minimum dependency set: all 269 tests passed.
- Ruff, Black and mypy passed for the library; source distribution and wheel build passed.
- Both CLI entry points, the offline example, and all seven notebook code cells
  passed, including optional HDF5 and SQLite round trips.
- The 0.3.0 wheel was installed from its local file with index/dependency access
  disabled. A separate import path verified the installed package, a two-minute
  synthetic query, SQLite/HDF5 exports, a durable cached job and the packaged
  console entry point with HTTP disabled. The source archive includes the
  minimum constraints, delivery evaluation and new integration tests.
- Release files: `dist/tick_vault-0.3.0-py3-none-any.whl` and
  `dist/tick_vault-0.3.0.tar.gz`. Wheel SHA256:
  `2055b8c0bb95f797e7dc4f4bb1a2859b492a7282c18c6e87d327e22bc3f51dcd`.
- Dependencies in the primary test environment: httpx 0.28.1, NumPy 2.5.4,
  pandas 3.0.6, Pydantic 2.14.1, PyTables 3.11.1. The dependency consistency check passed.
- Tests do not hide ResourceWarnings; the final verification run reported none.
- The follow-up review added 19 offline regressions, reproduced the new failures
  before repair, and reran both complete suites. Logs are retained locally as
  `artifacts/review-tests-python313.log` and `artifacts/review-tests-python311-min.log`.
  The rebuilt wheel passed the installed-package smoke test with HTTP disabled;
  its build and smoke logs are `artifacts/review-build.log` and
  `artifacts/review-wheel-smoke.log`. Package build/install used no index or
  dependency downloads.

The exact minimum set is `.github/constraints-min.txt`. It includes NumPy 1.24.0,
pandas 2.0.0, httpx 0.27.0, Pydantic 2.0.1, pydantic-settings 2.0.2,
PyTables 3.9.0, pytest 8.2.0, pytest-asyncio 0.24.0, Black 25.9.0,
Ruff 0.3.0 and mypy 1.8.0. Minimum Ruff/Black/mypy and dependency consistency
checks also passed. pytest-asyncio's dependency requires pytest >=8.2; the
settings floor includes the upstream
[dotenv prefix bug fix](https://github.com/pydantic/pydantic-settings/releases/tag/v2.0.2).
Pandas 2.0 comparisons use a common nanosecond resolution to compare identical
UTC timestamp values across reader/storage representations without losing precision.

The process-start/WAL tests found and repaired a real concurrent first-use lock
race in shared provider state. A wall-clock proxy-balancing test was replaced
with an explicit response gate that verifies fast consumers continue draining
the shared queue while another route waits, independent of disk timing.

The suite covers the exact issue #3 XAUUSD dates and proxy configuration using
a controlled HTTP response. That proxy is never contacted by the test.
Cached-file size/mtime signatures accelerate resume after initial validation;
changed files are revalidated, and readers always validate payload contents.
These signatures are cache checks, not cryptographic integrity proofs.

Shared receipts additionally verify a SHA256 digest and BI5 contents before reuse.
An owner interrupted before publication can require a replacement attempt under
the same budget. No test establishes a perpetual zero-error promise, a provider
quota, multi-machine coordination, Linux runtime results, or successful live feed
access. Windows/Linux Python 3.11–3.14 CI is configured; those remote jobs have
not been run from this local checkout.

## GitHub Page preparation (2026-10-13)

The single-page GitHub Pages project page contains the overview, full getting-
started guide and reliability details as anchored sections. A local HTML parser
verified every section and internal link. It uses system fonts and loads no
third-party scripts, fonts or images. The GitHub Actions workflow uploads only
docs/page/ as the Pages artifact.

After adding the GitHub Page, the wheel and source archive were rebuilt locally
without package-index access. The offline installed-wheel smoke check passed
again; the source archive contains the page, stylesheet and Pages workflow.

GitHub reports the target repository has Pages disabled and the connected
account lacks push permission. No branch, commit, Pages setting or published
site was changed remotely. The repository owner must grant write access and
enable Settings → Pages → Build and deployment → GitHub Actions before this
workflow can publish. See [GitHub custom Pages workflow instructions](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages).
