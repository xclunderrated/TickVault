# TickVault

Download, validate and query Dukascopy historical ticks in Python. Keep original
compressed hourly files locally, resume interrupted downloads, and decode only
the hours needed for a query.

This checkout implements the original roadmap and client reliability plan for version 0.3.0. It repairs the
worker hang reported in [issue #3](https://github.com/keyhankamyar/TickVault/issues/3)
and adds the two index price scales from
[issue #1](https://github.com/keyhankamyar/TickVault/issues/1).
See [validation and remaining service limits](docs/VALIDATION.md) for the evidence.
The [reliability reviews](docs/REVIEW.md) record thirteen additional defects and their fixes;
the [prevention and recovery plan](docs/RELIABILITY_PLAN.md) tracks implementation and external access gates.
The [GitHub Pages project page](https://keyhankamyar.github.io/TickVault/) puts the
overview, getting-started guide and reliability notes on one page.

## Install

Requires Python 3.11 or newer. From this checkout:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[dev,hdf5]"
.\.venv\Scripts\tick-vault --help
```

On Linux/macOS, activate your virtual environment and use
`python -m pip install -e ".[dev,hdf5]"`. HDF5 is optional:
`python -m pip install -e .` installs the downloader, pandas reader and SQLite
storage without PyTables. Version 0.3.0 has not been published to PyPI.

## Download and read

```python
import asyncio
from datetime import datetime
from tick_vault import download_and_read, reload_config

reload_config(base_directory="./tick_vault_data", worker_per_proxy=4)

async def main():
    ticks = await download_and_read(
        "EURUSD",
        datetime(2024, 1, 15, 12, 30),
        datetime(2024, 1, 15, 12, 40),
        show_progress=False,
    )
    print(ticks.head())

if __name__ == "__main__":
    asyncio.run(main())
```

Naive datetimes mean UTC. Aware datetimes are converted to UTC before rounding.
The downloader fetches complete hourly files overlapping a historical request.
The reader returns ticks in exactly `[start, end)`; a ten-minute request inside
one historical hour downloads that hour and returns only those ten minutes.
A two-minute uncached query still transfers an entire compressed hourly file;
query duration is not a download-size limit. For a limited internet allowance,
reuse cached data and set explicit query bounds before starting a live download.
An omitted `end` is evaluated at each call. Downloads cap the end at the current
UTC hour boundary, excluding incomplete and future hours.

Existing calls to `download_range`, `read_tick_data` and `reload_config` still work.
`download_range` now returns a `DownloadReport` with separate downloaded, no-data,
cached and failed counts. The report distinguishes successful discovery from a
cooperative stop. Corrupt responses are rejected before an atomic cache write.

```python
from tick_vault import download_range, read_tick_data

# In an async function:
report = await download_range("EURUSD", start, end)
# Later, without a network request:
ticks = read_tick_data("EURUSD", start, end, show_progress=False)
```

The five columns are `time`, `ask`, `bid`, `ask_volume` and `bid_volume`.
Times retain the original pandas UTC-naive representation. Prices use the
instrument's pipet scale; volumes retain the original integer millionths
representation (the BI5 float volume multiplied by 1,000,000 and rounded).
Duplicate timestamps are retained because distinct ticks can share a millisecond.

## Shared budgets and resumable jobs

By default, all TickVault calls and CLI processes for this user share a durable
provider budget: one request at a time, starts at least two seconds apart,
30 starts per minute and 1,800 per hour. These are client ceilings, not a
published Dukascopy allowance. Cached ranges need no HTTP requests.

Bulk downloads require a successful nonempty validated payload from the selected
hourly feed. Start with one explicit canary, which makes at most one GET and
does not retry:

```sh
python -m tick_vault canary EURUSD 2024-01-02T12:00:00Z --base-directory ./ticks --json
python -m tick_vault provider-status
python -m tick_vault download EURUSD 2024-01-02 --end 2024-01-03 --base-directory ./ticks --managed --json
python -m tick_vault job-status JOB_ID
python -m tick_vault resume JOB_ID
```

A managed download returns its durable job ID, state, completed/pending counts
and next eligible time. Normal throttling or a temporary outage leaves it waiting;
run `resume` after the deadline. Jobs do not schedule themselves. Permission,
source identity and corruption problems stay visible as `needs_action`.
New jobs store absolute vault and provider-state paths, so resuming from another
working directory continues using the original archive.

```python
from tick_vault import submit_download_job, resume_download_job

job = await submit_download_job("EURUSD", start, end)
print(job.job_id, job.state, job.completed, job.pending, job.next_eligible_at)
# Later, after the stored deadline:
job = await resume_download_job(job.job_id)
```

`download_range` retains its strict exception contract and attaches partial
progress as `error.failure_report`. Use the managed API when waiting states are
more useful than exceptions. Source-specific shared receipts let overlapping
jobs and separate vaults reuse the same validated hour. An interrupted owner may
need a later permitted fetch if it never published a complete receipt.

429 and 503 with Retry-After are persisted as soon as response headers arrive.
Without a usable header, the default cooldown is fifteen minutes. Recovery adds
nonnegative jitter and admits one probe; another throttle extends the pause and
reduces the request rate. Restarts, other symbols, vaults and proxy routes use the
same stored restriction. State defaults to `%LOCALAPPDATA%/TickVault/provider`
on Windows or `~/.local/state/tick-vault/provider` elsewhere. Use the same local
state directory for all cooperating processes. Other applications and machines
sharing your network do not automatically participate.

## Large ranges and storage

`read_tick_data` materializes a DataFrame. Use `iter_tick_data` for bounded memory:

```python
from tick_vault import iter_tick_data

for batch in iter_tick_data(
    "EURUSD", start, end,
    backend="thread", max_workers=4, max_pending=8,
    batch_size=100_000, show_progress=False,
):
    # Each frame has at most batch_size rows.
    print(len(batch))
```

Decode backends are `serial` (default), `thread` and `process`. Submitted parallel
work is bounded and results stay in chronological order. Memory depends on the
size of the pending hourly files, rather than the full history. Process decoding
on Windows must run behind `if __name__ == "__main__":`; use threads in notebooks.

Streaming SQLite and optional HDF5 backends support export and time-range queries:

```python
from tick_vault import export_sqlite, iter_sqlite, export_hdf5, iter_hdf5

count = export_sqlite("eurusd.sqlite", "EURUSD", start=start, end=end)
for batch in iter_sqlite("eurusd.sqlite", "EURUSD", start=start, end=end):
    print(batch.head())

# Requires the hdf5 extra:
count = export_hdf5("eurusd.h5", "EURUSD", start=start, end=end)
for batch in iter_hdf5("eurusd.h5", "EURUSD", start=start, end=end):
    print(batch.head())
```

Exports preserve repeated ticks and exact timestamp bounds. SQLite range queries
use a symbol/time index. Both exports replace their destination atomically only
after successful completion; a failed export leaves the previous file intact.
CSV, SQLite and HDF5 exports reject destinations belonging to vault metadata,
logs, provider state (including SQLite sidecars), or either raw cache.
HDF5 append requires chronological ranges; SQLite accepts earlier appended ranges
and orders query results by time. Append preserves duplicates and does not deduplicate.

## CLI

```powershell
tick-vault download EURUSD 2024-01-15T12:30 --end 2024-01-15T12:40 --json
tick-vault read EURUSD --start 2024-01-15T12:30 --end 2024-01-15T12:40 --output ticks.csv
tick-vault export EURUSD eurusd.sqlite --format sqlite
tick-vault export EURUSD eurusd.h5 --format hdf5
tick-vault status EURUSD
```

Use `--base-directory PATH` to select a vault. JSON and CSV go to standard output;
logs and progress go to standard error. Ctrl-C stops downloads cooperatively,
preserves completed results, and exits with code 130. See [CLI reference](docs/CLI.md).

## Reliability and resume

- Bounded queues and shared HTTP connections keep large ranges manageable.
- Workers pull from one shared queue, so faster workers take more available work.
- Adaptive concurrency observes request latency and throughput, grows cautiously,
  and reduces concurrency after transient errors or throttling.
- A persistent provider cooldown applies to all cooperating calls, processes,
  vaults, workers and configured proxies after a 429.
  `Retry-After` accepts seconds or HTTP dates, including zero. Delays exceeding the
  configured retry budget produce an explicit error.
- Typed HTTP/network failures propagate promptly, with a bounded retry count and
  timeout. No response-wait loop can hide a terminated worker.
- SQLite metadata records downloaded, confirmed no-data and failed outcomes
  separately. Failed and interrupted hours remain eligible for retry.
- Resume reuses the size and modification time of previously validated files.
  Changed files are revalidated; missing or corrupt files are scheduled again.
  A complete validated file left by an interrupted metadata write is
  recovered without downloading it again.
- Legacy metadata migrates additively. Old rows without a file are treated as
  unresolved, because the old schema cannot prove they were legitimate no-data.
- Confirmed no-data expires after `no_data_retry_after` seconds (default one day),
  or can be explicitly rechecked with `retry_no_data=True`.
- Strict reading verifies each requested hour has a successful or confirmed
  no-data outcome. Both reading modes report missing/corrupt files claimed as
  downloaded. `strict=False` clips to coverage and skips unresolved hours.

Graceful stop and progress callbacks:

```python
import asyncio

stop = asyncio.Event()

def progress(snapshot):
    print(snapshot.completed, snapshot.total, snapshot.active_workers)

# In an async function; another task may call stop.set():
report = await download_range(
    "EURUSD", start, end, stop_event=stop, progress_callback=progress
)
```

## Configuration

`reload_config` reloads defaults, `.env` and environment settings with explicit
keywords taking precedence. The shared configuration keeps its identity, so
existing imports immediately see new values. Unknown explicit settings fail.
Downloads, managed jobs and `download_and_read` capture their configuration.
Reloads affect new work without redirecting an active download or its reader.

| Setting | Default | Meaning |
| --- | --- | --- |
| `base_directory` | `./tick_vault_data` | Cache, metadata and logs |
| `datafeed_base_url` | `https://datafeed.dukascopy.com/datafeed/` | Explicit HTTPS hourly feed base |
| `worker_per_proxy` | `4` | Maximum workers per configured connection route |
| `adaptive_workers` | `True` | Adapt active concurrency |
| `adaptive_min_workers` | `1` | Minimum active worker count |
| `adaptive_adjustment_interval` | `8` | Observations per adjustment |
| `queue_size` | `128` | Pipeline queue capacity |
| `fetch_timeout` | `30.0` | HTTP request timeout, seconds |
| `fetch_max_response_bytes` | `67108864` | Maximum compressed success body, bytes |
| `fetch_max_retry_attempts` | `3` | Retries beyond the initial request |
| `fetch_base_retry_delay` | `1.0` | Exponential delay base, seconds |
| `fetch_max_retry_delay` | `60.0` | Maximum retry wait budget, seconds |
| `no_data_retry_after` | `86400.0` | Confirmed no-data recheck age, seconds |
| `provider_state_directory` | Per-user local state | Shared budget, leases, raw receipts and managed jobs |
| `provider_require_verified_access` | `True` | Require source access proof before uncached multi-hour work |
| `provider_key` | `dukascopy` | Stable provider/account group shared by every route |
| `provider_request_interval` | `2.0` | Minimum start spacing, seconds |
| `provider_max_concurrency` | `1` | Aggregate HTTP attempts in flight |
| `provider_minute_budget` / `provider_hour_budget` | `30` / `1800` | Rolling request-start ceilings |
| `provider_no_header_cooldown` | `900.0` | Pause after throttling with no usable Retry-After |
| `provider_cooldown_jitter` | `1.0` | Maximum extra nonnegative cooldown, seconds |
| `provider_admission_wait_budget` | `60.0` | Maximum in-call wait for admission, seconds |
| `provider_request_lease_seconds` / `provider_hour_lease_seconds` | `90.0` / `180.0` | Heartbeat-protected ownership lifetimes |
| `metadata_update_batch_size` | `100` | Metadata records per transaction |
| `metadata_update_batch_timeout` | `1.0` | Flush interval, seconds |
| `worker_queue_timeout` | `60.0` | Compatibility worker queue setting |
| `base_log_level` / `console_log_level` | `DEBUG` / `INFO` | File/console log levels |

All fields accept `TICK_VAULT_` environment names, such as
`TICK_VAULT_BASE_DIRECTORY=D:\\MarketData`.
`reload_config(env_file="custom.env")` selects a dotenv file; `env_file=None`
disables it. Library import creates no cache or log directories.

Configured instruments include forex majors, XAUUSD, XAGUSD, BTCUSD, ETHUSD,
USATECHIDXUSD and USA500IDXUSD. Other Dukascopy instruments can be downloaded;
pass a verified `pipet_scale` to read them. Symbols are normalized to uppercase
and validated before use in paths and SQL identifiers.

## Provider access

The legacy hourly feed remains the default. An endpoint migration asserted by
an issue reporter is not a verified replacement protocol. The library supports
an explicitly configured HTTPS hourly feed base, but does not switch hosts on
throttling or fabricate data.

A vault is pinned to its hourly feed identity. Changing `datafeed_base_url`
requires a separate `base_directory`; old bytes cannot silently satisfy a new
source. Unattributed legacy archives can inherit the standard legacy feed only.
Persisted provider ceilings can tighten; another caller cannot relax them by
raising its worker count or setting a faster policy.

[Dukascopy's current export documentation](https://www.dukascopy.com/wiki/en/development/data-export/)
also describes an AWS Requester Pays daily export. That requires AWS credentials,
billing authorization and a different timebase. It is not an interchangeable
hourly URL and this version does not implement or activate paid S3 downloads.

A bounded live probe with the new controls returned HTTP 429 and stored a shared
cooldown. No successful real payload was received. The deterministic
regression tests verify library behavior; a successful live payload download
remains a separate service-access validation. See [validation](docs/VALIDATION.md).

## Roadmap delivered

| Original item | Implementation |
| --- | --- |
| Async stop events | `stop_event`, cancellation cleanup and metadata flush |
| Dynamic worker balancing | Shared bounded queue and throughput snapshots |
| Adaptive worker scaling | Latency/error-aware concurrency controller |
| Multi-thread/process decoding | Ordered bounded `thread` and `process` backends |
| Streaming pipeline to SQLite | `export_sqlite`, indexed `iter_sqlite` |
| HDF5 backend | Optional `export_hdf5`, `iter_hdf5` |
| Unified download/read | `download_and_read` |
| Core/download/read packages | Implementations with compatibility import shims |
| Comprehensive pytest suite | Network, shutdown, migration, decode, storage and CLI regressions |
| CLI | `tick-vault` and `python -m tick_vault` |
| Enhanced documentation | This guide, CLI reference, validation record and runnable examples |
| Notebook tutorials | [Offline tutorial](notebooks/tickvault_tutorial.ipynb) |

## Development

```powershell
.\.venv\Scripts\python -m pytest
.\.venv\Scripts\python -m ruff check tick_vault tests examples
.\.venv\Scripts\python -m black --check tick_vault tests examples
.\.venv\Scripts\python -m build
```

The default tests make no provider requests. CI covers Windows and Linux on
Python 3.11–3.14, including optional HDF5. Run the
[offline example](examples/offline_analysis.py) or the notebook without credentials.
[download_one_hour.py](examples/download_one_hour.py) performs a bounded live
request only when explicitly invoked.

Local files retain Dukascopy's zero-based month layout:
`downloads/SYMBOL/YYYY/MM/DD/HHh_ticks.bi5`, where January is `00`.

MIT licensed. Original author: Keyhan Kamyar.
