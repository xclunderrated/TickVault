# TickVault command line guide

Install the project with `pip install -e .` or `pip install tick-vault` for a published release. Both `tick-vault` and `python -m tick_vault` run the same interface. Use `python -m tick_vault --help` or append `--help` to any command to see its options.

## Dates and local configuration

Dates accept ISO 8601: `2024-01-02`, `2024-01-02T12:30:00Z`, and `2024-01-02T14:30:00+02:00`. A date without a UTC offset is interpreted as UTC. Offsets are converted before selecting hours. Reads use exact inclusive start and exclusive end bounds, even within a single hour.

Use `--base-directory PATH` before or after the command. Otherwise TickVault reads `TICK_VAULT_BASE_DIRECTORY`, `.env`, or the default `./tick_vault_data`. The directory stores original compressed downloads, metadata, and logs. `--quiet` suppresses progress and routine messages. Logs and errors go to stderr; CSV and JSON go to stdout.

## Download and resume

```sh
python -m tick_vault download EURUSD 2024-01-02T12:00:00Z --end 2024-01-02T13:00:00Z --base-directory ./ticks --workers 2 --json
```

Explicit historical sub-hour ranges download their enclosing hourly files. The incomplete current UTC hour is excluded. Without `--end`, the cutoff is computed when the command starts. Rerun the same command to resume; completed files are reused. Missing files previously recorded as downloaded are eligible for repair. Unavailable hours are rechecked according to the configured retry age; `--retry-no-data` requests a recheck immediately.

An uncached two-minute request still downloads one compressed hour. `--end`
limits the time range, not an exact number of transferred bytes. Cached reads
and exports use no provider requests. This repair's validation stopped all live
data requests after its one unsuccessful canary to respect the user's quota.

`--workers` sets the maximum number of download workers per configured proxy. Adaptive scaling stays below this limit. `--proxy URL` can be repeated for an existing proxy configuration. Keep concurrency modest if the feed returns rate limits; retries honor server delays.

The shared provider budget additionally limits aggregate attempts, request-start
spacing and rolling windows. Worker counts and proxy counts cannot raise it.
All cooperating processes must use the same local provider state directory;
`--state-directory PATH` is supported by download, canary and job/provider
status commands, or set `TICK_VAULT_PROVIDER_STATE_DIRECTORY` once.

`--json` prints one final report, including requested symbol/range, total, attempted, downloaded, no-data, cached, failed, cancelled, and elapsed time. A known unavailable hour means the feed returned no data; it does not mean ticks were downloaded for that hour.

The first Ctrl-C requests a cooperative stop and saves completed hours. A second Ctrl-C interrupts immediately. A stopped download exits with status 130; rerun it to resume. Other errors exit with status 1, and invalid command syntax exits with status 2.

## Access check and managed jobs

```sh
python -m tick_vault canary EURUSD 2024-01-02T12:00:00Z --base-directory ./ticks --json
python -m tick_vault provider-status
python -m tick_vault download EURUSD 2024-01-02 --end 2024-01-03 --base-directory ./ticks --managed --json
python -m tick_vault job-status JOB_ID
python -m tick_vault resume JOB_ID
```

`canary` requires an aligned complete historical UTC hour and makes at most one
GET with zero automatic retries. A stored restriction can prevent the request.
JSON includes the attempt count, response status and sanitized headers, validated
tick count, state and any next eligible time. A 404, empty or corrupt body cannot
establish access. Uncached multi-hour work requires a nonempty validated payload
from that specific feed. Fully cached work still completes during a cooldown.

`provider-status` uses local state only and reports the shared budget, circuit,
in-flight count, recent starts and cooldown. It does not call the provider.

`download --managed` stores immutable range/settings and a job ID, then runs the
job. A normal throttle returns `waiting_for_provider`; transient transport/server
failures return `waiting_for_network`. `job-status` reads the stored job locally.
`resume` retains cached hours and observes the stored deadline before starting
work. It does not automatically schedule a later run. Source/access, permission,
corruption and permanent storage failures return `needs_action`.
New jobs save absolute archive and state paths. Resuming from a different working
directory retains the original archive; use `--state-directory` to locate the
same job store when you configured a custom store.

Managed/status/canary commands print JSON. Job results include `complete`,
`completed`, `pending`, `failed`, `retry_reason` and `next_eligible_at`. JSON for
strict failed downloads includes its partial failure report. An error before a
report is available uses null counts, indicating unknown progress, and never
claims completion. Interrupted output uses `paused`; durable job status has the
authoritative saved counts. Proxy credentials are not persisted in jobs; supply
authenticated `--proxy` options again when resuming.

Exit 0 means a completed operation or successful local status query. Managed
waiting work and deferred canaries exit 3, action-required failures exit 1,
invalid syntax exits 2, and interrupted work exits 130. Strict download failures
retain exit 1 even when their JSON explains a provider wait.

## Read compressed data as CSV

```sh
python -m tick_vault read EURUSD --start 2024-01-02T12:15:00Z --end 2024-01-02T12:45:00Z --base-directory ./ticks --output ./eurusd.csv
python -m tick_vault read EURUSD --base-directory ./ticks --backend thread --workers 2 --batch-size 50000 > eurusd.csv
```

The CSV columns are `time,ask,bid,ask_volume,bid_volume`. Prices use the symbol's registered pipet multiplier, or an explicit positive `--pipet-scale`. Volume values follow TickVault's existing convention: the feed's floating-point volumes multiplied by 1,000,000 and rounded to integers. Timestamps represent UTC and retain the historical timezone-naive DataFrame dtype. One schema header is emitted, including for an empty result.

Output defaults to stdout. File output is written to a temporary sibling and replaces the destination after success. A failed file export preserves the previous destination. A stdout consumer may already have received rows when a later chunk fails; check the exit code before using a redirected file.

The serial, thread, and process decoding backends preserve chronological output. `--batch-size` limits emitted DataFrame batches; complete compressed hours are still decoded individually. Reading verifies that all requested hours were attempted by default. `--allow-gaps` reads available hours when some were never downloaded. Claimed files that are missing or corrupt still raise errors; unavailable hours contribute no rows.

## Streaming exports

```sh
python -m tick_vault export EURUSD ./ticks.sqlite --base-directory ./ticks --format sqlite
python -m tick_vault export EURUSD ./ticks.h5 --base-directory ./ticks --format hdf5
python -m tick_vault export EURUSD ./ticks.csv --base-directory ./ticks --format csv
```

Without `--format`, `.db`, `.sqlite`, and `.sqlite3` select SQLite; `.h5`, `.hdf`, and `.hdf5` select HDF5; other extensions select CSV. Use `--start`, `--end`, `--backend`, `--workers`, `--pipet-scale`, and `--batch-size` as for `read`.

SQLite and HDF5 exports support `--if-exists replace|append|fail`; the default is replace. CSV supports replace and fail. Append does not deduplicate overlapping ranges. HDF5 appends require new ticks to start at or after the last stored tick to preserve streamed ordering. SQLite can append earlier ranges and uses its timestamp index when querying. Use non-overlapping ranges when appending.

Destination locks cover the entire export and coordinate streamed readers.
Their small `.tick-vault.lock` sidecar files persist so waiting processes keep
the same lock identity. `fail` mode publishes atomically without overwriting a
file created during export. Python storage APIs accept `lock_timeout=30.0`.
Every export format rejects the configured vault's metadata, active log and raw
cache, plus the default/configured provider database, its SQLite sidecars and its
shared raw cache. Keep exports in their own output location.
Non-cooperating external writers are outside that lock protocol.

HDF5 requires the optional dependency: `pip install "tick-vault[hdf5]"`. SQLite uses Python's standard library. Only CSV supports the output path `-` for stdout.

## Inspect local status

```sh
python -m tick_vault status EURUSD --base-directory ./ticks
python -m tick_vault status EURUSD --start 2024-01-02 --end 2024-01-03 --base-directory ./ticks
```

Status prints JSON from the local metadata database and does not contact Dukascopy. It counts recorded downloaded, unavailable, and failed hours in the optional range. Metadata describes hourly attempts; inspect or repair the underlying files before treating it as proof of tick integrity.

## Python and notebook examples

- [`examples/offline_analysis.py`](../examples/offline_analysis.py) creates a small synthetic archive, streams ticks, and exports/queries SQLite without internet access.
- [`examples/download_one_hour.py`](../examples/download_one_hour.py) performs one explicit historical-hour download and read.
- [`notebooks/tickvault_tutorial.ipynb`](../notebooks/tickvault_tutorial.ipynb) is an executable offline tutorial with an optional, disabled-by-default live download cell.

In notebooks, use `await download_and_read(...)` directly. In ordinary scripts, use `asyncio.run(download_and_read(...))` under `if __name__ == "__main__":`. Use that guard with the process decoding backend on Windows.
