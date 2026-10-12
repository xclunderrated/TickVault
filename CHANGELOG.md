# Changelog

All notable changes to TickVault will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - Unreleased

### Fixed
- Saved jobs retain absolute archive/state paths across working-directory changes.
- All export formats protect provider state, SQLite sidecars and the shared raw cache.
- Cache discovery, payload validation and metadata commits run off the event loop, preserving responsiveness for cancellation and lease renewal; owned disk writes finish before cancellation returns.
- Concurrent SQLite/HDF5 snapshots and readers retain accepted exports through process destination locks.
- CSV fail mode publishes without overwriting a file created during export.
- Valid orphan files recover from previous no-data metadata.
- Configuration/logger publication is atomic; active downloads and convenience readers keep their captured vault.
- Streaming HTTP classifies and persists throttle headers before reading error bodies; persistence failures stay visible.
- Vault source identity prevents old endpoint bytes from satisfying a different feed.
- Overlapping explicit empty-hour rechecks share one new result, including zero-TTL policies; later jobs can recheck again.
- Compatible runtime/development floors and a pinned Python 3.11 minimum dependency CI job.
- Failed or interrupted CLI JSON returns a structured incomplete state.

### Added
- A responsive, dependency-free GitHub Pages site with overview, installation and reliability guides.
- Offline tests reject real HTTP transports to prevent accidental provider traffic.
- Persistent shared provider pacing, rolling budgets, aggregate leases, cooldown jitter and one recovery probe.
- Hourly ownership with heartbeats, validated digest-addressed raw receipts and reuse across processes/vaults.
- Source-specific access proof before uncached bulk work, plus an explicit zero-retry one-GET canary.
- Durable managed download jobs with waiting/action states, status and resume APIs/CLI commands.
- Provider status, bounded sanitized response diagnostics and configurable response byte limits.

### Validation limits
- The governed live access check received HTTP 429; the library stored the restriction and stopped.
- Real payload access, vendor quota confirmation and optional paid/alternative delivery remain external gates.
- Interrupted owners may need a permitted replacement fetch before a complete receipt exists.
- This version is local and unreleased; no upstream push, issue change or PyPI publication occurred.

## [0.2.0] - Unreleased baseline

### Fixed
- HTTP failure categories survive dispatch so retries and forbidden errors work.
- Worker failure, timeout and cancellation no longer leave the orchestrator waiting indefinitely.
- Configuration reload updates all modules and logging destinations; imports create no files.
- Same-hour historical requests fetch their enclosing hour; exact UTC reader bounds exclude the end tick.
- Resume repairs missing/corrupt cache files, recovers unrecorded valid files and distinguishes confirmed no-data from failure.
- Legacy metadata migrates additively without assuming missing files prove a closed market.
- Streaming readers and metadata writers coexist through SQLite WAL mode.
- CLI flags preserve prior configuration, and early CSV pipe closure exits cleanly on Windows.
- Large-range queries avoid SQLite parameter limits and quadratic pending-list processing.
- BI5 validation rejects malformed LZMA, record lengths, timestamps and volumes before atomic saves.
- Added USATECHIDXUSD and USA500IDXUSD price scales from upstream issue #1.
- Corrected repository URLs, direct NumPy dependency and Python requirement documentation.

### Added
- Cooperative stop events, progress snapshots and download outcome reports.
- Shared queues, connection reuse, adaptive concurrency and an origin-wide rate-limit cooldown.
- Bounded ordered serial/thread/process reading and a streaming DataFrame iterator.
- Streaming SQLite and optional HDF5 exports with indexed time-range readers.
- Unified download_and_read API, command-line tools, runnable examples and an offline notebook.
- Core/download/read implementation packages with compatibility imports.
- Validated cache signatures avoid repeated decompression during warm resume.
- Offline regression suite and Windows/Linux Python 3.11–3.14 CI matrix.
- Explicit configurable HTTPS hourly feed base; no automatic endpoint switching.

### Validation limits
- The single live legacy-feed probe returned HTTP 429. Successful live payload retrieval is not verified.
- Dukascopy's documented paid daily S3 export uses a different protocol and is not implemented.
- 0.2.0 remains a local unreleased version; no upstream push or package publication occurred.

## [0.1.0] - 2025-10-13

### Added
- Initial release of TickVault
- Concurrent download functionality with multi-worker architecture
- Resume capability with SQLite metadata tracking
- Proxy support for distributed downloading
- Comprehensive retry logic with exponential backoff
- Data reading and decoding to pandas DataFrames
- Gap detection and data integrity verification
- Support for Forex majors, precious metals, and cryptocurrencies
- Configuration management via environment variables and .env files
- Structured logging with file and console handlers
- Progress bars for download and read operations

[0.1.0]: https://github.com/keyhankamyar/TickVault/releases/tag/v0.1.0
