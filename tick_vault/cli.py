"""Command line operations for downloading, reading, and exporting tick data."""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import errno
import json
import logging
import os
import signal
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any, TextIO

from . import config, reader
from .core.filelock import destination_lock, publish_no_clobber
from .core.paths import validate_export_destination
from .downloader import download_range
from .metadata import MetadataDB, SourceIdentityError

_COLUMNS = ["time", "ask", "bid", "ask_volume", "bid_volume"]


def _datetime(value: str) -> datetime:
    """Parse an ISO date/time, interpreting a missing UTC offset as UTC."""
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"Invalid ISO date/time {value!r}; use 2024-01-02T12:00:00Z"
        ) from error
    return (
        result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)
    )


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if result < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def _positive_float(value: str) -> float:
    import math

    try:
        result = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive finite number") from error
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return result


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    # SUPPRESS lets options work on either side of the subcommand without the
    # subparser's defaults overwriting options already supplied to the root.
    common.add_argument(
        "--base-directory",
        type=Path,
        default=argparse.SUPPRESS,
        help="data directory (otherwise TICK_VAULT_BASE_DIRECTORY or ./tick_vault_data)",
    )
    common.add_argument("--quiet", action="store_true", default=argparse.SUPPRESS)
    parser = argparse.ArgumentParser(
        prog="tick-vault",
        description="Download and process Dukascopy historical ticks.",
        parents=[common],
    )
    commands = parser.add_subparsers(dest="command", required=True)
    download = commands.add_parser(
        "download", parents=[common], help="download missing hours"
    )
    download.add_argument("symbol")
    download.add_argument("start", type=_datetime)
    download.add_argument(
        "--end", type=_datetime, help="exclusive end; default: current complete hour"
    )
    download.add_argument(
        "--workers", type=_positive_int, help="maximum workers per proxy (1–100)"
    )
    download.add_argument(
        "--proxy", action="append", dest="proxies", help="HTTP proxy URL; repeatable"
    )
    download.add_argument(
        "--retry-no-data",
        action="store_true",
        help="recheck previously unavailable hours",
    )
    download.add_argument(
        "--json", action="store_true", help="print the final download report as JSON"
    )
    download.add_argument(
        "--managed",
        action="store_true",
        help="persist a resumable job and report waiting states",
    )
    download.add_argument(
        "--state-directory", type=Path, help="shared local provider/job state directory"
    )

    def read_options(command: argparse.ArgumentParser) -> None:
        command.add_argument("symbol")
        command.add_argument("--start", type=_datetime)
        command.add_argument("--end", type=_datetime)
        command.add_argument(
            "--pipet-scale",
            type=_positive_float,
            help="integer price multiplier for this symbol",
        )
        command.add_argument(
            "--backend", choices=["serial", "thread", "process"], default="serial"
        )
        command.add_argument(
            "--workers", type=_positive_int, help="number of decoding workers"
        )
        command.add_argument(
            "--batch-size",
            type=_positive_int,
            default=100_000,
            help="maximum ticks per output batch",
        )
        command.add_argument(
            "--allow-gaps",
            action="store_true",
            help="read available hours even if some were not downloaded",
        )

    read = commands.add_parser(
        "read", parents=[common], help="stream downloaded ticks as CSV"
    )
    read_options(read)
    read.add_argument(
        "--output", default="-", help="CSV path, or - for stdout (default)"
    )
    export = commands.add_parser(
        "export", parents=[common], help="stream to CSV, SQLite, or HDF5"
    )
    read_options(export)
    export.add_argument("output", help="output path, or - for CSV on stdout")
    export.add_argument(
        "--format",
        choices=["csv", "sqlite", "hdf5"],
        help="otherwise inferred from file extension",
    )
    export.add_argument(
        "--if-exists",
        choices=["replace", "append", "fail"],
        default="replace",
        help="SQLite/HDF5 table handling; CSV uses replace or fail",
    )
    status = commands.add_parser(
        "status", parents=[common], help="print local metadata status as JSON"
    )
    status.add_argument("symbol")
    status.add_argument("--start", type=_datetime)
    status.add_argument("--end", type=_datetime)
    provider = commands.add_parser(
        "provider-status",
        parents=[common],
        help="inspect the shared provider budget without HTTP",
    )
    provider.add_argument("--state-directory", type=Path)
    canary = commands.add_parser(
        "canary",
        parents=[common],
        help="at most one GET to validate one complete historical hour",
    )
    canary.add_argument("symbol")
    canary.add_argument("hour", type=_datetime)
    canary.add_argument("--proxy")
    canary.add_argument("--state-directory", type=Path)
    canary.add_argument("--json", action="store_true")
    for name in ("job-status", "resume"):
        job = commands.add_parser(
            name, parents=[common], help="inspect or resume a durable download job"
        )
        job.add_argument("job_id")
        job.add_argument("--state-directory", type=Path)
        job.add_argument("--proxy", action="append", dest="proxies")
    return parser


def _configure(args: argparse.Namespace) -> None:
    overrides: dict[str, Any] = {}
    if hasattr(args, "base_directory"):
        overrides["base_directory"] = args.base_directory
    if getattr(args, "state_directory", None) is not None:
        overrides["provider_state_directory"] = args.state_directory
    if args.command == "download" and args.workers is not None:
        overrides["worker_per_proxy"] = args.workers
    if getattr(args, "quiet", False):
        overrides["console_log_level"] = "ERROR"
    if overrides:
        config.reload_config(**(config.CONFIG.model_dump() | overrides))
    for handler in logging.getLogger("tick_vault").handlers:
        if (
            getattr(handler, "_tick_vault", False)
            and isinstance(handler, logging.StreamHandler)
            and not isinstance(handler, logging.FileHandler)
        ):
            # Refresh streams for embedded calls to main() as well as shell use.
            handler.stream = sys.stderr


def _reader_options(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "start": args.start,
        "end": args.end,
        "pipet_scale": args.pipet_scale,
        "strict": not args.allow_gaps,
        "show_progress": False,
        "backend": args.backend,
        "max_workers": args.workers,
        "batch_size": args.batch_size,
    }


def _write_csv(args: argparse.Namespace, output: str) -> int:
    if output == "-":
        return _write_csv_locked(args, output)
    _validate_csv_destination(args, Path(output))
    with destination_lock(output):
        return _write_csv_locked(args, output)


def _validate_csv_destination(args: argparse.Namespace, destination: Path) -> None:
    validate_export_destination(destination)
    if getattr(args, "if_exists", "replace") == "append":
        raise ValueError("CSV append is unsupported; use SQLite/HDF5 or replace")
    if getattr(args, "if_exists", "replace") == "fail" and destination.exists():
        raise FileExistsError(f"Output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)


def _write_csv_locked(args: argparse.Namespace, output: str) -> int:
    """Write bounded batches, replacing a destination only after full success."""
    destination = None if output == "-" else Path(output)
    if destination is not None:
        _validate_csv_destination(args, destination)
    temporary: Path | None = None
    stream: TextIO | tempfile._TemporaryFileWrapper[str] = sys.stdout
    count = 0

    def write(operation: Callable[[], Any]) -> None:
        try:
            operation()
        except OSError as error:
            # Windows reports a closed pipe as EINVAL rather than EPIPE. Restrict
            # this conversion to stdout writes/flushes, never reader/disk errors.
            if destination is None and error.errno in (errno.EPIPE, errno.EINVAL):
                raise BrokenPipeError("CSV consumer closed stdout") from error
            raise

    try:
        if destination is not None:
            stream = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                delete=False,
                prefix=f".{destination.name}.",
                suffix=".tmp",
                dir=destination.parent,
            )
            temporary = Path(stream.name)
        wrote_header = False
        for frame in reader.iter_tick_data(args.symbol, **_reader_options(args)):
            write(partial(frame.to_csv, stream, index=False, header=not wrote_header))
            count += len(frame)
            wrote_header = True
        if not wrote_header:
            write(lambda: stream.write(",".join(_COLUMNS) + "\n"))
        write(stream.flush)
        if destination is not None:
            stream.close()
            assert temporary is not None
            if getattr(args, "if_exists", "replace") == "fail":
                publish_no_clobber(temporary, destination)
            else:
                os.replace(temporary, destination)
            temporary = None
        return count
    finally:
        if destination is not None and not stream.closed:
            stream.close()
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _silence_broken_stdout() -> None:
    """Keep interpreter shutdown from retrying a failed buffered pipe flush."""
    try:
        descriptor = sys.stdout.fileno()
        if descriptor < 0:
            return
        null_descriptor = os.open(os.devnull, os.O_WRONLY)
        try:
            os.dup2(null_descriptor, descriptor)
        finally:
            if null_descriptor != descriptor:
                os.close(null_descriptor)
    except (AttributeError, OSError, ValueError):
        pass  # Embedded/captured streams may not expose an operating-system fd.


async def _download(args: argparse.Namespace) -> Any:
    stop_event = asyncio.Event()
    previous = signal.getsignal(signal.SIGINT)

    def stop(signum: int, frame: Any) -> None:
        if stop_event.is_set():
            raise KeyboardInterrupt
        print(
            "Stopping download; saving completed hours. Press Ctrl-C again to abort.",
            file=sys.stderr,
        )
        stop_event.set()

    # signal.signal also works on Windows, where loop.add_signal_handler does not.
    installed = False
    try:
        try:
            signal.signal(signal.SIGINT, stop)
            installed = True
        except ValueError:
            pass  # main() may be embedded in a non-main thread.
        operation: Callable[..., Any] = download_range
        if getattr(args, "managed", False):
            from .download.jobs import submit_download_job

            operation = submit_download_job
        return await operation(
            args.symbol,
            args.start,
            args.end,
            args.proxies,
            stop_event=stop_event,
            show_progress=not (args.json or getattr(args, "quiet", False)),
            retry_no_data=args.retry_no_data,
        )
    finally:
        if installed:
            signal.signal(signal.SIGINT, previous)


def main(argv: list[str] | None = None) -> int:
    """Return 0 for success/status, 1 for failures, 3 for waiting, 130 for stop."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        _configure(args)
        if args.command == "provider-status":
            from .download.admission import provider_coordinator

            async def provider_snapshot() -> dict[str, Any]:
                coordinator = provider_coordinator(
                    config.snapshot_config(), args.state_directory
                )
                try:
                    return (await coordinator.status()).to_dict()
                finally:
                    await coordinator.close()

            print(
                json.dumps(
                    asyncio.run(provider_snapshot()),
                    default=_json_default,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "canary":
            from .download.probe import probe_feed

            probe = asyncio.run(probe_feed(args.symbol, args.hour, proxy=args.proxy))
            print(json.dumps(probe.to_dict(), default=_json_default, sort_keys=True))
            return (
                0 if probe.complete else (3 if probe.state.startswith("waiting") else 1)
            )
        if args.command in ("job-status", "resume"):
            from .download.jobs import get_download_job, resume_download_job

            if args.command == "resume":
                job = asyncio.run(
                    resume_download_job(
                        args.job_id,
                        state_path=args.state_directory,
                        proxies=args.proxies,
                    )
                )
            else:
                found_job = asyncio.run(
                    get_download_job(args.job_id, state_path=args.state_directory)
                )
                if found_job is None:
                    raise ValueError(
                        "Download job does not exist in this state directory"
                    )
                job = found_job
            print(json.dumps(job.to_dict(), default=_json_default, sort_keys=True))
            return (
                0
                if args.command == "job-status" or job.complete
                else (
                    130
                    if job.state == "paused"
                    else (1 if job.state == "needs_action" else 3)
                )
            )
        if (
            getattr(args, "end", None) is not None
            and getattr(args, "start", None) is not None
            and args.end <= args.start
        ):
            raise ValueError("end must be after start")
        if args.command == "download":
            report = asyncio.run(_download(args))
            if getattr(args, "managed", False):
                print(
                    json.dumps(report.to_dict(), default=_json_default, sort_keys=True)
                )
                return (
                    0
                    if report.complete
                    else (
                        130
                        if report.state == "paused"
                        else (1 if report.state == "needs_action" else 3)
                    )
                )
            if args.json:
                print(
                    json.dumps(
                        dataclasses.asdict(report),
                        default=_json_default,
                        sort_keys=True,
                    )
                )
            else:
                print(
                    f"{report.symbol}: {report.downloaded} downloaded, {report.no_data} unavailable, {report.cached} cached"
                )
            return 130 if report.cancelled else (1 if report.failed else 0)
        if args.command == "status":
            with MetadataDB() as db:
                result = db.status(args.symbol, start=args.start, end=args.end)
            print(json.dumps(result, default=_json_default, sort_keys=True))
            return 0
        if args.command == "read":
            count = _write_csv(args, args.output)
            if args.output != "-" and not getattr(args, "quiet", False):
                print(f"Exported {count} ticks to {args.output}", file=sys.stderr)
            return 0

        output_format = args.format
        if output_format is None:
            suffix = Path(args.output).suffix.lower()
            output_format = {
                ".db": "sqlite",
                ".sqlite": "sqlite",
                ".sqlite3": "sqlite",
                ".h5": "hdf5",
                ".hdf5": "hdf5",
                ".hdf": "hdf5",
            }.get(suffix, "csv")
        if output_format == "csv":
            count = _write_csv(args, args.output)
        else:
            if args.output == "-":
                raise ValueError("SQLite/HDF5 exports require a file path")
            from .read.storage import export_hdf5, export_sqlite

            operation = export_sqlite if output_format == "sqlite" else export_hdf5
            count = operation(
                args.output,
                args.symbol,
                if_exists=args.if_exists,
                **_reader_options(args),
            )
        if args.output != "-" and not getattr(args, "quiet", False):
            print(f"Exported {count} ticks to {args.output}", file=sys.stderr)
        return 0
    except (KeyboardInterrupt, asyncio.CancelledError) as interruption:
        if (
            getattr(args, "json", False)
            or getattr(args, "managed", False)
            or args.command in ("resume", "canary")
        ):
            interrupted_report = getattr(interruption, "failure_report", None)
            interrupted_json = (
                dataclasses.asdict(interrupted_report)
                if dataclasses.is_dataclass(interrupted_report)
                and not isinstance(interrupted_report, type)
                else {
                    "total": None,
                    "completed": None,
                    "pending": None,
                    "failed": None,
                    "job_id": getattr(args, "job_id", None),
                }
            )
            interrupted_json.update(
                state="paused",
                complete=False,
                next_eligible_at=None,
                retry_reason="interrupted",
                error_type="Interrupted",
                error="Completed hours are saved; resume the job or range",
            )
            print(
                json.dumps(
                    interrupted_json,
                    default=_json_default,
                    sort_keys=True,
                )
            )
        print(
            "Interrupted; rerun the same download to resume completed hours.",
            file=sys.stderr,
        )
        return 130
    except BrokenPipeError:
        _silence_broken_stdout()
        return 0  # A CSV consumer such as `head` may close stdout early.
    except Exception as error:
        if getattr(args, "json", False) or args.command in (
            "provider-status",
            "job-status",
            "resume",
            "canary",
        ):
            failure_report = getattr(error, "failure_report", None)
            failure_json = (
                dataclasses.asdict(failure_report)
                if dataclasses.is_dataclass(failure_report)
                and not isinstance(failure_report, type)
                else {
                    "state": "needs_action",
                    "complete": False,
                    "error_type": type(error).__name__,
                    "error": type(error).__name__,
                    "total": None,
                    "completed": None,
                    "pending": None,
                    "failed": None,
                    "retry_reason": None,
                    "next_eligible_at": None,
                    "job_id": getattr(args, "job_id", None),
                }
            )
            print(json.dumps(failure_json, default=_json_default, sort_keys=True))
        # Arbitrary HTTP/proxy exceptions may contain credentials. Local read
        # and export diagnostics stay descriptive; network errors use safe types.
        message = (
            type(error).__name__
            if args.command
            in ("download", "resume", "canary", "provider-status", "job-status")
            else str(error)
        )
        if isinstance(error, ValueError) and "end must be after start" in str(error):
            message = "end must be after start"
        if isinstance(error, SourceIdentityError):
            message = (
                "This vault belongs to another feed; use a separate base directory"
            )
        print(f"tick-vault: {message}", file=sys.stderr)
        return 1
