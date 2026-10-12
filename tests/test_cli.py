import asyncio
import errno
import json
import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from tick_vault import cli


@dataclass
class Report:
    symbol: str = "EURUSD"
    downloaded: int = 1
    no_data: int = 0
    cached: int = 2
    failed: int = 0
    cancelled: bool = False
    start: datetime = datetime(2024, 1, 2, tzinfo=UTC)


def test_dates_and_base_directory_work_before_or_after_subcommand(tmp_path):
    for arguments in (
        [
            "--base-directory",
            str(tmp_path),
            "download",
            "EURUSD",
            "2024-01-02T12:00:00+02:00",
        ],
        [
            "download",
            "EURUSD",
            "2024-01-02T12:00:00+02:00",
            "--base-directory",
            str(tmp_path),
        ],
    ):
        args = cli.build_parser().parse_args(arguments)
        assert args.base_directory == tmp_path
        assert args.start == datetime(2024, 1, 2, 10, tzinfo=UTC)


@pytest.mark.parametrize(
    "arguments",
    [
        ["download", "EURUSD", "yesterday"],
        ["download", "EURUSD", "2024-01-02", "--workers", "0"],
        ["read", "EURUSD", "--pipet-scale", "nan"],
        ["read", "EURUSD", "--batch-size", "-1"],
    ],
)
def test_invalid_cli_inputs_exit_nonzero(arguments, capsys):
    with pytest.raises(SystemExit) as error:
        cli.main(arguments)
    assert error.value.code == 2
    assert capsys.readouterr().err


def test_download_json_and_cooperative_stop_exit_code(monkeypatch, capsys):
    calls = []

    async def download(*args, **kwargs):
        calls.append((args, kwargs))
        return Report(cancelled=True)

    monkeypatch.setattr(cli, "download_range", download)
    assert (
        cli.main(["download", "EURUSD", "2024-01-02", "--end", "2024-01-03", "--json"])
        == 130
    )
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["cancelled"] is True
    assert result["start"] == "2024-01-02T00:00:00+00:00"
    assert isinstance(calls[0][1]["stop_event"], asyncio.Event)
    assert calls[0][1]["show_progress"] is False


def test_read_streams_csv_to_stdout_with_one_header(monkeypatch, capsys):
    rows = pd.DataFrame(
        {
            "time": [datetime(2024, 1, 2)],
            "ask": [1.2],
            "bid": [1.1],
            "ask_volume": [2.0],
            "bid_volume": [3.0],
        }
    )
    options = {}

    def batches(symbol, **kwargs):
        options.update(kwargs)
        yield rows
        yield rows

    monkeypatch.setattr(cli.reader, "iter_tick_data", batches, raising=False)
    assert (
        cli.main(
            ["read", "EURUSD", "--backend", "thread", "--workers", "2", "--allow-gaps"]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert output.count("time,ask,bid,ask_volume,bid_volume") == 1
    assert len(output.strip().splitlines()) == 3
    assert options["backend"] == "thread"
    assert options["max_workers"] == 2
    assert options["strict"] is False


def test_failed_file_read_preserves_previous_export(monkeypatch, tmp_path, capsys):
    output = tmp_path / "ticks.csv"
    output.write_text("previous complete export", encoding="utf-8")

    def batches(*args, **kwargs):
        yield pd.DataFrame({"time": [datetime(2024, 1, 2)]})
        raise RuntimeError("corrupt chunk")

    monkeypatch.setattr(cli.reader, "iter_tick_data", batches, raising=False)
    assert cli.main(["read", "EURUSD", "--output", str(output)]) == 1
    assert output.read_text(encoding="utf-8") == "previous complete export"
    assert set(tmp_path.iterdir()) == {
        output,
        tmp_path / ".ticks.csv.tick-vault.lock",
    }
    assert "corrupt chunk" in capsys.readouterr().err


def test_reversed_range_never_downloads(monkeypatch, capsys):
    async def fail(*args, **kwargs):
        raise AssertionError("unexpected network request")

    monkeypatch.setattr(cli, "download_range", fail)
    assert cli.main(["download", "EURUSD", "2024-01-03", "--end", "2024-01-02"]) == 1
    assert "end must be after start" in capsys.readouterr().err


def test_interrupt_returns_130_and_message(monkeypatch, capsys):
    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(cli, "download_range", interrupted)
    assert cli.main(["download", "EURUSD", "2024-01-02"]) == 130
    assert "resume" in capsys.readouterr().err


def test_first_ctrl_c_requests_cooperative_stop_and_restores_signal(
    monkeypatch, capsys
):
    previous = signal.getsignal(signal.SIGINT)

    async def stopped(*args, **kwargs):
        handler = signal.getsignal(signal.SIGINT)
        handler(signal.SIGINT, None)
        assert kwargs["stop_event"].is_set()
        return Report(cancelled=True)

    monkeypatch.setattr(cli, "download_range", stopped)
    assert cli.main(["download", "EURUSD", "2024-01-02", "--json"]) == 130
    assert signal.getsignal(signal.SIGINT) is previous
    assert "saving completed hours" in capsys.readouterr().err


def test_empty_csv_still_has_schema(monkeypatch, capsys):
    monkeypatch.setattr(
        cli.reader, "iter_tick_data", lambda *args, **kwargs: iter(()), raising=False
    )
    assert cli.main(["read", "EURUSD"]) == 0
    assert capsys.readouterr().out == "time,ask,bid,ask_volume,bid_volume\n"


def test_status_is_machine_readable_and_local_only(monkeypatch, capsys):
    class Database:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def status(self, symbol, start=None, end=None):
            return {"symbol": symbol, "downloaded": 3, "no_data": 1}

    monkeypatch.setattr(cli, "MetadataDB", Database)
    assert cli.main(["status", "EURUSD"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "symbol": "EURUSD",
        "downloaded": 3,
        "no_data": 1,
    }


def test_module_cli_smoke(tmp_path):
    environment = os.environ.copy()
    directory = tmp_path / "no-import-side-effects"
    environment["TICK_VAULT_BASE_DIRECTORY"] = str(directory)
    result = subprocess.run(
        [sys.executable, "-m", "tick_vault", "--help"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "download,read,export,status" in result.stdout
    assert not directory.exists()


def test_installed_console_script_smoke(tmp_path):
    executable = Path(sys.executable).parent / (
        "tick-vault.exe" if os.name == "nt" else "tick-vault"
    )
    if not executable.exists():
        pytest.skip("Install the package to test its generated console launcher")
    result = subprocess.run(
        [str(executable), "--help"], cwd=tmp_path, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "download,read,export,status" in result.stdout


def test_csv_disk_write_error_is_not_treated_as_closed_stdout(
    monkeypatch, tmp_path, capsys
):
    output = tmp_path / "ticks.csv"
    output.write_text("previous complete export", encoding="utf-8")

    class Frame:
        def to_csv(self, *args, **kwargs):
            raise OSError(errno.EINVAL, "disk write failed")

    monkeypatch.setattr(
        cli.reader, "iter_tick_data", lambda *args, **kwargs: iter([Frame()])
    )
    assert cli.main(["read", "EURUSD", "--output", str(output)]) == 1
    assert output.read_text(encoding="utf-8") == "previous complete export"
    assert set(tmp_path.iterdir()) == {
        output,
        tmp_path / ".ticks.csv.tick-vault.lock",
    }
    assert "disk write failed" in capsys.readouterr().err


def test_csv_reader_oserror_is_not_treated_as_closed_stdout(monkeypatch, capsys):
    def batches(*args, **kwargs):
        raise OSError(errno.EINVAL, "invalid cache request")

    monkeypatch.setattr(cli.reader, "iter_tick_data", batches)
    assert cli.main(["read", "EURUSD"]) == 1
    assert "invalid cache request" in capsys.readouterr().err


def test_csv_fail_mode_preserves_external_file_created_during_export(
    monkeypatch, tmp_path, capsys
):
    output = tmp_path / "ticks.csv"

    def batches(*args, **kwargs):
        # An external writer need not participate in our cooperative lock.
        # This file appears after the CLI's initial if_exists check.
        assert not output.exists()
        output.write_text("external complete export", encoding="utf-8")
        yield pd.DataFrame({"time": [datetime(2024, 1, 2)]})

    monkeypatch.setattr(cli.reader, "iter_tick_data", batches)
    assert (
        cli.main(
            ["export", "EURUSD", str(output), "--format", "csv", "--if-exists", "fail"]
        )
        == 1
    )
    assert output.read_text(encoding="utf-8") == "external complete export"
    assert set(tmp_path.iterdir()) == {
        output,
        tmp_path / ".ticks.csv.tick-vault.lock",
    }
    assert capsys.readouterr().err
