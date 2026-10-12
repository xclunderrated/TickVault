"""Download one explicitly requested historical hour; this script uses the feed."""

import argparse
import asyncio
import json
from datetime import timedelta

from tick_vault import probe_feed, read_tick_data, reload_config
from tick_vault.cli import _datetime


async def run(args: argparse.Namespace) -> int:
    reload_config(base_directory=args.base_directory, worker_per_proxy=1)
    checked = await probe_feed(args.symbol, args.start)
    if not checked.complete:
        print(json.dumps(checked.to_dict(), default=str, sort_keys=True))
        return 3 if checked.state.startswith("waiting") else 1
    data = read_tick_data(
        args.symbol, args.start, args.start + timedelta(hours=1), show_progress=False
    )
    print(data.head())
    print(f"Returned {len(data)} ticks in exactly the requested hour")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="EURUSD")
    parser.add_argument(
        "--start", type=_datetime, default=_datetime("2024-01-02T12:00:00Z")
    )
    parser.add_argument("--base-directory", default="./tick_vault_data")
    raise SystemExit(asyncio.run(run(parser.parse_args())))
