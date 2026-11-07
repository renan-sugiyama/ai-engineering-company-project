from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

import psycopg
from prefect.exceptions import PrefectException

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from data.pipelines.support_performance import get_reporting_periods, support_performance_flow


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use an ISO-8601 datetime with a timezone") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("Datetime bounds must include a timezone")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Nexova's weekly Customer Support performance pipeline."
    )
    parser.add_argument(
        "--period-start",
        type=_parse_datetime,
        help="Inclusive start of a complete Europe/Madrid reporting week (ISO-8601).",
    )
    parser.add_argument(
        "--period-end",
        type=_parse_datetime,
        help="Exclusive end of a complete Europe/Madrid reporting week (ISO-8601).",
    )
    parser.add_argument(
        "--trigger-source",
        choices=("manual", "schedule", "backfill"),
        default="manual",
        help="Execution source recorded in the run log.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    arguments = parser.parse_args(argv)
    if (arguments.period_start is None) != (arguments.period_end is None):
        parser.error("--period-start and --period-end must be provided together")

    if arguments.period_start is not None:
        try:
            get_reporting_periods(arguments.period_start, arguments.period_end)
        except ValueError as error:
            parser.error(str(error))

    try:
        result = support_performance_flow(
            arguments.period_start,
            arguments.period_end,
            arguments.trigger_source,
        )
    except (PrefectException, psycopg.Error, RuntimeError, ValueError):
        print(
            "Pipeline run failed; consult the structured run log for its sanitized error.",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, sort_keys=True, default=str))
    if result.get("status") == "Completed":
        return 0
    if result.get("status") == "Skipped":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())