#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from pathlib import Path


DEFAULT_PATTERNS = (
    "benchmark_*.csv",
    "sink_*.csv",
    "time_*.csv",
    "events_*.csv",
    "latency_test_*.csv",
)

DEFAULT_EXCLUDES = {
    "plot_data",
    "plot_data_paper",
    "plot_code",
    "plot_code_paper",
    "figs",
}


def _iter_csv_files(root: Path, patterns: tuple[str, ...], excludes: set[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in patterns:
        for path in root.rglob(pattern):
            if any(part in excludes for part in path.parts):
                continue
            files.append(path)
    return sorted(set(files))


def _convert_with_polars(csv_path: Path, parquet_path: Path) -> bool:
    try:
        import polars as pl  # type: ignore
    except Exception:
        return False

    lf = pl.scan_csv(csv_path)
    if hasattr(lf, "sink_parquet"):
        lf.sink_parquet(parquet_path, compression="zstd")
    else:
        lf.collect(streaming=True).write_parquet(parquet_path, compression="zstd")
    return True


def _convert_with_pandas(csv_path: Path, parquet_path: Path) -> None:
    import pandas as pd

    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path, index=False, compression="zstd")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert experiment CSV inputs to Parquet for faster reads."
    )
    parser.add_argument("root", type=Path, help="Root path to scan (e.g. data)")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Convert all CSV files under root (not just known experiment inputs).",
    )
    parser.add_argument(
        "--pattern",
        action="append",
        default=None,
        help="Optional glob pattern to convert. Repeatable.",
    )
    parser.add_argument(
        "--remove-csv",
        action="store_true",
        help="Remove CSV after successful conversion.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-convert even if Parquet exists and is newer.",
    )
    args = parser.parse_args()

    root = args.root
    if not root.exists():
        print(f"Root path not found: {root}")
        return 1

    if args.pattern:
        patterns = tuple(args.pattern)
    elif args.all:
        patterns = ("*.csv",)
    else:
        patterns = DEFAULT_PATTERNS

    csv_files = _iter_csv_files(root, patterns, DEFAULT_EXCLUDES)
    if not csv_files:
        print("No CSV files matched.")
        return 0

    converted = 0
    skipped = 0
    failed = 0
    start = time.time()

    for csv_path in csv_files:
        parquet_path = csv_path.with_suffix(".parquet")
        if parquet_path.exists() and not args.force:
            if parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
                if args.remove_csv:
                    try:
                        csv_path.unlink()
                        converted += 1
                    except Exception as exc:
                        failed += 1
                        print(f"Failed to remove CSV: {csv_path} ({exc})")
                else:
                    skipped += 1
                continue

        try:
            used_polars = _convert_with_polars(csv_path, parquet_path)
            if not used_polars:
                _convert_with_pandas(csv_path, parquet_path)
            if args.remove_csv:
                try:
                    csv_path.unlink()
                except Exception as exc:
                    failed += 1
                    print(f"Failed to remove CSV: {csv_path} ({exc})")
            converted += 1
        except Exception as exc:
            failed += 1
            print(f"Failed: {csv_path} ({exc})")

    elapsed = time.time() - start
    print(
        f"Done in {elapsed:.1f}s. Converted: {converted}, skipped: {skipped}, failed: {failed}."
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
