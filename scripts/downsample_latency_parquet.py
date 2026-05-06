#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import numpy as np
    import polars as pl
except Exception:
    np = None
    pl = None


@dataclass
class FileResult:
    path: Path
    status: str
    rows_before: int
    rows_after: int
    bytes_before: int
    bytes_after: int
    p999_before: float | None
    p999_after: float | None
    rel_err: float | None
    fraction_used: float | None
    note: str = ""


@dataclass
class ManifestState:
    path: Path
    manifest: dict[str, Any]
    files: dict[str, Any]


def _iter_latency_files(roots: list[Path], pattern: str) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        files.extend(sorted(root.rglob(pattern)))
    return sorted(set(files))


def _stable_seed(path: Path, base_seed: int) -> int:
    digest = hashlib.blake2b(str(path).encode("utf-8"), digest_size=8).digest()
    return (int.from_bytes(digest, "big") + base_seed) % (2**31 - 1)


def _find_latency_column(df: "pl.DataFrame") -> str | None:
    if "latency_ns" in df.columns:
        return "latency_ns"
    for col in df.columns:
        if "latency" in col.lower():
            return col
    return None


def _to_numeric_numpy(series: "pl.Series") -> "np.ndarray":
    return (
        series.cast(pl.Float64, strict=False)
        .drop_nulls()
        .to_numpy()
    )


def _p999(values: "np.ndarray") -> float | None:
    if values.size == 0:
        return None
    return float(np.quantile(values, 0.999, method="linear"))


def _candidate_fractions(base_fraction: float, adaptive: bool) -> list[float]:
    base = min(max(base_fraction, 1e-6), 1.0)
    if not adaptive:
        return [base]
    candidates = [base]
    while candidates[-1] < 1.0:
        nxt = min(1.0, candidates[-1] * 2.0)
        if abs(nxt - candidates[-1]) < 1e-12:
            break
        candidates.append(nxt)
    return candidates


def _sample_df(df: "pl.DataFrame", fraction: float, min_rows: int, seed: int) -> "pl.DataFrame":
    n_rows = df.height
    if n_rows == 0 or fraction >= 1.0:
        return df

    keep = int(round(n_rows * fraction))
    keep = max(keep, min_rows)
    keep = min(keep, n_rows)
    if keep >= n_rows:
        return df
    return df.sample(n=keep, with_replacement=False, shuffle=True, seed=seed)


def _downsample_file(
    path: Path,
    target_fraction: float,
    max_p999_rel_error: float,
    min_rows: int,
    seed: int,
    adaptive: bool,
    dry_run: bool,
) -> FileResult:
    bytes_before = path.stat().st_size

    df = pl.read_parquet(path)
    rows_before = df.height
    if rows_before == 0:
        return FileResult(
            path=path,
            status="skipped",
            rows_before=0,
            rows_after=0,
            bytes_before=bytes_before,
            bytes_after=bytes_before,
            p999_before=None,
            p999_after=None,
            rel_err=None,
            fraction_used=None,
            note="empty file",
        )

    latency_col = _find_latency_column(df)
    if latency_col is None:
        return FileResult(
            path=path,
            status="skipped",
            rows_before=rows_before,
            rows_after=rows_before,
            bytes_before=bytes_before,
            bytes_after=bytes_before,
            p999_before=None,
            p999_after=None,
            rel_err=None,
            fraction_used=None,
            note="no latency column",
        )

    full_values = _to_numeric_numpy(df[latency_col])
    p999_before = _p999(full_values)
    if p999_before is None:
        return FileResult(
            path=path,
            status="skipped",
            rows_before=rows_before,
            rows_after=rows_before,
            bytes_before=bytes_before,
            bytes_after=bytes_before,
            p999_before=None,
            p999_after=None,
            rel_err=None,
            fraction_used=None,
            note="latency column has no numeric values",
        )

    chosen_df: pl.DataFrame | None = None
    chosen_fraction: float | None = None
    chosen_p999: float | None = None
    chosen_rel_err: float | None = None

    file_seed = _stable_seed(path, seed)
    for fraction in _candidate_fractions(target_fraction, adaptive=adaptive):
        sampled = _sample_df(df, fraction=fraction, min_rows=min_rows, seed=file_seed)
        sampled_values = _to_numeric_numpy(sampled[latency_col])
        sample_p999 = _p999(sampled_values)
        if sample_p999 is None:
            continue
        if p999_before == 0.0:
            rel_err = 0.0
        else:
            rel_err = abs(sample_p999 - p999_before) / abs(p999_before)

        chosen_df = sampled
        chosen_fraction = fraction
        chosen_p999 = sample_p999
        chosen_rel_err = rel_err
        if rel_err <= max_p999_rel_error:
            break

    if chosen_df is None or chosen_fraction is None or chosen_p999 is None or chosen_rel_err is None:
        return FileResult(
            path=path,
            status="skipped",
            rows_before=rows_before,
            rows_after=rows_before,
            bytes_before=bytes_before,
            bytes_after=bytes_before,
            p999_before=p999_before,
            p999_after=None,
            rel_err=None,
            fraction_used=None,
            note="unable to produce a valid sample",
        )

    rows_after = chosen_df.height
    if rows_after >= rows_before:
        return FileResult(
            path=path,
            status="kept",
            rows_before=rows_before,
            rows_after=rows_before,
            bytes_before=bytes_before,
            bytes_after=bytes_before,
            p999_before=p999_before,
            p999_after=p999_before,
            rel_err=0.0,
            fraction_used=1.0,
            note="kept full file to satisfy p99.9 error threshold",
        )

    if dry_run:
        bytes_after_est = int(round(bytes_before * (rows_after / rows_before)))
        return FileResult(
            path=path,
            status="would_write",
            rows_before=rows_before,
            rows_after=rows_after,
            bytes_before=bytes_before,
            bytes_after=bytes_after_est,
            p999_before=p999_before,
            p999_after=chosen_p999,
            rel_err=chosen_rel_err,
            fraction_used=chosen_fraction,
            note="dry run",
        )

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        chosen_df.write_parquet(tmp_path, compression="zstd")
        bytes_after = tmp_path.stat().st_size
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return FileResult(
        path=path,
        status="written",
        rows_before=rows_before,
        rows_after=rows_after,
        bytes_before=bytes_before,
        bytes_after=bytes_after,
        p999_before=p999_before,
        p999_after=chosen_p999,
        rel_err=chosen_rel_err,
        fraction_used=chosen_fraction,
    )


def _format_bytes(num_bytes: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    unit = 0
    while value >= 1024.0 and unit < len(units) - 1:
        value /= 1024.0
        unit += 1
    return f"{value:.2f} {units[unit]}"


def _manifest_key(path: Path, base_dir: Path) -> str:
    try:
        return path.resolve().relative_to(base_dir.resolve()).as_posix()
    except Exception:
        return path.as_posix()


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "files": {}}

    try:
        data = json.loads(path.read_text())
    except Exception:
        return {"version": 1, "files": {}}

    if not isinstance(data, dict):
        return {"version": 1, "files": {}}
    files = data.get("files")
    if not isinstance(files, dict):
        data["files"] = {}
    data.setdefault("version", 1)
    return data


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    manifest = dict(manifest)
    files = manifest.get("files", {})
    if isinstance(files, dict):
        manifest["files"] = {k: files[k] for k in sorted(files)}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n")


def _manifest_entry(path: Path, result: FileResult | None = None) -> dict[str, Any]:
    st = path.stat()
    entry: dict[str, Any] = {
        "size_bytes": int(st.st_size),
        "mtime_ns": int(st.st_mtime_ns),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if result is not None:
        entry.update(
            {
                "status": result.status,
                "rows_after": result.rows_after,
                "bytes_after": result.bytes_after,
                "fraction_used": result.fraction_used,
                "p999_after": result.p999_after,
                "p999_rel_error": result.rel_err,
            }
        )
    return entry


def _build_manifest_states(
    files: list[Path],
    use_manifest: bool,
    local_manifest_name: str,
) -> dict[Path, ManifestState]:
    if not use_manifest:
        return {}

    states: dict[Path, ManifestState] = {}
    for parent in sorted({f.parent for f in files}):
        manifest_path = parent / local_manifest_name
        manifest = _load_manifest(manifest_path)
        mf = manifest.get("files", {})
        manifest_files = mf if isinstance(mf, dict) else {}
        manifest["files"] = manifest_files
        states[parent] = ManifestState(path=manifest_path, manifest=manifest, files=manifest_files)
    return states


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Downsample latency parquet files while constraining p99.9 error. "
            "The script preserves all columns and rewrites files with zstd."
        )
    )
    parser.add_argument(
        "roots",
        nargs="*",
        type=Path,
        default=[Path("data"), Path("data_paper")],
        help="Root directories to scan. Defaults to: data data_paper",
    )
    parser.add_argument(
        "--pattern",
        default="*latency*.parquet",
        help="Glob pattern for latency files.",
    )
    parser.add_argument(
        "--fraction",
        type=float,
        default=0.10,
        help="Starting row fraction to keep (0, 1]. Defaults to 0.10.",
    )
    parser.add_argument(
        "--max-p999-rel-error",
        type=float,
        default=0.05,
        help="Maximum allowed relative error on p99.9. Defaults to 0.05 (5%%).",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=100_000,
        help="Minimum rows to keep per file (if available). Defaults to 100000.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Base random seed for deterministic sampling.",
    )
    parser.add_argument(
        "--fixed-fraction",
        action="store_true",
        help="Disable adaptive fraction increase and keep exactly --fraction for all files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate and report without rewriting files.",
    )
    parser.add_argument(
        "--manifest-file",
        default=".downsample_latency_manifest.json",
        help=(
            "Per-directory manifest file name. "
            "A local manifest is stored next to each matching latency file directory."
        ),
    )
    parser.add_argument(
        "--no-manifest",
        action="store_true",
        help="Disable manifest loading/saving.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Process files even if manifest marks them unchanged.",
    )
    parser.add_argument(
        "--register-existing",
        action="store_true",
        help=(
            "Register matching files in manifest without rewriting data. "
            "Useful to seed manifest after an earlier run."
        ),
    )
    args = parser.parse_args()

    if np is None or pl is None:
        print(
            "Error: downsample_latency_parquet.py requires both numpy and polars.",
            file=sys.stderr,
        )
        return 2

    if not (0.0 < args.fraction <= 1.0):
        print("Error: --fraction must be in (0, 1].", file=sys.stderr)
        return 2
    if args.max_p999_rel_error < 0.0:
        print("Error: --max-p999-rel-error must be >= 0.", file=sys.stderr)
        return 2
    if args.min_rows < 1:
        print("Error: --min-rows must be >= 1.", file=sys.stderr)
        return 2
    if not isinstance(args.manifest_file, str) or not args.manifest_file.strip():
        print("Error: --manifest-file must be a non-empty filename.", file=sys.stderr)
        return 2

    files = _iter_latency_files(args.roots, args.pattern)
    if not files:
        print("No matching latency parquet files found.")
        return 0

    local_manifest_name = Path(args.manifest_file).name
    local_manifest_states = _build_manifest_states(
        files=files,
        use_manifest=not args.no_manifest,
        local_manifest_name=local_manifest_name,
    )

    if args.register_existing:
        if args.no_manifest:
            print("Error: --register-existing requires manifest support (do not pass --no-manifest).", file=sys.stderr)
            return 2
        registered = 0
        for file_path in files:
            manifest_state = local_manifest_states.get(file_path.parent)
            if manifest_state is None:
                continue
            key = _manifest_key(file_path, file_path.parent)
            manifest_state.files[key] = _manifest_entry(file_path)
            manifest_state.files[key]["status"] = "registered_existing"
            registered += 1
        touched = sorted({f.parent for f in files})
        for parent in touched:
            state = local_manifest_states.get(parent)
            if state is None:
                continue
            _save_manifest(state.path, state.manifest)
        print(f"Registered {registered} file(s) across {len(touched)} local manifest(s): {local_manifest_name}")
        return 0

    print(
        f"Scanning {len(files)} file(s), target fraction={args.fraction:.4f}, "
        f"max p99.9 rel error={args.max_p999_rel_error:.4f}"
    )

    results: list[FileResult] = []
    for idx, file_path in enumerate(files, 1):
        manifest_state = local_manifest_states.get(file_path.parent)
        if manifest_state is not None and not args.force:
            key = _manifest_key(file_path, file_path.parent)
            entry = manifest_state.files.get(key)
            if isinstance(entry, dict):
                st = file_path.stat()
                if entry.get("size_bytes") == int(st.st_size) and entry.get("mtime_ns") == int(st.st_mtime_ns):
                    result = FileResult(
                        path=file_path,
                        status="skipped_manifest",
                        rows_before=0,
                        rows_after=0,
                        bytes_before=int(st.st_size),
                        bytes_after=int(st.st_size),
                        p999_before=None,
                        p999_after=None,
                        rel_err=None,
                        fraction_used=None,
                        note="unchanged since manifest entry",
                    )
                    results.append(result)
                    print(
                        f"[{idx:>3}/{len(files)}] {result.status:>10} "
                        f"rows {result.rows_before}->{result.rows_after} "
                        f"bytes {result.bytes_before}->{result.bytes_after} "
                        f"frac=- p999_err=- {result.path} ({result.note})"
                    )
                    continue

        result = _downsample_file(
            path=file_path,
            target_fraction=args.fraction,
            max_p999_rel_error=args.max_p999_rel_error,
            min_rows=args.min_rows,
            seed=args.seed,
            adaptive=not args.fixed_fraction,
            dry_run=args.dry_run,
        )
        results.append(result)

        if manifest_state is not None and not args.dry_run and result.status in {"written", "kept"}:
            key = _manifest_key(file_path, file_path.parent)
            manifest_state.files[key] = _manifest_entry(file_path, result=result)

        rel_err_str = "-" if result.rel_err is None else f"{result.rel_err:.4%}"
        frac_str = "-" if result.fraction_used is None else f"{result.fraction_used:.4f}"
        print(
            f"[{idx:>3}/{len(files)}] {result.status:>10} "
            f"rows {result.rows_before}->{result.rows_after} "
            f"bytes {result.bytes_before}->{result.bytes_after} "
            f"frac={frac_str} p999_err={rel_err_str} {result.path}"
            + (f" ({result.note})" if result.note else "")
        )

    written = sum(1 for r in results if r.status == "written")
    would_write = sum(1 for r in results if r.status == "would_write")
    skipped = sum(1 for r in results if r.status == "skipped")
    skipped_manifest = sum(1 for r in results if r.status == "skipped_manifest")
    kept = sum(1 for r in results if r.status == "kept")
    before_total = sum(r.bytes_before for r in results)
    after_total = sum(r.bytes_after for r in results)
    saved = before_total - after_total

    print("\nSummary")
    print(f"  files:         {len(results)}")
    print(f"  written:       {written}")
    print(f"  would_write:   {would_write}")
    print(f"  kept_full:     {kept}")
    print(f"  skipped:       {skipped}")
    print(f"  skipped_cache: {skipped_manifest}")
    print(f"  size before:   {_format_bytes(before_total)}")
    print(f"  size after:    {_format_bytes(after_total)}")
    print(f"  size saved:    {_format_bytes(saved)}")

    if not args.no_manifest and not args.dry_run:
        saved_count = 0
        for state in local_manifest_states.values():
            _save_manifest(state.path, state.manifest)
            saved_count += 1
        print(f"  manifests:     {saved_count} local file(s) named {local_manifest_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
