#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from pathlib import Path


def _latest_simrun_log(run_logs_dir: Path) -> Path | None:
    pointer = run_logs_dir / "latest_simrun_log.txt"
    if pointer.exists():
        try:
            candidate = Path(pointer.read_text(encoding="utf-8").strip()).expanduser()
            if candidate.exists():
                return candidate
        except Exception:
            pass
    files = sorted(run_logs_dir.glob("simrun_*.log"), key=lambda path: path.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _latest_hub_num(results_root: Path) -> int | None:
    latest: int | None = None
    for root in (results_root / "hub", results_root):
        if not root.is_dir():
            continue
        try:
            entries = list(root.iterdir())
        except Exception:
            continue
        for entry in entries:
            if not entry.is_dir():
                continue
            name = str(entry.name)
            if not name.startswith("hub_"):
                continue
            suffix = name[4:]
            if not suffix.isdigit():
                continue
            num = int(suffix)
            if latest is None or num > latest:
                latest = num
    return latest


def _hub_simrun_log_path(results_root: Path, hub_num: int) -> Path:
    candidate_paths = [
        results_root / "hub" / f"hub_{hub_num}" / "simrun_print_log.csv",
        results_root / f"hub_{hub_num}" / "simrun_print_log.csv",
    ]
    for path in candidate_paths:
        if path.exists():
            return path
    return candidate_paths[0]


def _positive_float(value: float, label: str) -> float:
    try:
        out = float(value)
    except Exception as exc:
        raise SystemExit(f"Invalid {label}: {value}") from exc
    if out <= 0:
        raise SystemExit(f"{label} must be greater than 0")
    return out


def _read_hub_log_and_minutes(repo_root: Path) -> tuple[float, Path]:
    results_root = repo_root / "results"
    latest_hub = _latest_hub_num(results_root)
    latest_label = str(latest_hub) if latest_hub is not None else "none found"
    print(f"Latest simRun (hub) number: {latest_label}")
    while True:
        hub_raw = input("simNum for simRun log (empty = latest): ").strip()
        if not hub_raw:
            if latest_hub is None:
                print("No existing simRun hubs found yet. Enter a simNum.")
                continue
            hub_num = int(latest_hub)
            print(f"Using latest simNum: {hub_num}")
            break
        try:
            hub_num = int(hub_raw)
        except Exception:
            print("Enter a whole number (or leave empty for latest).")
            continue
        if hub_num < 0:
            print("simNum must be >= 0.")
            continue
        break
    sim_log_path = _hub_simrun_log_path(results_root, hub_num)
    while True:
        mins_raw = input("Watch for how many minutes? ").strip()
        try:
            mins = _positive_float(mins_raw, "minutes")
            return mins, sim_log_path
        except SystemExit as exc:
            print(exc)


def _read_minutes_and_optional_sim_log(repo_root: Path) -> tuple[float, Path | None]:
    while True:
        raw = input("Watch for how many minutes? (number or 'sim'): ").strip()
        if not raw:
            print("Enter a number or 'sim'.")
            continue
        if raw.lower() == "sim":
            mins, sim_log_path = _read_hub_log_and_minutes(repo_root)
            return mins, sim_log_path
        try:
            mins = _positive_float(raw, "minutes")
            return mins, None
        except SystemExit as exc:
            print(exc)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Watch the latest simRun log and print new output as it is appended.",
    )
    parser.add_argument(
        "--log-path",
        default="",
        help="Optional explicit log path. Default: latest simRun log.",
    )
    parser.add_argument(
        "--minutes",
        default="",
        help="How many minutes to watch. If omitted, prompts interactively. Enter 'sim' to pick a hub simRun log.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=1.0,
        help="How often to poll for updates in seconds (default: 1).",
    )
    parser.add_argument(
        "--from-start",
        action="store_true",
        help="Read from the beginning of the file instead of only new lines.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    run_logs_dir = repo_root / "results" / "run_logs"
    run_logs_dir.mkdir(parents=True, exist_ok=True)

    chosen = str(args.log_path).strip()
    if chosen:
        log_path = Path(chosen).expanduser()
        if not log_path.is_absolute():
            log_path = (Path.cwd() / log_path).resolve()
    else:
        default_log = _latest_simrun_log(run_logs_dir)
        if default_log is None:
            raise SystemExit("No simRun log found. Start simRun first.")
        log_path = default_log

    minutes_arg = str(args.minutes).strip()
    if minutes_arg:
        if minutes_arg.lower() == "sim":
            minutes, sim_log_path = _read_hub_log_and_minutes(repo_root)
            log_path = sim_log_path
        else:
            minutes = _positive_float(minutes_arg, "minutes")
    else:
        minutes, sim_log_path = _read_minutes_and_optional_sim_log(repo_root)
        if sim_log_path is not None:
            log_path = sim_log_path
    interval_seconds = _positive_float(args.interval_seconds, "interval-seconds")
    deadline = time.time() + (minutes * 60.0)

    last_size = 0
    if log_path.exists():
        if not args.from_start:
            try:
                last_size = int(log_path.stat().st_size)
            except Exception:
                last_size = 0
        print(f"Watching updates in {log_path}")
        if args.from_start:
            print("(Starting from beginning of existing file.)")
        else:
            print("(Starting from end of existing file; only new updates will print.)")
    else:
        print(f"Waiting for log file to appear: {log_path}")

    while time.time() < deadline:
        if log_path.exists():
            try:
                size = int(log_path.stat().st_size)
            except Exception:
                size = last_size
            if size < last_size:
                last_size = 0
                print("[INFO] log file was truncated; restarting from beginning.")
            if size > last_size:
                try:
                    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
                        handle.seek(last_size)
                        delta = handle.read()
                        last_size = int(handle.tell())
                except Exception:
                    delta = ""
                if delta:
                    print(delta, end="" if delta.endswith("\n") else "\n")
        time.sleep(interval_seconds)

    print(f"\nFinished watching after {minutes} minute(s).")


if __name__ == "__main__":
    main()
