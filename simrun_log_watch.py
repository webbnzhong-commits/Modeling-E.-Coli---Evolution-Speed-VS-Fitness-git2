#!/usr/bin/env python3
from __future__ import annotations

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


def _read_positive_float(prompt: str) -> float:
    while True:
        raw = input(prompt).strip()
        try:
            value = float(raw)
        except Exception:
            print("Enter a number.")
            continue
        if value <= 0:
            print("Enter a value greater than 0.")
            continue
        return value


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    run_logs_dir = repo_root / "results" / "run_logs"
    run_logs_dir.mkdir(parents=True, exist_ok=True)

    default_log = _latest_simrun_log(run_logs_dir)
    default_label = str(default_log) if default_log is not None else ""
    chosen = input(f"Log path [{default_label}]: ").strip()
    if chosen:
        log_path = Path(chosen).expanduser()
        if not log_path.is_absolute():
            log_path = (Path.cwd() / log_path).resolve()
    else:
        if default_log is None:
            raise SystemExit("No simRun log found. Start simRun first or provide a log path.")
        log_path = default_log

    minutes = _read_positive_float("Watch for how many minutes? ")
    interval_seconds = _read_positive_float("Check every how many seconds? ")
    deadline = time.time() + (minutes * 60.0)

    last_size = 0
    if log_path.exists():
        try:
            last_size = int(log_path.stat().st_size)
        except Exception:
            last_size = 0
        print(f"Watching updates in {log_path}")
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
