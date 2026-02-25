#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import select
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    import termios
    import tty
except Exception:
    termios = None
    tty = None


TERMINAL_STEP_STATUSES = {"ok", "failed", "no_master", "stopped", "aborted"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Start a brand-new hub run (headless), print simplified progress every 10 seconds, "
            "and force-stop leftover simulation processes after completion."
        )
    )
    parser.add_argument("--results-root", default="results", help="Results root (default: results)")
    parser.add_argument("--script", default="simulation_entry.py", help="Simulation entry script")
    parser.add_argument("--count", type=int, default=None, help="Simulations per master")
    parser.add_argument("--start-rate", type=float, default=None)
    parser.add_argument("--end-rate", type=float, default=None)
    parser.add_argument("--step", type=float, default=None)
    parser.add_argument("--species-threshold", type=int, default=None)
    parser.add_argument("--max-masters", type=int, default=None)
    parser.add_argument("--skip-plots", action="store_true")
    parser.add_argument(
        "--show-runner-output",
        action="store_true",
        help="Show hub_runner stdout/stderr for debugging (default: hidden)",
    )
    parser.add_argument(
        "--print-every-seconds",
        type=int,
        default=10,
        help="Progress print interval in seconds (default: 10)",
    )
    parser.add_argument(
        "--session-log",
        default=None,
        help=(
            "Write simRun progress prints to this log file. "
            "Default: results/run_logs/simrun_<pid>_<timestamp>.log"
        ),
    )
    return parser.parse_args()


def _hub_roots(results_root: Path) -> list[Path]:
    return [results_root / "hub", results_root]


def _is_hub_dir(path: Path) -> bool:
    name = str(path.name)
    return path.is_dir() and name.startswith("hub_") and name[4:].isdigit()


def _discover_hub_dirs(results_root: Path) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for root in _hub_roots(results_root):
        if not root.is_dir():
            continue
        for entry in root.iterdir():
            if not _is_hub_dir(entry):
                continue
            try:
                key = str(entry.resolve())
            except Exception:
                key = str(entry)
            if key in seen:
                continue
            seen.add(key)
            out.append(entry)
    return out


def _hub_index(path: Path) -> int:
    try:
        return int(str(path.name).split("_", 1)[1])
    except Exception:
        return -1


def _read_hub_meta(hub_dir: Path) -> dict | None:
    hub_meta_path = hub_dir / "hub_meta.json"
    # hub_runner rewrites this file frequently; retry briefly if we read mid-write.
    for attempt in range(4):
        try:
            payload = json.loads(hub_meta_path.read_text())
        except Exception:
            if attempt < 3:
                time.sleep(0.05)
                continue
            return None
        if isinstance(payload, dict):
            return payload
        return None
    return None


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _format_duration(seconds: float) -> str:
    sec = max(0, int(seconds))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _runtime_prediction_from_steps(
    steps: list[dict],
    total_steps: int,
    elapsed_s: float,
    now_ts: float,
    is_running: bool = False,
) -> dict:
    done = 0
    durations: list[float] = []
    done_duration_sum = 0.0
    running_elapsed = None

    for step in steps:
        if not isinstance(step, dict):
            continue
        status = str(step.get("status", "unknown"))
        if status in TERMINAL_STEP_STATUSES:
            done += 1
            dur = _safe_float(step.get("duration_s"))
            if dur is not None and dur > 0:
                durations.append(float(dur))
                done_duration_sum += float(dur)
        elif status == "running":
            started_at = _safe_float(step.get("started_at"))
            if started_at is not None:
                running_elapsed = max(0.0, float(now_ts - started_at))

    remaining_steps = max(0, int(total_steps) - int(done))
    # Keep ETA strict: only estimate from completed steps with known durations.
    if done <= 0 or len(durations) <= 0 or len(durations) < done:
        return {
            "remaining_s": None,
            "predicted_total_s": None,
            "eta_ts": None,
        }

    if running_elapsed is None and is_running and remaining_steps > 0 and elapsed_s > 0:
        inferred = max(0.0, float(elapsed_s) - float(done_duration_sum))
        if inferred > 0:
            running_elapsed = inferred

    avg_step_s = float(sum(durations) / len(durations))

    remaining_s = None
    predicted_total_s = None
    eta_ts = None
    if remaining_steps == 0:
        remaining_s = 0.0
    elif is_running and running_elapsed is not None:
        remaining_s = max(0.0, avg_step_s - float(running_elapsed)) + (
            avg_step_s * max(0, remaining_steps - 1)
        )
    else:
        remaining_s = avg_step_s * remaining_steps
    predicted_total_s = float(elapsed_s + remaining_s)
    eta_ts = float(now_ts + remaining_s)

    return {
        "remaining_s": remaining_s,
        "predicted_total_s": predicted_total_s,
        "eta_ts": eta_ts,
    }


def _rate_label(rate: float) -> str:
    return f"env_{float(rate):.2f}".replace(".", "p")


def _master_index(path: Path) -> int:
    try:
        return int(str(path.name).split("_", 1)[1])
    except Exception:
        return -1


def _safe_read_json(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _running_master_metrics(summary: dict) -> dict | None:
    if not isinstance(summary, dict):
        return None
    if str(summary.get("status", "")) != "running":
        return None
    next_idx = summary.get("next_step_idx")
    current_rate = _safe_float(summary.get("current_env_rate"))
    hub_dir = summary.get("hub_dir")
    if (not isinstance(next_idx, int)) or (current_rate is None) or (not isinstance(hub_dir, Path)):
        return None

    env_dir = hub_dir / _rate_label(float(current_rate))
    if not env_dir.is_dir():
        return None

    master_dirs = []
    for child in env_dir.iterdir():
        if child.is_dir() and str(child.name).startswith("master_"):
            master_dirs.append(child)
    if master_dirs:
        master_dirs.sort(key=lambda p: (_master_index(p), str(p.name)))
        master_dir = master_dirs[-1]
        try:
            master_run_num = int(master_dir.name.split("_", 1)[1])
        except Exception:
            master_run_num = None
    else:
        master_dir = None
        master_run_num = None

    run_dirs = []
    for child in env_dir.iterdir():
        if not child.is_dir():
            continue
        try:
            int(child.name)
            run_dirs.append(child)
        except Exception:
            continue
    if not run_dirs:
        return {
            "master_run_num": master_run_num,
            "run_count": 0,
            "avg_interval_1000": None,
            "avg_fps": None,
            "species_measure": None,
            "total_species": None,
            "max_species": None,
            "max_frames": None,
            "env_dir": env_dir,
            "master_dir": master_dir,
        }

    total_species = 0.0
    has_species = False
    max_species = None
    max_frames = None
    interval_sum = 0.0
    interval_count = 0

    for run_dir in run_dirs:
        meta = _safe_read_json(run_dir / "run_meta.json")
        if isinstance(meta, dict):
            species = _safe_float(meta.get("amnt_of_species"))
            if species is not None:
                has_species = True
                total_species += float(species)
                if max_species is None:
                    max_species = float(species)
                else:
                    max_species = max(float(max_species), float(species))
            frames = _safe_float(meta.get("frame_count"))
            if frames is not None:
                if max_frames is None:
                    max_frames = float(frames)
                else:
                    max_frames = max(float(max_frames), float(frames))

        fps_path = run_dir / "fps_log.csv"
        if fps_path.exists():
            try:
                with fps_path.open("r", encoding="utf-8", errors="ignore") as handle:
                    for raw in handle:
                        line = raw.strip()
                        if not line or line.startswith("timestamp"):
                            continue
                        parts = line.split(",", 1)
                        if len(parts) != 2:
                            continue
                        interval = _safe_float(parts[1])
                        if interval is None or interval <= 0:
                            continue
                        interval_sum += float(interval)
                        interval_count += 1
            except Exception:
                pass

    avg_interval = (interval_sum / interval_count) if interval_count > 0 else None
    avg_fps = (1000.0 / avg_interval) if (avg_interval is not None and avg_interval > 0) else None
    species_measure = float(total_species) if has_species else max_species

    return {
        "master_run_num": master_run_num,
        "run_count": int(len(run_dirs)),
        "avg_interval_1000": avg_interval,
        "avg_fps": avg_fps,
        "species_measure": species_measure,
        "total_species": (float(total_species) if has_species else None),
        "max_species": max_species,
        "max_frames": max_frames,
        "env_dir": env_dir,
        "master_dir": master_dir,
    }


def _snapshot_summary(hub_dir: Path) -> dict | None:
    meta = _read_hub_meta(hub_dir)
    if not isinstance(meta, dict):
        return None
    rates = meta.get("rates")
    steps = meta.get("steps")
    total = len(rates) if isinstance(rates, list) else 0
    if not isinstance(steps, list):
        steps = []

    counts: dict[str, int] = {}
    done = 0
    now_ts = time.time()
    for step in steps:
        if not isinstance(step, dict):
            continue
        status = str(step.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1
        if status in TERMINAL_STEP_STATUSES:
            done += 1

    created_at = _safe_float(meta.get("created_at"))
    elapsed = now_ts - created_at if created_at is not None else 0.0
    status = str(meta.get("status", "unknown"))
    next_step_idx = done + 1 if (status == "running" and done < total) else None
    current_env_rate = None
    if next_step_idx is not None and isinstance(rates, list) and (0 <= (next_step_idx - 1) < len(rates)):
        current_env_rate = _safe_float(rates[next_step_idx - 1])
    species_threshold = _safe_int(meta.get("species_threshold"), 0)
    running_metrics: dict | None = None
    running_step_fraction = 0.0
    if status == "running":
        running_metrics = _running_master_metrics(
            {
                "status": status,
                "next_step_idx": next_step_idx,
                "current_env_rate": current_env_rate,
                "hub_dir": hub_dir,
            }
        )
        if species_threshold > 0 and isinstance(running_metrics, dict):
            species_measure = _safe_float(running_metrics.get("species_measure"))
            if species_measure is not None:
                running_step_fraction = max(
                    0.0,
                    min(1.0, float(species_measure) / float(species_threshold)),
                )

    progress_units = max(
        0.0,
        min(float(total), float(done) + float(running_step_fraction)),
    )
    progress_pct = (progress_units / float(total) * 100.0) if total > 0 else None

    prediction = _runtime_prediction_from_steps(
        steps=steps,
        total_steps=total,
        elapsed_s=elapsed,
        now_ts=now_ts,
        is_running=(status == "running"),
    )
    remaining_s = _safe_float(prediction.get("remaining_s"))
    predicted_total_s = _safe_float(prediction.get("predicted_total_s"))
    eta_ts = _safe_float(prediction.get("eta_ts"))
    if (
        total > 0
        and progress_units > 0
        and (
            remaining_s is None
            or predicted_total_s is None
            or eta_ts is None
        )
    ):
        completion_ratio = progress_units / float(total)
        predicted_total_s = float(elapsed / completion_ratio)
        remaining_s = max(0.0, float(predicted_total_s) - float(elapsed))
        eta_ts = float(now_ts + float(remaining_s))

    last_step = None
    valid_steps: list[dict] = []
    for step in steps:
        if isinstance(step, dict):
            valid_steps.append(step)
    if valid_steps:
        valid_steps.sort(key=lambda row: _safe_int(row.get("step_index"), -1))
        last_step = valid_steps[-1]

    return {
        "hub_idx": _hub_index(hub_dir),
        "hub_dir": hub_dir,
        "status": status,
        "done": int(done),
        "total": int(total),
        "progress_pct": progress_pct,
        "species_threshold": max(0, int(species_threshold)),
        "counts": counts,
        "elapsed_s": float(elapsed),
        "remaining_s": remaining_s,
        "predicted_total_s": predicted_total_s,
        "eta_ts": eta_ts,
        "next_step_idx": next_step_idx,
        "current_env_rate": current_env_rate,
        "running_metrics": running_metrics,
        "last_step": last_step,
    }


def _print_snapshot(summary: dict, final: bool = False) -> None:
    phase = "FINAL" if final else "UPDATE"
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    counts = summary.get("counts", {}) if isinstance(summary, dict) else {}
    ok = _safe_int(counts.get("ok"), 0)
    failed = _safe_int(counts.get("failed"), 0)
    stopped = _safe_int(counts.get("stopped"), 0)
    no_master = _safe_int(counts.get("no_master"), 0)
    aborted = _safe_int(counts.get("aborted"), 0)

    done = _safe_int(summary.get("done"), 0)
    total = _safe_int(summary.get("total"), 0)
    progress_pct = _safe_float(summary.get("progress_pct"))
    progress_text = f"{done}/{total}"
    if progress_pct is not None:
        progress_text = f"{progress_text} ({progress_pct:.2f}%)"
    print(
        f"[{ts}] [{phase}] hub_{summary.get('hub_idx')} "
        f"status={summary.get('status')} "
        f"progress={progress_text} "
        f"ok={ok} failed={failed} stopped={stopped} no_master={no_master} aborted={aborted} "
        f"elapsed={_format_duration(_safe_float(summary.get('elapsed_s')) or 0.0)}"
    )
    remaining_s = _safe_float(summary.get("remaining_s"))
    predicted_total_s = _safe_float(summary.get("predicted_total_s"))
    eta_ts = _safe_float(summary.get("eta_ts"))
    if remaining_s is not None and predicted_total_s is not None and eta_ts is not None:
        finish_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(eta_ts)))
        print(
            f"  estimate remaining={_format_duration(remaining_s)} "
            f"total={_format_duration(predicted_total_s)} finish_at={finish_at}"
        )

    next_idx = summary.get("next_step_idx")
    current_rate = summary.get("current_env_rate")
    if isinstance(next_idx, int) and _safe_float(current_rate) is not None:
        print(f"  running step={next_idx} env_rate={float(current_rate):.6g}")
        running_metrics = summary.get("running_metrics")
        if not isinstance(running_metrics, dict):
            running_metrics = _running_master_metrics(summary)
        if isinstance(running_metrics, dict):
            master_num = running_metrics.get("master_run_num")
            run_count = _safe_int(running_metrics.get("run_count"), 0)
            avg_interval = _safe_float(running_metrics.get("avg_interval_1000"))
            avg_fps = _safe_float(running_metrics.get("avg_fps"))
            total_species = _safe_float(running_metrics.get("total_species"))
            max_species = _safe_float(running_metrics.get("max_species"))
            species_measure = _safe_float(running_metrics.get("species_measure"))
            species_threshold = _safe_int(summary.get("species_threshold"), 0)
            max_frames = _safe_float(running_metrics.get("max_frames"))
            species_text = f"{total_species:.1f}" if total_species is not None else "--"
            species_max_text = f"{max_species:.1f}" if max_species is not None else "--"
            if species_threshold > 0:
                if species_measure is not None:
                    species_progress = (
                        f"{species_measure:.1f}/{species_threshold} "
                        f"({(100.0 * float(species_measure) / float(species_threshold)):.2f}%)"
                    )
                else:
                    species_progress = f"--/{species_threshold}"
            else:
                species_progress = "--"
            frames_text = f"{max_frames:.0f}" if max_frames is not None else "--"
            if master_num is None:
                master_text = "master=--"
            else:
                master_text = f"master=master_{int(master_num)}"
            if avg_interval is not None and avg_fps is not None:
                perf = f"avg_1000_iter={avg_interval:.2f}s avg_fps={avg_fps:.2f}"
            else:
                perf = "avg_1000_iter=-- avg_fps=--"
            print(
                f"  running {master_text} sims={run_count} {perf} "
                f"species_progress={species_progress} "
                f"species_total={species_text} species_max={species_max_text} frames_max={frames_text}"
            )

    last_step = summary.get("last_step")
    if isinstance(last_step, dict):
        step_num = _safe_int(last_step.get("step_index"), -1) + 1
        env_rate = _safe_float(last_step.get("env_rate"))
        master_run = last_step.get("master_run_num")
        fit = last_step.get("fit")
        r2 = None
        apex_x = None
        apex_y = None
        if isinstance(fit, dict):
            r2 = _safe_float(fit.get("r2"))
            apex_x = _safe_float(fit.get("apex_x"))
            apex_y = _safe_float(fit.get("apex_y"))
        step_status = str(last_step.get("status", "unknown"))
        msg = f"  last step={step_num} status={step_status}"
        if env_rate is not None:
            msg += f" env_rate={env_rate:.6g}"
        if master_run is not None:
            msg += f" master={master_run}"
        if r2 is not None:
            msg += f" r2={r2:.4f}"
        if apex_x is not None and apex_y is not None:
            msg += f" apex=({apex_x:.4g},{apex_y:.4g})"
        print(msg)


def _terminate_process_group(pgid: int, grace_seconds: float = 8.0) -> None:
    if pgid <= 0:
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except Exception:
        return

    deadline = time.time() + max(0.0, float(grace_seconds))
    while time.time() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        except Exception:
            break
        time.sleep(0.2)

    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except Exception:
        return


def _tail_text_file(path: Path, max_lines: int = 80, max_bytes: int = 262144) -> str:
    try:
        data = path.read_bytes()
    except Exception:
        return ""
    if not data:
        return ""
    if len(data) > max_bytes:
        data = data[-max_bytes:]
    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    return "\n".join(lines).strip()


def _recent_simrun_print_log_lines(hub_dir: Path, max_lines: int = 10) -> list[str]:
    csv_path = Path(hub_dir) / "simrun_print_log.csv"
    if not csv_path.exists():
        return []
    tail = _tail_text_file(csv_path, max_lines=max(1, int(max_lines)) + 8, max_bytes=1048576)
    if not tail:
        return []

    raw_lines = [line for line in tail.splitlines() if line.strip()]
    if raw_lines and str(raw_lines[0]).strip().lower() == "timestamp,message":
        raw_lines = raw_lines[1:]
    if len(raw_lines) > max_lines:
        raw_lines = raw_lines[-max_lines:]

    out: list[str] = []
    for line in raw_lines:
        try:
            row = next(csv.reader([line]))
        except Exception:
            out.append(str(line))
            continue
        if len(row) >= 2:
            stamp = str(row[0]).strip()
            message = str(row[1])
            out.append(f"{stamp} {message}".strip())
        elif len(row) == 1:
            out.append(str(row[0]))
    return out


def _can_read_quit_key() -> bool:
    if termios is None or tty is None:
        return False
    try:
        return bool(sys.stdin.isatty())
    except Exception:
        return False


def _poll_quit_key() -> bool:
    if not _can_read_quit_key():
        return False
    try:
        readable, _, _ = select.select([sys.stdin], [], [], 0.0)
        if not readable:
            return False
        ch = sys.stdin.read(1)
        if not ch:
            return False
        return str(ch).lower() == "q"
    except Exception:
        return False


def _format_print_text(args, kwargs) -> str:
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "\n")
    if sep is None:
        sep = " "
    if end is None:
        end = "\n"
    try:
        body = sep.join(str(arg) for arg in args)
    except Exception:
        body = " ".join(repr(arg) for arg in args)
    return f"{body}{end}"


class _HubCsvPrintLog:
    def __init__(self, max_rows: int = 10) -> None:
        self._hub_dir: Path | None = None
        self._csv_path: Path | None = None
        self._rows: list[tuple[str, str]] = []
        self._max_rows = max(1, int(max_rows))
        self._buffer: list[str] = []

    def _load_existing_rows(self, csv_path: Path) -> list[tuple[str, str]]:
        if not csv_path.exists():
            return []
        tail = _tail_text_file(
            csv_path,
            max_lines=self._max_rows + 20,
            max_bytes=2097152,
        )
        if not tail:
            return []
        raw_lines = [line for line in tail.splitlines() if line.strip()]
        if raw_lines and str(raw_lines[0]).strip().lower() == "timestamp,message":
            raw_lines = raw_lines[1:]
        rows: list[tuple[str, str]] = []
        for line in raw_lines:
            try:
                row = next(csv.reader([line]))
            except Exception:
                continue
            if not row:
                continue
            stamp = str(row[0]).strip()
            message = str(row[1]) if len(row) >= 2 else ""
            rows.append((stamp, message))
        if len(rows) > self._max_rows:
            rows = rows[-self._max_rows:]
        return rows

    def _persist_rows(self) -> None:
        if self._csv_path is None:
            return
        if len(self._rows) > self._max_rows:
            self._rows = self._rows[-self._max_rows:]
        try:
            self._csv_path.parent.mkdir(parents=True, exist_ok=True)
            with self._csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["timestamp", "message"])
                for stamp, message in self._rows:
                    writer.writerow([stamp, message])
        except Exception:
            pass

    def attach_hub(self, hub_dir: Path | None) -> None:
        current = str(self._hub_dir) if self._hub_dir is not None else None
        incoming = str(hub_dir) if hub_dir is not None else None
        if current == incoming:
            return
        self.close()
        if hub_dir is None:
            return
        csv_path = Path(hub_dir) / "simrun_print_log.csv"
        try:
            self._hub_dir = Path(hub_dir)
            self._csv_path = csv_path
            self._rows = self._load_existing_rows(csv_path)
            # Enforce truncation as soon as this hub is attached.
            self._persist_rows()
            if self._buffer:
                pending = list(self._buffer)
                self._buffer.clear()
                for message in pending:
                    self.write(message)
        except Exception:
            self._hub_dir = None
            self._csv_path = None
            self._rows = []

    def write(self, text: str) -> None:
        if self._csv_path is None:
            self._buffer.append(str(text))
            if len(self._buffer) > 50000:
                self._buffer = self._buffer[-50000:]
            return
        message = str(text).replace("\r\n", "\n").replace("\r", "\n")
        lines = message.split("\n")
        if lines and lines[-1] == "":
            lines = lines[:-1]
        if not lines:
            lines = [""]
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        try:
            for line in lines:
                self._rows.append((stamp, line))
            if len(self._rows) > self._max_rows:
                self._rows = self._rows[-self._max_rows:]
            self._persist_rows()
        except Exception:
            pass

    def close(self) -> None:
        self._hub_dir = None
        self._csv_path = None
        self._rows = []


def _make_tee_print(base_print, log_handle, hub_csv_log: _HubCsvPrintLog | None = None):
    def _tee_print(*args, **kwargs):
        skip_hub_csv = bool(kwargs.pop("_skip_hub_csv", False))
        base_print(*args, **kwargs)
        if log_handle is None:
            pass
        else:
            log_kwargs = dict(kwargs)
            log_kwargs["file"] = log_handle
            log_kwargs["flush"] = True
            base_print(*args, **log_kwargs)

        if hub_csv_log is None or skip_hub_csv:
            return
        target_file = kwargs.get("file", None)
        if target_file not in (None, sys.stdout, sys.stderr):
            return
        hub_csv_log.write(_format_print_text(args, kwargs))

    return _tee_print


def main() -> None:
    args = _parse_args()
    results_root = Path(args.results_root)
    results_root.mkdir(parents=True, exist_ok=True)
    run_logs_dir = results_root / "run_logs"
    run_logs_dir.mkdir(parents=True, exist_ok=True)
    hub_csv_log = _HubCsvPrintLog(max_rows=10)

    original_print = print
    session_log_handle = None
    session_log_path = None
    try:
        if args.session_log:
            session_log_path = Path(str(args.session_log)).expanduser()
            if not session_log_path.is_absolute():
                session_log_path = (Path.cwd() / session_log_path).resolve()
        else:
            session_log_path = run_logs_dir / f"simrun_{os.getpid()}_{int(time.time() * 1000)}.log"
        session_log_path.parent.mkdir(parents=True, exist_ok=True)
        session_log_handle = session_log_path.open("a", encoding="utf-8")
        globals()["print"] = _make_tee_print(original_print, session_log_handle, hub_csv_log)
        try:
            (run_logs_dir / "latest_simrun_log.txt").write_text(str(session_log_path))
        except Exception:
            pass
        print(f"simRun session log: {session_log_path}")
    except Exception as exc:
        globals()["print"] = original_print
        original_print(f"[WARN] failed to start simRun session log: {exc}")

    before = _discover_hub_dirs(results_root)
    before_keys = set()
    for path in before:
        try:
            before_keys.add(str(path.resolve()))
        except Exception:
            before_keys.add(str(path))

    repo_root = Path(__file__).resolve().parent
    hub_runner_path = repo_root / "hub_runner.py"
    if not hub_runner_path.exists():
        raise SystemExit(f"Missing hub_runner.py at {hub_runner_path}")

    cmd = [
        sys.executable,
        str(hub_runner_path),
        "--results-root",
        str(results_root),
        "--script",
        str(args.script),
        "--no-screen",
        "--screen-hold-seconds",
        "0",
    ]
    if args.count is not None:
        cmd.extend(["--count", str(int(args.count))])
    if args.start_rate is not None:
        cmd.extend(["--start-rate", str(float(args.start_rate))])
    if args.end_rate is not None:
        cmd.extend(["--end-rate", str(float(args.end_rate))])
    if args.step is not None:
        cmd.extend(["--step", str(float(args.step))])
    if args.species_threshold is not None:
        cmd.extend(["--species-threshold", str(int(args.species_threshold))])
    if args.max_masters is not None:
        cmd.extend(["--max-masters", str(int(args.max_masters))])
    if bool(args.skip_plots):
        cmd.append("--skip-plots")

    print(f"Starting new hub run: {' '.join(cmd)}")
    shutdown_signal_path = (
        Path(tempfile.gettempdir())
        / f"hub_runner_shutdown_{os.getpid()}_{int(time.time() * 1000)}.signal"
    )
    proc_env = os.environ.copy()
    proc_env["HUB_RUNNER_SHUTDOWN_FILE"] = str(shutdown_signal_path)
    # Mark launches originating from simRun so hub_runner can keep master UI headless.
    proc_env["HUB_RUNNER_FROM_SIMRUN"] = "1"
    runner_log_path: Path | None = None
    runner_log_handle = None
    if bool(args.show_runner_output):
        runner_stdout = None
        runner_stderr = None
    else:
        runner_log_path = run_logs_dir / f"hub_runner_{os.getpid()}_{int(time.time() * 1000)}.log"
        runner_log_handle = runner_log_path.open("wb")
        runner_stdout = runner_log_handle
        runner_stderr = subprocess.STDOUT
        print(f"hub_runner raw log: {runner_log_path}")
    proc = subprocess.Popen(
        cmd,
        cwd=str(repo_root),
        stdout=runner_stdout,
        stderr=runner_stderr,
        env=proc_env,
        start_new_session=True,
    )
    pgid = int(proc.pid)

    hub_dir: Path | None = None
    discovered_once = False
    recent_history_printed = False
    recent_history_cache: list[str] | None = None
    interval = max(1, int(args.print_every_seconds))
    next_print = time.time() + interval
    quit_requested = False
    quit_requested_at = None
    waiting_for_hub_meta_notified = False

    def _print_recent_history_if_available() -> None:
        nonlocal recent_history_printed, recent_history_cache
        if recent_history_printed or hub_dir is None:
            return
        recent_history_printed = True
        if recent_history_cache is None:
            recent_history_cache = _recent_simrun_print_log_lines(hub_dir, max_lines=10)
        recent = recent_history_cache
        recent_history_cache = None
        if not recent:
            return
        print(
            f"Recent simrun_print_log history (last {len(recent)} lines):",
            _skip_hub_csv=True,
        )
        for line in recent:
            print(f"  {line}", _skip_hub_csv=True)

    use_keypoll = _can_read_quit_key()
    stdin_fd = None
    stdin_attrs = None
    if use_keypoll:
        try:
            stdin_fd = sys.stdin.fileno()
            stdin_attrs = termios.tcgetattr(stdin_fd)
            tty.setcbreak(stdin_fd)
            print("Press q to gracefully stop this hub run.")
        except Exception:
            use_keypoll = False
            stdin_fd = None
            stdin_attrs = None

    try:
        while proc.poll() is None:
            if use_keypoll and _poll_quit_key() and (not quit_requested):
                quit_requested = True
                quit_requested_at = float(time.time())
                try:
                    shutdown_signal_path.write_text(str(time.time()))
                    print(
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                        "q pressed: requested graceful hub shutdown"
                    )
                except Exception:
                    print(
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                        "q pressed: failed to write graceful shutdown signal; forcing stop soon"
                    )

            if quit_requested and quit_requested_at is not None:
                if (time.time() - float(quit_requested_at)) > 180.0:
                    print(
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                        "graceful shutdown timeout reached; forcing process-group stop"
                    )
                    _terminate_process_group(pgid)
                    break

            if hub_dir is None:
                current = _discover_hub_dirs(results_root)
                new_candidates = []
                for path in current:
                    try:
                        key = str(path.resolve())
                    except Exception:
                        key = str(path)
                    if key not in before_keys:
                        new_candidates.append(path)
                if new_candidates:
                    new_candidates.sort(key=lambda p: (_hub_index(p), str(p)))
                    hub_dir = new_candidates[-1]
                    recent_history_cache = _recent_simrun_print_log_lines(hub_dir, max_lines=10)
                    hub_csv_log.attach_hub(hub_dir)
                    _print_recent_history_if_available()
                    print(f"New hub created: {hub_dir}")

            now = time.time()
            if now >= next_print:
                if hub_dir is not None:
                    summary = _snapshot_summary(hub_dir)
                    if isinstance(summary, dict):
                        _print_snapshot(summary, final=False)
                        discovered_once = True
                        waiting_for_hub_meta_notified = False
                    else:
                        if not waiting_for_hub_meta_notified:
                            print(
                                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [UPDATE] "
                                "waiting for hub metadata (hub is being initialized)."
                            )
                            waiting_for_hub_meta_notified = True
                else:
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [UPDATE] waiting for new hub directory...")
                next_print = now + interval

            time.sleep(1.0)

        returncode = int(proc.returncode if proc.returncode is not None else 1)

        if hub_dir is None:
            current = _discover_hub_dirs(results_root)
            new_candidates = []
            for path in current:
                try:
                    key = str(path.resolve())
                except Exception:
                    key = str(path)
                if key not in before_keys:
                    new_candidates.append(path)
            if new_candidates:
                new_candidates.sort(key=lambda p: (_hub_index(p), str(p)))
                hub_dir = new_candidates[-1]

        if hub_dir is not None:
            recent_history_cache = _recent_simrun_print_log_lines(hub_dir, max_lines=10)
            hub_csv_log.attach_hub(hub_dir)
            _print_recent_history_if_available()
            summary = _snapshot_summary(hub_dir)
            if isinstance(summary, dict):
                if not discovered_once:
                    _print_snapshot(summary, final=False)
                _print_snapshot(summary, final=True)
            print(f"Hub directory: {hub_dir}")
        else:
            print("Hub directory could not be determined.")

        _terminate_process_group(pgid)
        if returncode != 0:
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                f"hub_runner exited with code {returncode}."
            )
            if runner_log_path is not None:
                tail = _tail_text_file(runner_log_path, max_lines=80)
                if tail:
                    print("Last hub_runner output:")
                    print(tail)
                print(f"hub_runner log: {runner_log_path}")
            raise SystemExit(returncode)
    finally:
        if runner_log_handle is not None:
            try:
                runner_log_handle.close()
            except Exception:
                pass
        if stdin_fd is not None and stdin_attrs is not None:
            try:
                termios.tcsetattr(stdin_fd, termios.TCSADRAIN, stdin_attrs)
            except Exception:
                pass
        try:
            shutdown_signal_path.unlink(missing_ok=True)
        except Exception:
            pass
        _terminate_process_group(pgid)
        if session_log_handle is not None:
            try:
                session_log_handle.flush()
                session_log_handle.close()
            except Exception:
                pass
        hub_csv_log.close()
        globals()["print"] = original_print


if __name__ == "__main__":
    main()
