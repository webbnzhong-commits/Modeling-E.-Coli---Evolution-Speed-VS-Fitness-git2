#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
        "--print-every-seconds",
        type=int,
        default=10,
        help="Progress print interval in seconds (default: 10)",
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
    try:
        payload = json.loads(hub_meta_path.read_text())
    except Exception:
        return None
    if isinstance(payload, dict):
        return payload
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

    return {
        "master_run_num": master_run_num,
        "run_count": int(len(run_dirs)),
        "avg_interval_1000": avg_interval,
        "avg_fps": avg_fps,
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
    for step in steps:
        if not isinstance(step, dict):
            continue
        status = str(step.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1
        if status in TERMINAL_STEP_STATUSES:
            done += 1

    created_at = _safe_float(meta.get("created_at"))
    elapsed = time.time() - created_at if created_at is not None else 0.0
    status = str(meta.get("status", "unknown"))
    next_step_idx = done + 1 if (status == "running" and done < total) else None
    current_env_rate = None
    if next_step_idx is not None and isinstance(rates, list) and (0 <= (next_step_idx - 1) < len(rates)):
        current_env_rate = _safe_float(rates[next_step_idx - 1])

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
        "counts": counts,
        "elapsed_s": float(elapsed),
        "next_step_idx": next_step_idx,
        "current_env_rate": current_env_rate,
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

    print(
        f"[{ts}] [{phase}] hub_{summary.get('hub_idx')} "
        f"status={summary.get('status')} "
        f"progress={summary.get('done')}/{summary.get('total')} "
        f"ok={ok} failed={failed} stopped={stopped} no_master={no_master} aborted={aborted} "
        f"elapsed={_format_duration(_safe_float(summary.get('elapsed_s')) or 0.0)}"
    )

    next_idx = summary.get("next_step_idx")
    current_rate = summary.get("current_env_rate")
    if isinstance(next_idx, int) and _safe_float(current_rate) is not None:
        print(f"  running step={next_idx} env_rate={float(current_rate):.6g}")
        running_metrics = _running_master_metrics(summary)
        if isinstance(running_metrics, dict):
            master_num = running_metrics.get("master_run_num")
            run_count = _safe_int(running_metrics.get("run_count"), 0)
            avg_interval = _safe_float(running_metrics.get("avg_interval_1000"))
            avg_fps = _safe_float(running_metrics.get("avg_fps"))
            total_species = _safe_float(running_metrics.get("total_species"))
            max_species = _safe_float(running_metrics.get("max_species"))
            max_frames = _safe_float(running_metrics.get("max_frames"))
            species_text = f"{total_species:.1f}" if total_species is not None else "--"
            species_max_text = f"{max_species:.1f}" if max_species is not None else "--"
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


def main() -> None:
    args = _parse_args()
    results_root = Path(args.results_root)
    results_root.mkdir(parents=True, exist_ok=True)

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
    proc = subprocess.Popen(
        cmd,
        cwd=str(repo_root),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=proc_env,
        start_new_session=True,
    )
    pgid = int(proc.pid)

    hub_dir: Path | None = None
    discovered_once = False
    interval = max(1, int(args.print_every_seconds))
    next_print = time.time() + interval
    quit_requested = False
    quit_requested_at = None

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
                    print(f"New hub created: {hub_dir}")

            now = time.time()
            if now >= next_print:
                if hub_dir is not None:
                    summary = _snapshot_summary(hub_dir)
                    if isinstance(summary, dict):
                        _print_snapshot(summary, final=False)
                        discovered_once = True
                    else:
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [UPDATE] waiting for hub metadata...")
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
            raise SystemExit(returncode)
    finally:
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


if __name__ == "__main__":
    main()
