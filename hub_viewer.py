#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import queue
import shutil
import subprocess
import threading
import time
from pathlib import Path

import hub_loading_pool as hlp
import hub_runner as hr
from settings_manager import load_settings


_SIM_COLORS = [
    (82, 205, 255),
    (255, 174, 92),
    (162, 234, 122),
    (255, 122, 167),
    (210, 162, 255),
    (255, 224, 120),
    (130, 200, 255),
    (255, 136, 136),
]

_TIMELINE_MAX_SNAPSHOTS_PER_RUN = 24
_BACKGROUND_MODEL_UPDATE_EVERY_ROWS = 12
_HUB_LOADING_WORKERS = 6


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only hub viewer with sim list, hub graph, and normalized timeline."
    )
    parser.add_argument(
        "--results-root",
        default="results",
        help="Root folder containing hub_* directories (default: results).",
    )
    parser.add_argument(
        "--hub-index",
        type=int,
        default=None,
        help="Open a specific hub index (e.g. --hub-index 5).",
    )
    parser.add_argument(
        "--hub-dir",
        default=None,
        help="Open a specific hub directory path.",
    )
    parser.add_argument(
        "--no-selector",
        action="store_true",
        help="Skip selector UI when no hub is specified and use latest discovered hub.",
    )
    return parser.parse_args()


def _parse_rate_label(path_name: str) -> float | None:
    if not path_name.startswith("env_"):
        return None
    raw = path_name[4:].replace("p", ".")
    try:
        return float(raw)
    except Exception:
        return None


def _safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _apex_from_points(points: list[tuple[float, float]]) -> tuple[float | None, float | None]:
    if not points:
        return (None, None)
    try:
        x_val, y_val = max(points, key=lambda p: p[1])
    except Exception:
        return (None, None)
    if not (hr._is_number(x_val) and hr._is_number(y_val)):
        return (None, None)
    return (float(x_val), float(y_val))


def _sample_paths_evenly(paths: list[Path], limit: int) -> list[Path]:
    if limit <= 0 or len(paths) <= max(1, int(limit)):
        return list(paths)
    if len(paths) <= 2:
        return list(paths)
    limit = max(2, int(limit))
    span = float(len(paths) - 1)
    step = span / float(limit - 1)
    sampled = []
    for idx in range(limit):
        src_idx = int(round(float(idx) * step))
        src_idx = max(0, min(len(paths) - 1, src_idx))
        sampled.append(paths[src_idx])
    deduped = []
    seen = set()
    for path in sampled:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _resolve_hub_dir(args: argparse.Namespace, results_root: Path) -> tuple[int | None, Path]:
    if args.hub_dir:
        path = Path(args.hub_dir).expanduser().resolve()
        if not path.is_dir():
            raise SystemExit(f"Hub directory not found: {path}")
        idx = hr._parse_hub_id(path)
        return idx, path

    if args.hub_index is not None:
        idx = max(0, int(args.hub_index))
        path = hr._hub_container_dir(results_root) / f"hub_{idx}"
        if (not path.is_dir()) and (results_root / f"hub_{idx}").is_dir():
            path = results_root / f"hub_{idx}"
        if not path.is_dir():
            raise SystemExit(f"Hub directory not found: {path}")
        return idx, path

    if not args.no_selector:
        choice = hr._select_hub_run_ui(results_root)
        if isinstance(choice, dict) and choice.get("mode") == "continue":
            idx = _safe_int(choice.get("hub_idx"))
            path = choice.get("hub_dir")
            if idx is not None and isinstance(path, Path) and path.is_dir():
                return idx, path

    hubs = hr._collect_hub_runs(results_root)
    if not hubs:
        raise SystemExit(f"No hub runs found in: {results_root}")
    latest = hubs[-1]
    idx = _safe_int(latest.get("hub_idx"))
    path = latest.get("hub_dir")
    if not isinstance(path, Path):
        raise SystemExit("Failed to resolve latest hub path.")
    return idx, path


def _load_hub_meta(hub_dir: Path) -> dict:
    meta_path = hub_dir / "hub_meta.json"
    if not meta_path.exists():
        return {}
    try:
        payload = json.loads(meta_path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rates_from_meta(hub_dir: Path, hub_meta: dict) -> list[float]:
    rates = []
    raw_rates = hub_meta.get("rates")
    if isinstance(raw_rates, list):
        for value in raw_rates:
            if hr._is_number(value):
                rates.append(float(value))
    if rates:
        return rates

    raw_steps = hub_meta.get("steps")
    if isinstance(raw_steps, list):
        candidate = []
        for step in raw_steps:
            if not isinstance(step, dict):
                continue
            env_rate = step.get("env_rate")
            if hr._is_number(env_rate):
                candidate.append(float(env_rate))
        if candidate:
            candidate = sorted(set(candidate))
            return candidate

    from_dirs = []
    for path in sorted(hub_dir.glob("env_*")):
        if not path.is_dir():
            continue
        rate = _parse_rate_label(path.name)
        if hr._is_number(rate):
            from_dirs.append(float(rate))
    return sorted(set(from_dirs))


def _increment_suffixes(increment_mode: str) -> list[str]:
    if str(increment_mode) == "step0p01":
        return ["_step0p01", ""]
    return [""]


def _master_parsed_arithmetic_points(
    master_dir: Path,
    run_nums: list[int],
    increment_mode: str = "default",
) -> list[tuple[float, float]]:
    master_name = master_dir.name
    for suffix in _increment_suffixes(increment_mode):
        candidates = [
            master_dir / f"parsedArithmeticMeanSimulatino{master_name}{suffix}_Log.csv",
            master_dir / f"parsedArithmeticMeanSimulationo{master_name}{suffix}_Log.csv",
            master_dir / f"parsedArithmeticMeanSimulation{master_name}{suffix}_Log.csv",
            master_dir / f"parsedArithmeticMeanSimulatin{master_name}{suffix}_Log.csv",
            master_dir / f"parsedArithmeticMeanSimulatino{master_name}{suffix}_log.csv",
            master_dir / f"parsedArithmeticMeanSimulationo{master_name}{suffix}_log.csv",
            master_dir / f"parsedArithmeticMeanSimulation{master_name}{suffix}_log.csv",
            master_dir / f"parsedArithmeticMeanSimulatin{master_name}{suffix}_log.csv",
        ]
        for path in candidates:
            points = hr._extract_points_from_csv(path)
            if points:
                return points
        if suffix:
            for candidate in sorted(
                master_dir.glob(f"parsedArithmeticMean*{master_name}*{suffix}*.csv")
            ):
                points = hr._extract_points_from_csv(candidate)
                if points:
                    return points
        else:
            for candidate in sorted(master_dir.glob(f"parsedArithmeticMean*{master_name}_*.csv")):
                if "_step" in candidate.name.lower():
                    continue
                points = hr._extract_points_from_csv(candidate)
                if points:
                    return points

    for suffix in _increment_suffixes(increment_mode):
        combined_points = hr._extract_points_from_csv(
            master_dir / f"combinedArithmeticMeanSimulatino{master_name}{suffix}_Log.csv"
        )
        if combined_points:
            return combined_points

    merged = []
    for run_num in run_nums:
        run_dir = master_dir.parent / str(run_num)
        merged.extend(
            _run_parsed_arithmetic_points(
                run_dir,
                int(run_num),
                increment_mode=increment_mode,
            )
        )
    return merged


def _refresh_row_from_disk(
    row: dict,
    recompute_fit: bool = True,
    increment_mode: str = "default",
) -> None:
    row["_full_loaded"] = False
    env_dir_raw = row.get("env_dir")
    if not env_dir_raw:
        row["_full_loaded"] = True
        return
    env_dir = Path(str(env_dir_raw))
    if not env_dir.is_dir():
        row["_full_loaded"] = True
        return

    master_dir = hr._latest_master_dir(env_dir)
    run_nums = hr._master_run_nums(master_dir) if master_dir is not None else []
    if not run_nums:
        run_nums = hr._discover_env_run_nums(env_dir)
    if master_dir is None and not run_nums:
        row["_full_loaded"] = True
        return

    max_species = hr._max_species(env_dir, run_nums)
    total_species = hr._total_species(env_dir, run_nums)
    max_frames = hr._max_frames(env_dir, run_nums)
    max_elapsed = hr._max_elapsed_seconds(env_dir, run_nums)

    if master_dir is not None:
        try:
            master_run_num = int(master_dir.name.split("_", 1)[1])
        except Exception:
            master_run_num = None
        points = _master_parsed_arithmetic_points(
            master_dir,
            run_nums,
            increment_mode=increment_mode,
        )
        row["master_dir"] = str(master_dir)
    else:
        master_run_num = None
        points = []
        for run_num in run_nums:
            points.extend(
                _run_parsed_arithmetic_points(
                    env_dir / str(run_num),
                    int(run_num),
                    increment_mode=increment_mode,
                )
            )

    if not points:
        points = hr._snapshot_points_from_runs(env_dir, run_nums)

    fit = None
    apex_x = None
    apex_y = None
    if (not recompute_fit) and isinstance(row.get("fit"), dict):
        fit = row.get("fit")
        if isinstance(fit, dict):
            apex_x = fit.get("apex_x")
            apex_y = fit.get("apex_y")
    if not (hr._is_number(apex_x) and hr._is_number(apex_y)):
        if hr._is_number(row.get("apex_evolution_rate")) and hr._is_number(row.get("apex_fitness")):
            apex_x = row.get("apex_evolution_rate")
            apex_y = row.get("apex_fitness")
    if recompute_fit or (not isinstance(fit, dict)):
        fit = hr._fit_stitched_gaussian(points)
        if isinstance(fit, dict):
            apex_x = fit.get("apex_x")
            apex_y = fit.get("apex_y")
        elif not (hr._is_number(apex_x) and hr._is_number(apex_y)):
            apex_x, apex_y = _apex_from_points(points)

    if master_run_num is not None:
        row["master_run_num"] = int(master_run_num)
    row["run_nums"] = list(run_nums)
    row["max_species"] = max_species
    row["total_species"] = total_species
    row["max_frames"] = max_frames
    row["duration_s"] = float(max_elapsed) if hr._is_number(max_elapsed) else row.get("duration_s")
    row["points"] = points
    row["point_count"] = int(len(points))
    row["fit"] = fit if isinstance(fit, dict) else None
    row["apex_evolution_rate"] = float(apex_x) if hr._is_number(apex_x) else None
    row["apex_fitness"] = float(apex_y) if hr._is_number(apex_y) else None
    row["_full_loaded"] = True


def _build_rows(
    hub_dir: Path,
    hub_meta: dict,
    refresh_disk: bool = True,
    increment_mode: str = "default",
) -> list[dict]:
    rates = _rates_from_meta(hub_dir, hub_meta)
    planned = hub_meta.get("planned_master_ids")
    planned_ids = planned if isinstance(planned, list) else []

    step_info_by_idx = {}
    raw_steps = hub_meta.get("steps")
    if isinstance(raw_steps, list):
        for step in raw_steps:
            if not isinstance(step, dict):
                continue
            step_idx = _safe_int(step.get("step_index"))
            if step_idx is not None:
                step_info_by_idx[step_idx] = step

    rows = []
    for idx, rate in enumerate(rates):
        env_dir = hub_dir / hr._rate_label(float(rate))
        step_info = step_info_by_idx.get(int(idx), {})
        status = str(step_info.get("status", "pending"))
        apex_fitness = _safe_float(step_info.get("apex_fitness")) if isinstance(step_info, dict) else None
        apex_evo = _safe_float(step_info.get("apex_evolution_rate")) if isinstance(step_info, dict) else None
        preview_points = []
        if hr._is_number(apex_evo) and hr._is_number(apex_fitness):
            preview_points = [(float(apex_evo), float(apex_fitness))]
        row = {
            "step_index": int(idx),
            "env_rate": float(rate),
            "planned_master_run_num": (
                _safe_int(step_info.get("planned_master_run_num"))
                if isinstance(step_info, dict) and step_info.get("planned_master_run_num") is not None
                else (
                    _safe_int(planned_ids[idx])
                    if idx < len(planned_ids)
                    else None
                )
            ),
            "master_run_num": _safe_int(step_info.get("master_run_num")) if isinstance(step_info, dict) else None,
            "status": status,
            "max_species": _safe_float(step_info.get("max_species")) if isinstance(step_info, dict) else None,
            "total_species": _safe_float(step_info.get("total_species")) if isinstance(step_info, dict) else None,
            "max_frames": _safe_float(step_info.get("max_frames")) if isinstance(step_info, dict) else None,
            "apex_fitness": apex_fitness,
            "apex_evolution_rate": apex_evo,
            "duration_s": _safe_float(step_info.get("duration_s")) if isinstance(step_info, dict) else None,
            "env_dir": str(env_dir),
            "master_dir": step_info.get("master_dir") if isinstance(step_info, dict) else None,
            "fit": step_info.get("fit") if isinstance(step_info.get("fit"), dict) else None,
            "points": preview_points,
            "run_nums": step_info.get("run_nums") if isinstance(step_info.get("run_nums"), list) else [],
            "_full_loaded": bool(refresh_disk),
        }
        if refresh_disk:
            _refresh_row_from_disk(
                row,
                increment_mode=increment_mode,
            )
        if row.get("master_run_num") is not None and row.get("status") in ("pending", "running", ""):
            row["status"] = "ok"
        rows.append(row)
    return rows


def _compute_hub_graph_points(rows: list[dict], include_apex_fallback: bool = False) -> list[dict]:
    graph_points = []
    for row_idx, row in enumerate(rows):
        env_rate = row.get("env_rate")
        row_loaded = bool(row.get("_full_loaded"))
        points = row.get("points")
        if (not hr._is_number(env_rate)) or (not isinstance(points, list)) or (not row_loaded):
            points = []
        for src_idx, pair in enumerate(points, start=1):
            if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                continue
            evo_val = pair[0]
            fit_val = pair[1]
            if not (hr._is_number(evo_val) and hr._is_number(fit_val)):
                continue
            graph_points.append(
                {
                    "row_index": int(row_idx),
                    "x": float(env_rate),
                    "y": float(evo_val),
                    "fitness": float(fit_val),
                    "source_row_index": int(src_idx),
                }
            )
        if points:
            continue
        if (not include_apex_fallback) or (not row_loaded):
            continue
        apex_x = row.get("apex_evolution_rate")
        apex_y = row.get("apex_fitness")
        if not (hr._is_number(apex_x) and hr._is_number(apex_y) and hr._is_number(env_rate)):
            continue
        graph_points.append(
            {
                "row_index": int(row_idx),
                "x": float(env_rate),
                "y": float(apex_x),
                "fitness": float(apex_y),
                "source_row_index": 0,
            }
        )
    return graph_points


def _write_hub_fit_equations_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "enviorment change rate",
                "master run",
                "apex x",
                "apex y",
                "sigma left",
                "sigma right",
                "r2",
                "equation",
            ]
        )
        ordered_rows = sorted(
            [row for row in rows if isinstance(row, dict)],
            key=lambda row: (_safe_int(row.get("step_index")) if _safe_int(row.get("step_index")) is not None else 10**9),
        )
        for row in ordered_rows:
            fit = row.get("fit")
            if not isinstance(fit, dict):
                continue
            env_rate = row.get("env_rate")
            if not hr._is_number(env_rate):
                continue
            master_run_num = _safe_int(row.get("master_run_num"))
            writer.writerow(
                [
                    float(env_rate),
                    (master_run_num if master_run_num is not None else ""),
                    (fit.get("apex_x") if hr._is_number(fit.get("apex_x")) else ""),
                    (fit.get("apex_y") if hr._is_number(fit.get("apex_y")) else ""),
                    (fit.get("sigma_left") if hr._is_number(fit.get("sigma_left")) else ""),
                    (fit.get("sigma_right") if hr._is_number(fit.get("sigma_right")) else ""),
                    (fit.get("r2") if hr._is_number(fit.get("r2")) else ""),
                    str(fit.get("equation", "")),
                ]
            )


def _sync_hub_meta_steps_from_rows(hub_meta: dict, rows: list[dict]) -> bool:
    if not isinstance(hub_meta, dict):
        return False
    steps = hub_meta.get("steps")
    if not isinstance(steps, list):
        return False

    step_by_idx = {}
    for step in steps:
        if not isinstance(step, dict):
            continue
        step_idx = _safe_int(step.get("step_index"))
        if step_idx is None:
            continue
        step_by_idx[int(step_idx)] = step

    changed = False
    for row in rows:
        if not isinstance(row, dict):
            continue
        step_idx = _safe_int(row.get("step_index"))
        if step_idx is None:
            continue
        step = step_by_idx.get(int(step_idx))
        if not isinstance(step, dict):
            continue

        fit_val = row.get("fit") if isinstance(row.get("fit"), dict) else None
        updates = {
            "fit": fit_val,
            "master_run_num": _safe_int(row.get("master_run_num")),
            "master_dir": (
                str(row.get("master_dir"))
                if isinstance(row.get("master_dir"), (str, Path)) and str(row.get("master_dir")).strip()
                else None
            ),
            "run_nums": (list(row.get("run_nums")) if isinstance(row.get("run_nums"), list) else []),
            "max_species": (_safe_float(row.get("max_species"))),
            "total_species": (_safe_float(row.get("total_species"))),
            "max_frames": (_safe_float(row.get("max_frames"))),
            "apex_evolution_rate": (_safe_float(row.get("apex_evolution_rate"))),
            "apex_fitness": (_safe_float(row.get("apex_fitness"))),
            "duration_s": (_safe_float(row.get("duration_s"))),
            "point_count": (
                int(row.get("point_count"))
                if isinstance(row.get("point_count"), int)
                else (
                    len(row.get("points"))
                    if isinstance(row.get("points"), list)
                    else 0
                )
            ),
        }
        for key, value in updates.items():
            if step.get(key) != value:
                step[key] = value
                changed = True

    if changed:
        hub_meta["updated_at"] = float(time.time())
    return changed


def _timeline_fit(points: list[tuple[float, float]]) -> dict | None:
    if len(points) < 2:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    model = hr._linear_fit(xs, ys)
    if model is None:
        return None
    slope, intercept = model
    residual = 0.0
    mean_y = sum(ys) / len(ys)
    total_var = 0.0
    for xv, yv in points:
        pred = (float(slope) * float(xv)) + float(intercept)
        residual += (float(yv) - pred) ** 2
        total_var += (float(yv) - mean_y) ** 2
    r2 = None
    if total_var > 1e-12:
        r2 = 1.0 - (residual / total_var)
    sign = "-" if float(intercept) < 0 else "+"
    equation = f"y = {float(slope):.5g}x {sign} {abs(float(intercept)):.5g}"
    return {
        "slope": float(slope),
        "intercept": float(intercept),
        "r2": float(r2) if hr._is_number(r2) else None,
        "equation": equation,
    }


def _timeline_master_average(series: list[dict], bins: int = 101) -> list[tuple[float, float]]:
    if not isinstance(series, list) or not series:
        return []
    bins = max(3, int(bins))
    grid = [float(i) / float(bins - 1) for i in range(bins)]
    per_bin_values = [[] for _ in range(bins)]

    for item in series:
        if not isinstance(item, dict):
            continue
        raw_pts = item.get("points")
        if not isinstance(raw_pts, list) or not raw_pts:
            continue

        by_x = {}
        for p in raw_pts:
            if not isinstance(p, (tuple, list)) or len(p) < 2:
                continue
            xv, yv = p[0], p[1]
            if not (hr._is_number(xv) and hr._is_number(yv)):
                continue
            x_float = max(0.0, min(1.0, float(xv)))
            by_x[x_float] = float(yv)
        if not by_x:
            continue
        pts = sorted(by_x.items(), key=lambda pair: pair[0])

        seg = 0
        for idx, gx in enumerate(grid):
            if gx <= pts[0][0]:
                y_interp = pts[0][1]
            elif gx >= pts[-1][0]:
                y_interp = pts[-1][1]
            else:
                while (seg + 1) < len(pts) and pts[seg + 1][0] < gx:
                    seg += 1
                x0, y0 = pts[seg]
                x1, y1 = pts[seg + 1]
                if x1 <= x0:
                    y_interp = y0
                else:
                    t = (gx - x0) / (x1 - x0)
                    y_interp = y0 + (t * (y1 - y0))
            per_bin_values[idx].append(float(y_interp))

    out = []
    for idx, values in enumerate(per_bin_values):
        if not values:
            continue
        out.append((grid[idx], float(sum(values) / len(values))))
    return out


def _timeline_interp_y(points: list[tuple[float, float]], x_norm: float) -> float | None:
    if not isinstance(points, list) or not points:
        return None
    x_norm = max(0.0, min(1.0, float(x_norm)))
    cleaned = []
    for pair in points:
        if not isinstance(pair, (tuple, list)) or len(pair) < 2:
            continue
        xv, yv = pair[0], pair[1]
        if hr._is_number(xv) and hr._is_number(yv):
            cleaned.append((float(xv), float(yv)))
    if not cleaned:
        return None
    cleaned.sort(key=lambda p: p[0])
    if x_norm <= cleaned[0][0]:
        return float(cleaned[0][1])
    if x_norm >= cleaned[-1][0]:
        return float(cleaned[-1][1])
    for idx in range(1, len(cleaned)):
        x0, y0 = cleaned[idx - 1]
        x1, y1 = cleaned[idx]
        if x_norm <= x1:
            if x1 <= x0:
                return float(y0)
            t = (x_norm - x0) / (x1 - x0)
            return float(y0 + (t * (y1 - y0)))
    return float(cleaned[-1][1])


def _run_parsed_arithmetic_points(
    run_dir: Path,
    run_num: int,
    increment_mode: str = "default",
) -> list[tuple[float, float]]:
    for suffix in _increment_suffixes(increment_mode):
        candidates = [
            run_dir / f"parsedArithmeticMeanSimulatino{run_num}{suffix}_Log.csv",
            run_dir / f"parsedArithmeticMeanSimulationo{run_num}{suffix}_Log.csv",
            run_dir / f"parsedArithmeticMeanSimulation{run_num}{suffix}_Log.csv",
            run_dir / f"parsedArithmeticMeanSimulatin{run_num}{suffix}_Log.csv",
            run_dir / f"parsedArithmeticMeanSimulatino{run_num}{suffix}_log.csv",
            run_dir / f"parsedArithmeticMeanSimulationo{run_num}{suffix}_log.csv",
            run_dir / f"parsedArithmeticMeanSimulation{run_num}{suffix}_log.csv",
            run_dir / f"parsedArithmeticMeanSimulatin{run_num}{suffix}_log.csv",
        ]
        for path in candidates:
            points = hr._extract_points_from_csv(path)
            if points:
                return points
        if suffix:
            for candidate in sorted(run_dir.glob(f"parsedArithmeticMean*{run_num}*{suffix}*.csv")):
                points = hr._extract_points_from_csv(candidate)
                if points:
                    return points
        else:
            for candidate in sorted(run_dir.glob(f"parsedArithmeticMean*{run_num}_*.csv")):
                if "_step" in candidate.name.lower():
                    continue
                points = hr._extract_points_from_csv(candidate)
                if points:
                    return points
    return []


class HubViewer:
    def __init__(self, hub_idx: int | None, hub_dir: Path, hub_meta: dict, results_root: Path) -> None:
        import pygame

        self.pg = pygame
        pygame.init()
        self.window_w = 1360
        self.window_h = 820
        self.screen = pygame.display.set_mode((self.window_w, self.window_h))
        label = f"Hub Viewer - hub_{hub_idx}" if hub_idx is not None else f"Hub Viewer - {hub_dir.name}"
        pygame.display.set_caption(label)

        self.font = pygame.font.SysFont("Consolas", 19)
        self.small = pygame.font.SysFont("Consolas", 15)
        self.tiny = pygame.font.SysFont("Consolas", 12)
        self.clock = pygame.time.Clock()

        self.hub_idx = hub_idx
        self.hub_dir = hub_dir
        self.hub_meta = hub_meta
        self.results_root = results_root

        self.rows = []
        self.graph_points = []
        self.hub_fit_report = {}
        self.hub_best_fit = None
        self.hub_dot_hits = []
        self.table_row_hits = []
        self._table_rect = None
        self._table_scrollbar_track_rect = None
        self._table_scrollbar_thumb_rect = None
        self._table_scrollbar_dragging = False
        self._table_scrollbar_drag_offset = 0
        self.selected_row_index = None
        self.table_scroll = 0.0
        self.table_row_h = 24
        self.timeline_cache = {}
        self._loading_pool = hlp.HubLoadingPool(workers=_HUB_LOADING_WORKERS)
        self._loader_thread = None
        self._loader_stop_event = threading.Event()
        self._loader_updates = queue.SimpleQueue()
        self._loader_token = 0
        self._loader_active = False
        self._loader_total_steps = 1
        self._loader_done_steps = 1
        self._loader_status = ""
        self._loader_error = None
        self._loader_rows_total = 1
        self._loader_rows_done = 1
        self._loader_rows_status = ""
        self._loader_rows_active = False
        self._loader_rows_started_at = None
        self._loader_timeline_total = 1
        self._loader_timeline_done = 1
        self._loader_timeline_status = ""
        self._loader_timeline_active = False
        self._loader_timeline_started_at = None
        self._timeline_loader_thread = None
        self._timeline_loader_stop_event = threading.Event()
        self._selected_loader_thread = None
        self._selected_loader_stop_event = threading.Event()
        self._selected_loader_updates = queue.SimpleQueue()
        self._selected_loader_token = 0
        self._selected_row_loading_idx = None
        self._selected_row_loading_status = ""
        self._selected_row_loader_error = None
        self._selected_row_loading_started_at = None
        self._selected_row_load_avg_seconds = None
        self._master_refresh_thread = None
        self._master_refresh_stop_event = threading.Event()
        self._master_refresh_updates = queue.SimpleQueue()
        self._master_refresh_token = 0
        self._master_refresh_active = False
        self._master_refresh_total_steps = 1
        self._master_refresh_done_steps = 1
        self._master_refresh_status = ""
        self._master_refresh_error = None
        self._master_refresh_started_at = None
        self._fit_cache = {}
        self._sum_norm_scale = 1.0
        self.graph_modes = [
            "normal",
            "hub_3d",
            "hub_3d_evo_fit_env",
            "timeline_hub",
            "spectrum",
            "range",
            "range_hub",
            "range_hub_3d_fit",
            "master_fit_lines_3d",
        ]
        self.graph_mode_index = 0
        self._back_button_rect = None
        self._next_button_rect = None
        self._export_button_rect = None
        self._settings_button_rect = None
        self._normalize_button_rect = None
        self._increment_button_rect = None
        self._hub_graph_rect = None
        self._selected_scatter_plot_rect = None
        self._selected_scatter_point_hits = []
        self._selected_scatter_selected = None
        self._graph3d_dragging = False
        self._graph3d_last_mouse = None
        self._graph3d_yaw = -0.9
        self._graph3d_pitch = 0.45
        self._graph3d_zoom = 1.0
        self._graph3d_zoom_min = 0.45
        self._graph3d_zoom_max = 3.0
        self._range_slider_rect = None
        self._range_slider_dragging = False
        self._env_range_slider_rect = None
        self._env_range_drag_handle = None
        self._timeline_prev_button_rect = None
        self._timeline_play_button_rect = None
        self._timeline_next_button_rect = None
        self._timeline_load_button_rect = None
        self._timeline_step_mode_button_rect = None
        self._timeline_slider_rect = None
        self._timeline_slider_dragging = False
        self._equation_copy_hits = []
        self.timeline_progress = 0.0
        self.timeline_playing = False
        self.timeline_frame_count = 101
        self.timeline_play_frames_per_sec = 12.0
        self.timeline_step_mode = "normal"
        self.normalize_modes = ["none", "range", "sum"]
        self.normalize_mode_index = 0
        self.normalize_mode = "none"
        self.normalize_display = False
        self.increment_modes = ["default", "step0p01"]
        self.increment_mode_index = 0
        self.increment_mode = "default"
        self.range_top_n = 80
        self._range_top_n_auto_all = True
        self.range_top_n_min = 1
        self.range_top_n_max = 1
        self.env_global_min = 0.0
        self.env_global_max = 1.0
        # Start unset so first reload defaults to the full discovered env range.
        self.env_view_min = None
        self.env_view_max = None
        self.export_status = ""
        self._export_status_ok = True
        self.last_reload = 0.0
        self.running = True

        self.reload_from_disk()

    def _rebuild_graph_models(self) -> None:
        self.graph_points = _compute_hub_graph_points(self.rows, include_apex_fallback=True)
        self.hub_fit_report = hr._fit_hub_models_from_rows(self.rows)
        self.hub_best_fit = (
            self.hub_fit_report.get("best_model") if isinstance(self.hub_fit_report, dict) else None
        )
        self._fit_cache.clear()

    def _fit_report_for_graph_points(self, graph_points: list[dict], scope: str = "main") -> dict:
        if not isinstance(graph_points, list) or not graph_points:
            return {}
        low = float(self.env_view_min) if hr._is_number(self.env_view_min) else -1e9
        high = float(self.env_view_max) if hr._is_number(self.env_view_max) else 1e9
        key = (
            str(scope),
            round(low, 5),
            round(high, 5),
            int(len(graph_points)),
        )
        cached = self._fit_cache.get(key)
        if isinstance(cached, dict):
            return cached
        report = hr._fit_hub_models_from_graph_points(graph_points)
        if not isinstance(report, dict):
            report = {}
        if len(self._fit_cache) > 24:
            self._fit_cache.clear()
        self._fit_cache[key] = report
        return report

    def _refresh_row_ui_state(self) -> None:
        self.range_top_n_max = max(
            self.range_top_n_min,
            max(
                [
                    len(row.get("points", []))
                    for row in self.rows
                    if isinstance(row, dict) and isinstance(row.get("points"), list)
                ]
                or [self.range_top_n_min]
            ),
        )
        self.range_top_n = max(
            self.range_top_n_min,
            min(int(self.range_top_n), int(self.range_top_n_max)),
        )
        if bool(self._range_top_n_auto_all):
            self.range_top_n = int(self.range_top_n_max)
        self._refresh_sum_norm_scale()
        env_values = [
            float(row.get("env_rate"))
            for row in self.rows
            if isinstance(row, dict) and hr._is_number(row.get("env_rate"))
        ]
        if env_values:
            self.env_global_min = min(env_values)
            self.env_global_max = max(env_values)
        else:
            self.env_global_min = 0.0
            self.env_global_max = 1.0
        if self.env_global_max <= self.env_global_min:
            self.env_view_min = float(self.env_global_min)
            self.env_view_max = float(self.env_global_max)
        else:
            if not (hr._is_number(self.env_view_min) and hr._is_number(self.env_view_max)):
                self.env_view_min = float(self.env_global_min)
                self.env_view_max = float(self.env_global_max)
            else:
                low = max(
                    float(self.env_global_min),
                    min(float(self.env_view_min), float(self.env_view_max)),
                )
                high = min(
                    float(self.env_global_max),
                    max(float(self.env_view_min), float(self.env_view_max)),
                )
                if high < low:
                    low = float(self.env_global_min)
                    high = float(self.env_global_max)
                self.env_view_min = float(low)
                self.env_view_max = float(high)
        if self.rows:
            if self.selected_row_index is None:
                self.selected_row_index = 0
            else:
                self.selected_row_index = max(0, min(int(self.selected_row_index), len(self.rows) - 1))
        else:
            self.selected_row_index = None

    def _refresh_sum_norm_scale(self) -> None:
        max_norm = 0.0
        for row in self.rows:
            if not isinstance(row, dict):
                continue
            points = row.get("points")
            if not isinstance(points, list) or (not points):
                continue
            fit_vals = []
            total = 0.0
            for pair in points:
                if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                    continue
                fit_val = pair[1]
                if not hr._is_number(fit_val):
                    continue
                fit_float = float(fit_val)
                fit_vals.append(fit_float)
                total += fit_float
            if abs(total) <= 1e-12:
                continue
            for fit_float in fit_vals:
                norm_val = fit_float / total
                if math.isfinite(norm_val) and norm_val > max_norm:
                    max_norm = float(norm_val)
        if (not math.isfinite(max_norm)) or max_norm <= 1e-12:
            self._sum_norm_scale = 1.0
        else:
            self._sum_norm_scale = float(1.0 / max_norm)

    def _stop_background_loader(self, wait: bool = False) -> None:
        if isinstance(self._loader_thread, threading.Thread) and self._loader_thread.is_alive():
            self._loader_stop_event.set()
            if wait:
                self._loader_thread.join(timeout=2.0)
        if isinstance(self._timeline_loader_thread, threading.Thread) and self._timeline_loader_thread.is_alive():
            self._timeline_loader_stop_event.set()
            if wait:
                self._timeline_loader_thread.join(timeout=2.0)
        self._timeline_loader_thread = None
        self._loader_active = False
        self._loader_rows_active = False
        self._loader_timeline_active = False
        if not self._loader_error:
            self._loader_rows_done = max(1, int(self._loader_rows_total))
            self._loader_timeline_done = max(1, int(self._loader_timeline_total))
            self._loader_done_steps = max(1, int(self._loader_total_steps))
        self._loader_rows_started_at = None
        self._loader_timeline_started_at = None

    def _start_background_loader(self, rows_seed: list[dict]) -> None:
        total_rows = len(rows_seed)
        now_ts = float(time.time())
        self._loader_token += 1
        token = int(self._loader_token)
        self._loader_stop_event = threading.Event()
        self._loader_updates = queue.SimpleQueue()
        self._loader_total_steps = max(1, int(total_rows))
        self._loader_done_steps = 0
        self._loader_error = None
        self._loader_rows_total = max(1, int(total_rows))
        self._loader_rows_done = 0 if total_rows > 0 else 1
        self._loader_rows_status = f"Loading simulation rows: 0/{total_rows}"
        self._loader_rows_active = bool(total_rows > 0)
        self._loader_rows_started_at = (now_ts if total_rows > 0 else None)
        self._loader_timeline_total = 1
        self._loader_timeline_done = 1
        self._loader_timeline_status = "Timeline cache idle (loads on request)"
        self._loader_timeline_active = False
        self._loader_timeline_started_at = None
        self._loader_status = self._loader_rows_status
        self._loader_active = bool(total_rows > 0)
        if total_rows <= 0:
            self._loader_total_steps = 1
            self._loader_done_steps = 1
            self._loader_status = "Loaded 100%"
            self._loader_rows_started_at = None
            self._loader_timeline_started_at = None
            return
        self._loader_thread = threading.Thread(
            target=self._background_loader_worker,
            args=(token, rows_seed, self._loader_stop_event),
            daemon=True,
            name=f"hub_viewer_loader_{self.hub_idx if self.hub_idx is not None else 'x'}",
        )
        self._loader_thread.start()

    def _timeline_cache_progress_counts(self) -> tuple[int, int]:
        required_keys = []
        seen = set()
        for row in self.rows:
            key = self._timeline_cache_key_for_row(row)
            if key is None or key in seen:
                continue
            seen.add(key)
            required_keys.append(key)
        loaded = 0
        for key in required_keys:
            if isinstance(self.timeline_cache.get(key), dict):
                loaded += 1
        return int(loaded), int(len(required_keys))

    def _start_timeline_cache_loader(self, force_reload: bool = False) -> None:
        if self._loader_timeline_active:
            return
        if isinstance(self._timeline_loader_thread, threading.Thread) and self._timeline_loader_thread.is_alive():
            return
        self._loader_error = None

        rows_to_load = []
        seen = set()
        for row in self.rows:
            if not isinstance(row, dict):
                continue
            key = self._timeline_cache_key_for_row(row)
            if key is None or key in seen:
                continue
            seen.add(key)
            if (not force_reload) and isinstance(self.timeline_cache.get(key), dict):
                continue
            rows_to_load.append(dict(row))

        total_rows = len(rows_to_load)
        self._loader_timeline_total = max(1, int(total_rows))
        self._loader_timeline_done = 0 if total_rows > 0 else 1
        if total_rows > 0:
            self._loader_timeline_status = f"Loading timeline cache: 0/{int(total_rows)}"
        else:
            self._loader_timeline_status = "Timeline cache already loaded"
        self._loader_timeline_active = bool(total_rows > 0)
        self._loader_timeline_started_at = float(time.time()) if total_rows > 0 else None
        self._loader_status = self._loader_timeline_status
        if total_rows <= 0:
            return

        self._timeline_loader_stop_event = threading.Event()
        token = int(self._loader_token)
        self._timeline_loader_thread = threading.Thread(
            target=self._timeline_cache_loader_worker,
            args=(token, rows_to_load, self._timeline_loader_stop_event),
            daemon=True,
            name=f"hub_viewer_timeline_loader_{self.hub_idx if self.hub_idx is not None else 'x'}",
        )
        self._timeline_loader_thread.start()

    def _timeline_cache_loader_worker(
        self,
        token: int,
        rows_seed: list[dict],
        stop_event: threading.Event,
    ) -> None:
        try:
            total_rows = len(rows_seed)
            if total_rows <= 0:
                return
            done = 0

            def _timeline_task(_idx: int, row_seed: dict):
                row = dict(row_seed)
                key = self._timeline_cache_key_for_row(row)
                payload = self._build_timeline_payload_for_row(row)
                return key, payload

            for _, result in self._loading_pool.run_indexed(
                rows_seed,
                _timeline_task,
                stop_event=stop_event,
            ):
                if stop_event.is_set():
                    return
                if isinstance(result, tuple) and len(result) >= 2:
                    key = result[0]
                    payload = result[1]
                    if key is not None and isinstance(payload, dict):
                        self._loader_updates.put(("timeline", token, key, payload))
                done += 1
                self._loader_updates.put(
                    (
                        "progress_timeline",
                        token,
                        int(done),
                        int(total_rows),
                        f"Loading timeline cache: {done}/{total_rows}",
                    )
                )
            if not stop_event.is_set():
                self._loader_updates.put(
                    (
                        "progress_timeline",
                        token,
                        int(total_rows),
                        int(total_rows),
                        "Timeline cache loaded",
                    )
                )
        except Exception as exc:
            self._loader_updates.put(("error_timeline", token, str(exc)))

    def _background_loader_worker(
        self,
        token: int,
        rows_seed: list[dict],
        stop_event: threading.Event,
    ) -> None:
        try:
            loaded_rows = [dict(row) for row in rows_seed]
            total_rows = len(loaded_rows)
            row_done = 0

            def _row_task(_idx: int, row_seed: dict) -> dict:
                row = dict(row_seed)
                _refresh_row_from_disk(
                    row,
                    recompute_fit=False,
                    increment_mode=self.increment_mode,
                )
                return row

            for idx, row in self._loading_pool.run_indexed(
                loaded_rows,
                _row_task,
                stop_event=stop_event,
            ):
                if stop_event.is_set():
                    return
                loaded_rows[int(idx)] = row
                row_done += 1
                self._loader_updates.put(("row", token, int(idx), row))
                self._loader_updates.put(
                    (
                        "progress_rows",
                        token,
                        int(row_done),
                        int(total_rows),
                        f"Loading simulation rows: {row_done}/{total_rows}",
                    )
                )
                if (
                    row_done == 1
                    or (row_done % int(_BACKGROUND_MODEL_UPDATE_EVERY_ROWS) == 0)
                    or (row_done == total_rows)
                ):
                    graph_points = _compute_hub_graph_points(loaded_rows, include_apex_fallback=True)
                    fit_report = hr._fit_hub_models_from_rows(loaded_rows)
                    best_fit = fit_report.get("best_model") if isinstance(fit_report, dict) else None
                    self._loader_updates.put(("models", token, graph_points, fit_report, best_fit))

            graph_points = _compute_hub_graph_points(loaded_rows, include_apex_fallback=True)
            fit_report = hr._fit_hub_models_from_rows(loaded_rows)
            best_fit = fit_report.get("best_model") if isinstance(fit_report, dict) else None
            self._loader_updates.put(("models", token, graph_points, fit_report, best_fit))
            self._loader_updates.put(("done", token, int(total_rows), int(total_rows), "Loaded 100%"))
        except Exception as exc:
            self._loader_updates.put(("error", token, str(exc)))

    def _drain_background_loader_updates(self, max_items: int = 64) -> None:
        rows_changed = False
        items = 0
        while items < max(1, int(max_items)):
            try:
                item = self._loader_updates.get_nowait()
            except queue.Empty:
                break
            items += 1
            if not isinstance(item, tuple) or len(item) < 2:
                continue
            event = str(item[0])
            token = int(item[1])
            if token != int(self._loader_token):
                continue
            if event == "row" and len(item) >= 4:
                row_idx = int(item[2])
                row = item[3]
                if 0 <= row_idx < len(self.rows) and isinstance(row, dict):
                    self.rows[row_idx] = row
                    rows_changed = True
            elif event == "timeline" and len(item) >= 4:
                key = item[2]
                payload = item[3]
                if key is not None and isinstance(payload, dict):
                    self.timeline_cache[key] = payload
            elif event == "models" and len(item) >= 5:
                graph_points = item[2]
                fit_report = item[3]
                best_fit = item[4]
                if isinstance(graph_points, list):
                    self.graph_points = graph_points
                if isinstance(fit_report, dict):
                    self.hub_fit_report = fit_report
                    self.hub_best_fit = best_fit if isinstance(best_fit, dict) else None
                self._fit_cache.clear()
            elif event == "progress_rows" and len(item) >= 5:
                if not hr._is_number(self._loader_rows_started_at):
                    self._loader_rows_started_at = float(time.time())
                self._loader_rows_done = max(0, int(item[2]))
                self._loader_rows_total = max(1, int(item[3]))
                self._loader_rows_status = str(item[4])
                if self._loader_rows_done >= self._loader_rows_total:
                    self._loader_rows_active = False
                self._loader_status = self._loader_rows_status
            elif event == "progress_timeline" and len(item) >= 5:
                if not hr._is_number(self._loader_timeline_started_at):
                    self._loader_timeline_started_at = float(time.time())
                self._loader_timeline_done = max(0, int(item[2]))
                self._loader_timeline_total = max(1, int(item[3]))
                self._loader_timeline_status = str(item[4])
                if self._loader_timeline_done >= self._loader_timeline_total:
                    self._loader_timeline_done = int(self._loader_timeline_total)
                    self._loader_timeline_active = False
                    self._loader_timeline_started_at = None
                    if "loaded" not in self._loader_timeline_status.lower():
                        self._loader_timeline_status = "Timeline cache loaded"
                self._loader_status = self._loader_timeline_status
            elif event == "error_timeline" and len(item) >= 3:
                self._loader_error = str(item[2])
                self._loader_timeline_status = f"Timeline cache failed: {self._loader_error}"
                self._loader_status = self._loader_timeline_status
                self._loader_timeline_active = False
                self._loader_timeline_started_at = None
            elif event == "done" and len(item) >= 5:
                self._loader_rows_done = max(1, int(self._loader_rows_total))
                self._loader_rows_active = False
                self._loader_status = str(item[4])
                self._loader_active = False
                self._loader_rows_started_at = None
                rows_changed = True
            elif event == "error" and len(item) >= 3:
                self._loader_error = str(item[2])
                self._loader_status = "Background loading failed"
                self._loader_rows_status = self._loader_status
                self._loader_rows_active = False
                self._loader_active = False
                self._loader_rows_started_at = None
            self._loader_total_steps = max(1, int(self._loader_rows_total))
            self._loader_done_steps = max(0, int(self._loader_rows_done))
            if self._loader_done_steps > self._loader_total_steps:
                self._loader_done_steps = int(self._loader_total_steps)
        if rows_changed:
            self._refresh_row_ui_state()

    def _loader_progress_ratio(self) -> float:
        den = max(1, int(self._loader_total_steps))
        return max(0.0, min(1.0, float(self._loader_done_steps) / float(den)))

    def _eta_seconds(self, done_steps: int, total_steps: int, started_at: float | None) -> float | None:
        done = max(0, int(done_steps))
        total = max(1, int(total_steps))
        if done >= total:
            return 0.0
        if done <= 0 or (not hr._is_number(started_at)):
            return None
        elapsed = max(0.0, float(time.time()) - float(started_at))
        if elapsed < 0.2:
            return None
        rate = float(done) / elapsed
        if rate <= 1e-9:
            return None
        remaining = float(total - done) / rate
        if (not math.isfinite(remaining)) or remaining < 0:
            return None
        return float(remaining)

    def _eta_text(self, done_steps: int, total_steps: int, started_at: float | None) -> str:
        eta_remaining = self._eta_seconds(done_steps, total_steps, started_at)
        if not hr._is_number(eta_remaining):
            return "Time remaining --:--"
        return f"Time remaining {hr._fmt_duration(eta_remaining)}"

    def _selected_row_eta_text(self) -> str:
        if not hr._is_number(self._selected_row_load_avg_seconds):
            return "Time remaining --:--"
        avg_seconds = max(0.0, float(self._selected_row_load_avg_seconds))
        if not hr._is_number(self._selected_row_loading_started_at):
            return f"Time remaining {hr._fmt_duration(avg_seconds)}"
        elapsed = max(0.0, float(time.time()) - float(self._selected_row_loading_started_at))
        remaining = max(0.0, avg_seconds - elapsed)
        return f"Time remaining {hr._fmt_duration(remaining)}"

    def _loader_rows_progress_ratio(self) -> float:
        den = max(1, int(self._loader_rows_total))
        return max(0.0, min(1.0, float(self._loader_rows_done) / float(den)))

    def _loader_timeline_progress_ratio(self) -> float:
        den = max(1, int(self._loader_timeline_total))
        return max(0.0, min(1.0, float(self._loader_timeline_done) / float(den)))

    def _master_refresh_progress_ratio(self) -> float:
        den = max(1, int(self._master_refresh_total_steps))
        return max(0.0, min(1.0, float(self._master_refresh_done_steps) / float(den)))

    def _stop_master_refresh(self, wait: bool = False) -> None:
        if isinstance(self._master_refresh_thread, threading.Thread) and self._master_refresh_thread.is_alive():
            self._master_refresh_stop_event.set()
            if wait:
                self._master_refresh_thread.join(timeout=2.0)
        self._master_refresh_token += 1
        self._master_refresh_updates = queue.SimpleQueue()
        self._master_refresh_active = False
        if not self._master_refresh_error:
            self._master_refresh_done_steps = max(1, int(self._master_refresh_total_steps))
        self._master_refresh_started_at = None

    def _master_refresh_worker(self, token: int, stop_event: threading.Event) -> None:
        try:
            latest_meta = _load_hub_meta(self.hub_dir)
            rows = _build_rows(
                self.hub_dir,
                latest_meta,
                refresh_disk=False,
                increment_mode=self.increment_mode,
            )
            total_rows = len(rows)
            total_steps = max(1, int(total_rows) + 4)
            self._master_refresh_updates.put(
                (
                    "progress",
                    int(token),
                    0,
                    int(total_steps),
                    f"Refreshing master fits: 0/{total_rows}",
                )
            )
            refresh_done = 0

            def _refresh_row_task(_idx: int, row_seed: dict) -> dict:
                row = dict(row_seed)
                _refresh_row_from_disk(
                    row,
                    recompute_fit=True,
                    increment_mode=self.increment_mode,
                )
                return row

            for idx, row in self._loading_pool.run_indexed(
                rows,
                _refresh_row_task,
                stop_event=stop_event,
            ):
                if stop_event.is_set():
                    return
                rows[int(idx)] = row
                refresh_done += 1
                self._master_refresh_updates.put(
                    (
                        "progress",
                        int(token),
                        int(refresh_done),
                        int(total_steps),
                        f"Refreshing master fits: {refresh_done}/{total_rows}",
                    )
                )

            issues = []
            done_steps = int(total_rows)

            if stop_event.is_set():
                return
            try:
                _write_hub_fit_equations_csv(self.hub_dir / "hub_fit_equations.csv", rows)
            except Exception as exc:
                issues.append(f"hub_fit_equations.csv ({exc})")
            done_steps += 1
            self._master_refresh_updates.put(
                (
                    "progress",
                    int(token),
                    int(done_steps),
                    int(total_steps),
                    "Writing hub_fit_equations.csv",
                )
            )

            if stop_event.is_set():
                return
            try:
                hr._write_hub_all_points_csv(self.hub_dir / "hub_all_points.csv", rows)
            except Exception as exc:
                issues.append(f"hub_all_points.csv ({exc})")
            done_steps += 1
            self._master_refresh_updates.put(
                (
                    "progress",
                    int(token),
                    int(done_steps),
                    int(total_steps),
                    "Writing hub_all_points.csv",
                )
            )

            if stop_event.is_set():
                return
            try:
                hr._write_hub_stats_csv(self.hub_dir / "hub_stats.csv", rows)
            except Exception as exc:
                issues.append(f"hub_stats.csv ({exc})")
            done_steps += 1
            self._master_refresh_updates.put(
                (
                    "progress",
                    int(token),
                    int(done_steps),
                    int(total_steps),
                    "Writing hub_stats.csv",
                )
            )

            if stop_event.is_set():
                return
            try:
                if _sync_hub_meta_steps_from_rows(latest_meta, rows):
                    (self.hub_dir / "hub_meta.json").write_text(json.dumps(latest_meta, indent=2))
            except Exception as exc:
                issues.append(f"hub_meta.json ({exc})")
            done_steps += 1

            if stop_event.is_set():
                return
            graph_points = _compute_hub_graph_points(rows, include_apex_fallback=True)
            fit_report = hr._fit_hub_models_from_rows(rows)
            best_fit = fit_report.get("best_model") if isinstance(fit_report, dict) else None
            fit_count = len(
                [row for row in rows if isinstance(row, dict) and isinstance(row.get("fit"), dict)]
            )
            summary = (
                f"Updated all master best-fit lines ({fit_count}/{len(rows)}) and rewrote hub fit files"
                if not issues
                else (
                    f"Updated fits in memory ({fit_count}/{len(rows)}); disk update issues: {'; '.join(issues)}"
                )
            )
            self._master_refresh_updates.put(
                (
                    "done",
                    int(token),
                    int(total_steps),
                    rows,
                    latest_meta,
                    graph_points,
                    fit_report if isinstance(fit_report, dict) else {},
                    best_fit if isinstance(best_fit, dict) else None,
                    issues,
                    summary,
                )
            )
        except Exception as exc:
            self._master_refresh_updates.put(("error", int(token), str(exc)))

    def _start_master_refresh(self) -> None:
        if isinstance(self._master_refresh_thread, threading.Thread) and self._master_refresh_thread.is_alive():
            self._set_export_status(False, "Refresh already running")
            return
        self._stop_background_loader(wait=False)
        self._stop_selected_loader(wait=False)
        self._loader_token += 1
        self._selected_loader_token += 1
        self._loader_updates = queue.SimpleQueue()
        self._selected_loader_updates = queue.SimpleQueue()
        self._master_refresh_token += 1
        token = int(self._master_refresh_token)
        self._master_refresh_stop_event = threading.Event()
        self._master_refresh_updates = queue.SimpleQueue()
        self._master_refresh_total_steps = 1
        self._master_refresh_done_steps = 0
        self._master_refresh_status = "Preparing full refresh..."
        self._master_refresh_error = None
        self._master_refresh_active = True
        self._master_refresh_started_at = float(time.time())
        self._set_export_status(True, "Refreshing all master fits in background...")
        self._master_refresh_thread = threading.Thread(
            target=self._master_refresh_worker,
            args=(int(token), self._master_refresh_stop_event),
            daemon=True,
            name=f"hub_viewer_refresh_{self.hub_idx if self.hub_idx is not None else 'x'}",
        )
        self._master_refresh_thread.start()

    def _drain_master_refresh_updates(self, max_items: int = 64) -> None:
        items = 0
        while items < max(1, int(max_items)):
            try:
                item = self._master_refresh_updates.get_nowait()
            except queue.Empty:
                break
            items += 1
            if not isinstance(item, tuple) or len(item) < 2:
                continue
            event = str(item[0])
            token = int(item[1])
            if token != int(self._master_refresh_token):
                continue
            if event == "progress" and len(item) >= 5:
                if not hr._is_number(self._master_refresh_started_at):
                    self._master_refresh_started_at = float(time.time())
                self._master_refresh_done_steps = max(0, int(item[2]))
                self._master_refresh_total_steps = max(1, int(item[3]))
                self._master_refresh_status = str(item[4])
            elif event == "done" and len(item) >= 10:
                self._master_refresh_done_steps = max(0, int(item[2]))
                self._master_refresh_total_steps = max(1, int(item[2]))
                rows = item[3]
                latest_meta = item[4]
                graph_points = item[5]
                fit_report = item[6]
                best_fit = item[7]
                issues = item[8]
                summary = str(item[9])
                if isinstance(rows, list):
                    self.rows = rows
                if isinstance(latest_meta, dict):
                    self.hub_meta = latest_meta
                if isinstance(graph_points, list):
                    self.graph_points = graph_points
                if isinstance(fit_report, dict):
                    self.hub_fit_report = fit_report
                self.hub_best_fit = best_fit if isinstance(best_fit, dict) else None
                self.timeline_cache.clear()
                self._fit_cache.clear()
                self._refresh_row_ui_state()
                self.last_reload = time.time()
                self._master_refresh_status = "Refresh complete"
                self._master_refresh_error = None
                self._master_refresh_active = False
                self._master_refresh_started_at = None
                self._set_export_status(len(issues) == 0, summary)
            elif event == "error" and len(item) >= 3:
                self._master_refresh_error = str(item[2])
                self._master_refresh_status = "Refresh failed"
                self._master_refresh_active = False
                self._master_refresh_started_at = None
                self._set_export_status(False, f"Refresh failed ({self._master_refresh_error})")

    def _stop_selected_loader(self, wait: bool = False) -> None:
        if isinstance(self._selected_loader_thread, threading.Thread) and self._selected_loader_thread.is_alive():
            self._selected_loader_stop_event.set()
            if wait:
                self._selected_loader_thread.join(timeout=1.5)
        self._selected_row_loading_idx = None
        self._selected_row_loading_status = ""
        self._selected_row_loading_started_at = None

    def _row_needs_full_load(self, row: dict | None) -> bool:
        if not isinstance(row, dict):
            return False
        if bool(row.get("_full_loaded")):
            return False
        points = row.get("points")
        if isinstance(points, list) and len(points) > 1:
            return False
        return True

    def _selected_row_loader_worker(
        self,
        token: int,
        row_idx: int,
        row_seed: dict,
        stop_event: threading.Event,
    ) -> None:
        try:
            row = dict(row_seed)
            _refresh_row_from_disk(
                row,
                recompute_fit=False,
                increment_mode=self.increment_mode,
            )
            if stop_event.is_set():
                return
            self._selected_loader_updates.put(("row", int(token), int(row_idx), row))
        except Exception as exc:
            self._selected_loader_updates.put(("error", int(token), int(row_idx), str(exc)))

    def _start_selected_row_loader(self, row_idx: int) -> None:
        if row_idx < 0 or row_idx >= len(self.rows):
            return
        row = self.rows[row_idx]
        if not self._row_needs_full_load(row):
            return
        if (
            isinstance(self._selected_loader_thread, threading.Thread)
            and self._selected_loader_thread.is_alive()
            and self._selected_row_loading_idx == int(row_idx)
        ):
            return
        self._stop_selected_loader(wait=False)
        self._selected_loader_token += 1
        token = int(self._selected_loader_token)
        self._selected_loader_stop_event = threading.Event()
        self._selected_row_loading_idx = int(row_idx)
        self._selected_row_loader_error = None
        self._selected_row_loading_status = f"Loading selected master row {int(row_idx) + 1}..."
        self._selected_row_loading_started_at = float(time.time())
        seed = dict(row)
        self._selected_loader_thread = threading.Thread(
            target=self._selected_row_loader_worker,
            args=(int(token), int(row_idx), seed, self._selected_loader_stop_event),
            daemon=True,
            name=f"hub_viewer_selected_loader_{int(row_idx)}",
        )
        self._selected_loader_thread.start()

    def _drain_selected_loader_updates(self, max_items: int = 4) -> None:
        changed = False
        items = 0
        while items < max(1, int(max_items)):
            try:
                item = self._selected_loader_updates.get_nowait()
            except queue.Empty:
                break
            items += 1
            if not isinstance(item, tuple) or not item:
                continue
            event = str(item[0])
            if event == "row" and len(item) >= 4:
                token = int(item[1])
                row_idx = int(item[2])
                row = item[3]
                if token != int(self._selected_loader_token):
                    continue
                if 0 <= row_idx < len(self.rows) and isinstance(row, dict):
                    self.rows[row_idx] = row
                    changed = True
                if hr._is_number(self._selected_row_loading_started_at):
                    elapsed = max(0.0, float(time.time()) - float(self._selected_row_loading_started_at))
                    if elapsed > 0.05:
                        if hr._is_number(self._selected_row_load_avg_seconds):
                            prev = float(self._selected_row_load_avg_seconds)
                            self._selected_row_load_avg_seconds = (0.65 * prev) + (0.35 * elapsed)
                        else:
                            self._selected_row_load_avg_seconds = float(elapsed)
                self._selected_row_loading_status = ""
                if self._selected_row_loading_idx == row_idx:
                    self._selected_row_loading_idx = None
                self._selected_row_loading_started_at = None
            elif event == "error" and len(item) >= 4:
                token = int(item[1])
                row_idx = int(item[2])
                if token != int(self._selected_loader_token):
                    continue
                self._selected_row_loader_error = str(item[3])
                self._selected_row_loading_status = f"Selected master load failed: {self._selected_row_loader_error}"
                if 0 <= row_idx < len(self.rows) and isinstance(self.rows[row_idx], dict):
                    self.rows[row_idx]["_full_loaded"] = True
                if self._selected_row_loading_idx == row_idx:
                    self._selected_row_loading_idx = None
                self._selected_row_loading_started_at = None
        if changed:
            self._rebuild_graph_models()
            self._refresh_row_ui_state()

    def _queue_selected_row_load(self) -> None:
        if self.selected_row_index is None:
            return
        row_idx = max(0, min(int(self.selected_row_index), len(self.rows) - 1))
        self._start_selected_row_loader(row_idx)

    def reload_from_disk(self, force_full: bool = False, run_in_background: bool = True) -> None:
        self._stop_master_refresh(wait=False)
        self._stop_background_loader(wait=(not run_in_background))
        self._stop_selected_loader(wait=False)
        self.hub_meta = _load_hub_meta(self.hub_dir)
        self.rows = _build_rows(
            self.hub_dir,
            self.hub_meta,
            refresh_disk=False,
            increment_mode=self.increment_mode,
        )
        self.timeline_cache.clear()
        self._rebuild_graph_models()
        if force_full or (not run_in_background):
            seed_rows = _build_rows(
                self.hub_dir,
                self.hub_meta,
                refresh_disk=False,
                increment_mode=self.increment_mode,
            )
            loaded_rows = [dict(row) for row in seed_rows]

            def _full_row_task(_idx: int, row_seed: dict) -> dict:
                row = dict(row_seed)
                _refresh_row_from_disk(
                    row,
                    recompute_fit=True,
                    increment_mode=self.increment_mode,
                )
                return row

            for idx, row in self._loading_pool.run_indexed(loaded_rows, _full_row_task):
                loaded_rows[int(idx)] = row
            self.rows = loaded_rows
            self.timeline_cache.clear()
            self._rebuild_graph_models()
            self._loader_total_steps = 1
            self._loader_done_steps = 1
            self._loader_status = "Loaded 100%"
            self._loader_error = None
            self._loader_active = False
            self._loader_rows_total = 1
            self._loader_rows_done = 1
            self._loader_rows_status = "Simulation rows loaded"
            self._loader_rows_active = False
            self._loader_rows_started_at = None
            self._loader_timeline_total = 1
            self._loader_timeline_done = 1
            self._loader_timeline_status = "Timeline cache idle (loads on request)"
            self._loader_timeline_active = False
            self._loader_timeline_started_at = None
        else:
            self._start_background_loader(self.rows)
        self._refresh_row_ui_state()
        self.timeline_progress = max(0.0, min(1.0, float(self.timeline_progress)))
        self.timeline_playing = False
        self.last_reload = time.time()

    def _refresh_all_master_fit_lines(self) -> None:
        self._start_master_refresh()

    def _selected_row(self) -> dict | None:
        if not self.rows:
            return None
        if self.selected_row_index is None:
            return None
        idx = max(0, min(int(self.selected_row_index), len(self.rows) - 1))
        self.selected_row_index = idx
        self._queue_selected_row_load()
        return self.rows[idx]

    def _table_visible_rows(self) -> int:
        rect = self._table_rect
        if rect is None:
            return max(1, int((self.window_h - 140) // max(1, int(self.table_row_h))))
        table_top = rect.y + 30
        table_bottom = rect.bottom - 28
        rows_h = max(0, table_bottom - table_top)
        return max(1, rows_h // max(1, int(self.table_row_h)))

    def _ensure_selected_visible(self) -> None:
        if self.selected_row_index is None:
            return
        total = len(self.rows)
        if total <= 0:
            return
        idx = max(0, min(int(self.selected_row_index), total - 1))
        self.selected_row_index = idx
        visible = self._table_visible_rows()
        max_scroll = max(0, total - visible)
        start_idx = int(max(0, min(max_scroll, int(self.table_scroll))))
        if idx < start_idx:
            self.table_scroll = float(idx)
        elif idx >= (start_idx + visible):
            self.table_scroll = float(idx - visible + 1)
        self.table_scroll = max(0.0, min(float(max_scroll), float(self.table_scroll)))

    def _move_selected_row(self, delta: int) -> None:
        total = len(self.rows)
        if total <= 0:
            self.selected_row_index = None
            self._selected_scatter_selected = None
            return
        delta_i = int(delta)
        if self.selected_row_index is None:
            idx = 0 if delta_i >= 0 else (total - 1)
        else:
            current = int(self.selected_row_index)
            if delta_i == 0:
                idx = max(0, min(current, total - 1))
            else:
                idx = (current + delta_i) % total
        self.selected_row_index = idx
        self._selected_scatter_selected = None
        self._ensure_selected_visible()
        self._queue_selected_row_load()

    def _env_rate_in_view(self, env_rate) -> bool:
        if not hr._is_number(env_rate):
            return False
        low = min(float(self.env_view_min), float(self.env_view_max))
        high = max(float(self.env_view_min), float(self.env_view_max))
        val = float(env_rate)
        return (low - 1e-9) <= val <= (high + 1e-9)

    def _env_slider_ratio_for_value(self, value: float) -> float:
        if self.env_global_max <= self.env_global_min:
            return 0.0
        ratio = (float(value) - float(self.env_global_min)) / max(
            1e-9,
            float(self.env_global_max) - float(self.env_global_min),
        )
        return max(0.0, min(1.0, ratio))

    def _env_slider_value_from_mouse(self, mx: int) -> float:
        if self._env_range_slider_rect is None:
            return float(self.env_view_min)
        ratio = (float(mx) - float(self._env_range_slider_rect.x)) / max(
            1.0,
            float(self._env_range_slider_rect.width),
        )
        ratio = max(0.0, min(1.0, ratio))
        return float(self.env_global_min) + (
            ratio * (float(self.env_global_max) - float(self.env_global_min))
        )

    def _env_slider_handle_x(self, which: str) -> int:
        if self._env_range_slider_rect is None:
            return 0
        value = float(self.env_view_min) if which == "min" else float(self.env_view_max)
        ratio = self._env_slider_ratio_for_value(value)
        return self._env_range_slider_rect.x + int(ratio * self._env_range_slider_rect.width)

    def _set_env_slider_from_mouse(self, mx: int, handle: str) -> None:
        if self._env_range_slider_rect is None:
            return
        if self.env_global_max <= self.env_global_min:
            self.env_view_min = float(self.env_global_min)
            self.env_view_max = float(self.env_global_max)
            return
        value = self._env_slider_value_from_mouse(mx)
        low = min(float(self.env_view_min), float(self.env_view_max))
        high = max(float(self.env_view_min), float(self.env_view_max))
        if handle == "min":
            low = min(value, high)
        else:
            high = max(value, low)
        self.env_view_min = max(float(self.env_global_min), min(float(self.env_global_max), float(low)))
        self.env_view_max = max(float(self.env_global_min), min(float(self.env_global_max), float(high)))

    def _active_graph_mode(self) -> str:
        if not self.graph_modes:
            return "normal"
        idx = int(self.graph_mode_index) % len(self.graph_modes)
        self.graph_mode_index = idx
        return str(self.graph_modes[idx])

    def _rotate_graph_mode(self, delta: int) -> None:
        if not self.graph_modes:
            self.graph_mode_index = 0
            return
        self.graph_mode_index = (int(self.graph_mode_index) + int(delta)) % len(self.graph_modes)
        self._graph3d_dragging = False
        self._graph3d_last_mouse = None

    def _adjust_graph3d_zoom(self, steps: float) -> None:
        if not hr._is_number(steps):
            return
        zoom = float(self._graph3d_zoom) * (1.12 ** float(steps))
        zoom = max(float(self._graph3d_zoom_min), min(float(self._graph3d_zoom_max), zoom))
        self._graph3d_zoom = float(zoom)

    def _active_normalize_mode(self) -> str:
        modes = self.normalize_modes if isinstance(self.normalize_modes, list) and self.normalize_modes else ["none", "range", "sum"]
        idx = int(self.normalize_mode_index) % len(modes)
        self.normalize_mode_index = idx
        mode = str(modes[idx])
        self.normalize_mode = mode
        self.normalize_display = mode != "none"
        return mode

    def _normalize_mode_label(self) -> str:
        mode = self._active_normalize_mode()
        if mode == "range":
            return "Range"
        if mode == "sum":
            return "Sum"
        return "None"

    def _normalize_mode_description(self) -> str:
        mode = self._active_normalize_mode()
        if mode == "range":
            return "Display normalization: range (min-max mapped to 0..1)"
        if mode == "sum":
            return "Display normalization: sum (fitness divided by total fitness per master, globally scaled)"
        return "Display normalization: none"

    def _cycle_normalize_mode(self) -> None:
        modes = self.normalize_modes if isinstance(self.normalize_modes, list) and self.normalize_modes else ["none", "range", "sum"]
        self.normalize_mode_index = (int(self.normalize_mode_index) + 1) % len(modes)
        self._active_normalize_mode()

    def _active_increment_mode(self) -> str:
        modes = self.increment_modes if isinstance(self.increment_modes, list) and self.increment_modes else ["default", "step0p01"]
        idx = int(self.increment_mode_index) % len(modes)
        self.increment_mode_index = idx
        mode = str(modes[idx])
        self.increment_mode = mode
        return mode

    def _increment_mode_label(self) -> str:
        mode = self._active_increment_mode()
        if mode == "step0p01":
            return "0.01"
        return "0.001"

    def _cycle_increment_mode(self) -> None:
        modes = self.increment_modes if isinstance(self.increment_modes, list) and self.increment_modes else ["default", "step0p01"]
        self.increment_mode_index = (int(self.increment_mode_index) + 1) % len(modes)
        self._active_increment_mode()
        self._selected_scatter_selected = None
        self._set_export_status(True, f"Increment step: {self._increment_mode_label()} (reloading)")
        self.reload_from_disk(force_full=False, run_in_background=True)

    def _normalization_context_for_values(self, values: list[float]) -> dict | None:
        numeric = [float(v) for v in values if hr._is_number(v)]
        if not numeric:
            return None
        return {
            "range": (float(min(numeric)), float(max(numeric))),
            "sum": float(sum(numeric)),
            "sum_scale": float(self._sum_norm_scale),
        }

    def _normalize_value_for_display(self, value: float, context: tuple[float, float] | dict | float | None) -> float:
        if not hr._is_number(value):
            return 0.0
        val = float(value)
        mode = self._active_normalize_mode()
        if mode == "none":
            return val
        if mode == "sum":
            total = None
            scale = 1.0
            if isinstance(context, dict):
                total_val = context.get("sum")
                if hr._is_number(total_val):
                    total = float(total_val)
                scale_val = context.get("sum_scale")
                if hr._is_number(scale_val):
                    scale = float(scale_val)
            elif hr._is_number(context):
                total = float(context)
            if total is None or abs(float(total)) <= 1e-12:
                return 0.0
            norm = (val / float(total)) * float(scale)
            return max(0.0, min(1.0, float(norm)))
        bounds = None
        if isinstance(context, dict):
            range_val = context.get("range")
            if isinstance(range_val, (tuple, list)) and len(range_val) >= 2:
                bounds = (float(range_val[0]), float(range_val[1]))
        elif isinstance(context, (tuple, list)) and len(context) >= 2:
            bounds = (float(context[0]), float(context[1]))
        if bounds is None:
            return val
        low = float(bounds[0])
        high = float(bounds[1])
        if high <= low:
            return 0.5
        norm = (val - low) / (high - low)
        return max(0.0, min(1.0, float(norm)))

    def _normalize_graph_points_for_display(
        self,
        graph_points: list[dict],
        group_key: str = "row_index",
    ) -> tuple[list[dict], dict]:
        if (not self.normalize_display) or (not isinstance(graph_points, list)):
            return graph_points, {}
        contexts: dict = {}
        for point in graph_points:
            if not isinstance(point, dict):
                continue
            fit_val = point.get("fitness")
            if not hr._is_number(fit_val):
                continue
            group = point.get(group_key)
            if group not in contexts:
                contexts[group] = {
                    "range": [float(fit_val), float(fit_val)],
                    "sum": float(fit_val),
                    "sum_scale": float(self._sum_norm_scale),
                }
            else:
                cur = contexts[group]
                range_cur = cur.get("range")
                if isinstance(range_cur, list) and len(range_cur) >= 2:
                    range_cur[0] = min(float(range_cur[0]), float(fit_val))
                    range_cur[1] = max(float(range_cur[1]), float(fit_val))
                cur["sum"] = float(cur.get("sum", 0.0)) + float(fit_val)
        normalized = []
        for point in graph_points:
            if not isinstance(point, dict):
                continue
            fit_val = point.get("fitness")
            if not hr._is_number(fit_val):
                normalized.append(point)
                continue
            group = point.get(group_key)
            group_context = contexts.get(group)
            norm_fit = self._normalize_value_for_display(float(fit_val), group_context if isinstance(group_context, dict) else None)
            item = dict(point)
            item["fitness"] = float(norm_fit)
            normalized.append(item)
        final_contexts = {}
        for key, payload in contexts.items():
            if not isinstance(payload, dict):
                continue
            range_payload = payload.get("range")
            if isinstance(range_payload, list) and len(range_payload) >= 2:
                range_tuple = (float(range_payload[0]), float(range_payload[1]))
            elif isinstance(range_payload, tuple) and len(range_payload) >= 2:
                range_tuple = (float(range_payload[0]), float(range_payload[1]))
            else:
                range_tuple = None
            final_contexts[key] = {
                "range": range_tuple,
                "sum": float(payload.get("sum", 0.0)),
                "sum_scale": float(payload.get("sum_scale", self._sum_norm_scale)),
            }
        return normalized, final_contexts

    def _timeline_cache_key_for_row(self, row: dict | None):
        if not isinstance(row, dict):
            return None
        env_dir_raw = row.get("env_dir")
        if not env_dir_raw:
            return None
        return (str(Path(str(env_dir_raw))), "timeline_snapshots")

    def _build_timeline_payload_for_row(self, row: dict | None) -> dict:
        if not isinstance(row, dict):
            return {
                "series": [],
                "fit": None,
                "master_points": [],
                "master_fit": None,
                "cloud_series": [],
                "timeline_source": "none",
            }
        env_dir_raw = row.get("env_dir")
        if not env_dir_raw:
            return {
                "series": [],
                "fit": None,
                "master_points": [],
                "master_fit": None,
                "cloud_series": [],
                "timeline_source": "none",
            }
        env_dir = Path(str(env_dir_raw))

        run_dirs = {}
        snapshot_files_by_run = {}
        for child in sorted(env_dir.iterdir()) if env_dir.is_dir() else []:
            if not child.is_dir():
                continue
            run_num = _safe_int(child.name)
            if run_num is None:
                continue
            run_dirs[int(run_num)] = child
            snap_dir = child / "snapshots"
            if not snap_dir.is_dir():
                continue
            snap_files = _sample_paths_evenly(
                sorted(snap_dir.glob("arith_mean_*.csv")),
                _TIMELINE_MAX_SNAPSHOTS_PER_RUN,
            )
            if not snap_files:
                continue
            snapshot_files_by_run[int(run_num)] = snap_files

        all_points = []
        series = []
        cloud_series = []
        used_snapshots = False
        used_fallback = False
        run_nums_sorted = sorted(run_dirs.keys())
        run_results: dict[int, dict] = {}

        def _timeline_run_task(_idx: int, run_num: int) -> dict:
            run_num = int(run_num)
            run_dir = run_dirs[int(run_num)]
            snap_files = snapshot_files_by_run.get(int(run_num), [])
            norm_points = []
            norm_cloud_samples = []
            used_snapshots_local = False
            used_fallback_local = False
            if snap_files:
                frame_entries = []
                for path in snap_files:
                    try:
                        frame = int(path.stem.split("_")[-1])
                    except Exception:
                        continue
                    raw_points = hr._extract_points_from_csv(path)
                    numeric_points = []
                    for pair in raw_points:
                        if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                            continue
                        evo_val, fit_val = pair[0], pair[1]
                        if hr._is_number(evo_val) and hr._is_number(fit_val):
                            numeric_points.append((float(evo_val), float(fit_val)))
                    apex_x, apex_y = _apex_from_points(numeric_points)
                    frame_entries.append(
                        {
                            "frame": int(frame),
                            "points": numeric_points,
                            "apex_x": float(apex_x) if hr._is_number(apex_x) else None,
                            "apex_y": float(apex_y) if hr._is_number(apex_y) else None,
                        }
                    )
                frame_entries.sort(key=lambda item: int(item.get("frame", 0)))
                frame_points = [
                    (int(item["frame"]), float(item["apex_x"]))
                    for item in frame_entries
                    if hr._is_number(item.get("apex_x"))
                ]
                if len(frame_points) == 1:
                    norm_points = [(0.0, float(frame_points[0][1])), (1.0, float(frame_points[0][1]))]
                elif len(frame_points) > 1:
                    start_f = float(frame_points[0][0])
                    end_f = float(frame_points[-1][0])
                    if end_f > start_f:
                        norm_points = [
                            ((float(frame) - start_f) / (end_f - start_f), float(yv))
                            for frame, yv in frame_points
                        ]
                    else:
                        den = max(1, len(frame_points) - 1)
                        norm_points = [
                            (float(i) / float(den), float(frame_points[i][1]))
                            for i in range(len(frame_points))
                        ]
                if frame_entries:
                    if len(frame_entries) == 1:
                        only_points = list(frame_entries[0].get("points", []))
                        norm_cloud_samples = [
                            {"norm": 0.0, "points": only_points},
                            {"norm": 1.0, "points": only_points},
                        ]
                    else:
                        start_f = float(frame_entries[0].get("frame", 0))
                        end_f = float(frame_entries[-1].get("frame", 0))
                        if end_f > start_f:
                            for item in frame_entries:
                                norm_val = (float(item.get("frame", 0)) - start_f) / (end_f - start_f)
                                norm_cloud_samples.append({"norm": float(norm_val), "points": list(item.get("points", []))})
                        else:
                            den = max(1, len(frame_entries) - 1)
                            for i, item in enumerate(frame_entries):
                                norm_val = float(i) / float(den)
                                norm_cloud_samples.append({"norm": float(norm_val), "points": list(item.get("points", []))})
                if norm_points:
                    used_snapshots_local = True
            if not norm_points:
                parsed_points = _run_parsed_arithmetic_points(
                    run_dir,
                    int(run_num),
                    increment_mode=self.increment_mode,
                )
                apex_x, _ = _apex_from_points(parsed_points)
                if hr._is_number(apex_x):
                    norm_points = [(0.0, float(apex_x)), (1.0, float(apex_x))]
                    used_fallback_local = True
                numeric_parsed = []
                for pair in parsed_points:
                    if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                        continue
                    evo_val, fit_val = pair[0], pair[1]
                    if hr._is_number(evo_val) and hr._is_number(fit_val):
                        numeric_parsed.append((float(evo_val), float(fit_val)))
                if numeric_parsed:
                    norm_cloud_samples = [
                        {"norm": 0.0, "points": list(numeric_parsed)},
                        {"norm": 1.0, "points": list(numeric_parsed)},
                    ]
            return {
                "run_num": int(run_num),
                "norm_points": norm_points,
                "norm_cloud_samples": norm_cloud_samples,
                "used_snapshots": bool(used_snapshots_local),
                "used_fallback": bool(used_fallback_local),
            }

        for _, run_payload in self._loading_pool.run_indexed(run_nums_sorted, _timeline_run_task):
            if not isinstance(run_payload, dict):
                continue
            run_num = _safe_int(run_payload.get("run_num"))
            if run_num is None:
                continue
            run_results[int(run_num)] = run_payload

        for idx, run_num in enumerate(run_nums_sorted):
            run_payload = run_results.get(int(run_num))
            if not isinstance(run_payload, dict):
                continue
            norm_points = run_payload.get("norm_points")
            norm_cloud_samples = run_payload.get("norm_cloud_samples")
            if bool(run_payload.get("used_snapshots")):
                used_snapshots = True
            if bool(run_payload.get("used_fallback")):
                used_fallback = True
            if not isinstance(norm_points, list) or not norm_points:
                continue
            all_points.extend(norm_points)
            series.append(
                {
                    "run_num": int(run_num),
                    "color": _SIM_COLORS[idx % len(_SIM_COLORS)],
                    "points": norm_points,
                }
            )
            if isinstance(norm_cloud_samples, list) and norm_cloud_samples:
                cloud_series.append(
                    {
                        "run_num": int(run_num),
                        "color": _SIM_COLORS[idx % len(_SIM_COLORS)],
                        "samples": norm_cloud_samples,
                    }
                )

        if used_snapshots and used_fallback:
            source = "mixed"
        elif used_snapshots:
            source = "snapshots"
        elif used_fallback:
            source = "final_only"
        else:
            source = "none"
        master_points = _timeline_master_average(series)
        return {
            "series": series,
            "fit": _timeline_fit(all_points),
            "master_points": master_points,
            "master_fit": _timeline_fit(master_points),
            "cloud_series": cloud_series,
            "timeline_source": source,
        }

    def _timeline_payload_for_row(self, row: dict | None, build_if_missing: bool = False) -> dict:
        key = self._timeline_cache_key_for_row(row)
        if key is None:
            return {
                "series": [],
                "fit": None,
                "master_points": [],
                "master_fit": None,
                "cloud_series": [],
                "timeline_source": "none",
            }
        cached = self.timeline_cache.get(key)
        if isinstance(cached, dict):
            return cached
        if build_if_missing:
            payload = self._build_timeline_payload_for_row(row)
            self.timeline_cache[key] = payload
            return payload
        return {
            "series": [],
            "fit": None,
            "master_points": [],
            "master_fit": None,
            "cloud_series": [],
            "timeline_source": "none",
        }

    def _cloud_points_for_progress(self, samples: list, cursor_norm: float) -> list[tuple[float, float]]:
        if not isinstance(samples, list) or not samples:
            return []
        target = max(0.0, min(1.0, float(cursor_norm)))
        best = None
        best_dist = None
        for item in samples:
            if not isinstance(item, dict):
                continue
            norm_val = item.get("norm")
            points = item.get("points")
            if (not hr._is_number(norm_val)) or (not isinstance(points, list)):
                continue
            dist = abs(float(norm_val) - target)
            if best is None or best_dist is None or dist < best_dist:
                best = item
                best_dist = dist
        if not isinstance(best, dict):
            return []
        out = []
        for pair in best.get("points", []):
            if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                continue
            evo_val, fit_val = pair[0], pair[1]
            if hr._is_number(evo_val) and hr._is_number(fit_val):
                out.append((float(evo_val), float(fit_val)))
        return out

    def _timeline_step_size(self) -> float:
        frames = max(2, int(self.timeline_frame_count))
        return 1.0 / float(frames - 1)

    def _timeline_step_mode_label(self) -> str:
        if str(self.timeline_step_mode) == "point1":
            return "0.1"
        return "normal"

    def _toggle_timeline_step_mode(self) -> None:
        if str(self.timeline_step_mode) == "point1":
            self.timeline_step_mode = "normal"
        else:
            self.timeline_step_mode = "point1"

    def _timeline_frame_index(self) -> int:
        frames = max(2, int(self.timeline_frame_count))
        idx = int(round(float(self.timeline_progress) * float(frames - 1)))
        return max(0, min(frames - 1, idx))

    def _step_timeline_frame(self, delta_steps: int) -> None:
        if str(self.timeline_step_mode) == "point1":
            new_progress = round(float(self.timeline_progress) + (float(delta_steps) * 0.1), 1)
        else:
            step = self._timeline_step_size()
            new_progress = float(self.timeline_progress) + (float(delta_steps) * step)
        self.timeline_progress = max(0.0, min(1.0, new_progress))

    def _set_timeline_slider_from_mouse(self, mx: int) -> None:
        if self._timeline_slider_rect is None:
            return
        ratio = (float(mx) - float(self._timeline_slider_rect.x)) / max(1.0, float(self._timeline_slider_rect.width))
        ratio = max(0.0, min(1.0, ratio))
        self.timeline_progress = float(ratio)

    def _update_timeline_playback(self, dt_s: float) -> None:
        mode = self._active_graph_mode()
        if mode != "timeline_hub":
            return
        if not self.timeline_playing or self._timeline_slider_dragging:
            return
        step = self._timeline_step_size()
        self.timeline_progress += float(max(0.0, dt_s)) * float(self.timeline_play_frames_per_sec) * step
        if self.timeline_progress >= 1.0:
            self.timeline_progress = 1.0
            self.timeline_playing = False

    def _next_export_path(self, base_dir: Path, base_name: str) -> Path:
        base = base_dir / base_name
        if not base.exists():
            return base
        stem = base.stem
        suffix = base.suffix
        for idx in range(1, 1000):
            candidate = base_dir / f"{stem}_{idx}{suffix}"
            if not candidate.exists():
                return candidate
        return base

    def _export_root_dir(self) -> Path:
        export_dir = self.hub_dir / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        return export_dir

    def _set_export_status(self, ok: bool, message: str) -> None:
        self.export_status = str(message)
        self._export_status_ok = bool(ok)

    def _draw_equation_copy_button(self, rect, equation_text: str, y: int, margin: int = 10) -> int:
        if not str(equation_text).strip():
            return 0
        label = "Copy"
        btn_w = max(40, int(self.tiny.size(label)[0]) + 10)
        btn_h = max(14, int(self.tiny.get_linesize()) + 2)
        btn_rect = self.pg.Rect(
            int(rect.right - margin - btn_w),
            int(y),
            int(btn_w),
            int(btn_h),
        )
        self.pg.draw.rect(self.screen, (52, 58, 76), btn_rect)
        self.pg.draw.rect(self.screen, (150, 156, 174), btn_rect, 1)
        txt = self.tiny.render(label, True, (232, 236, 245))
        self.screen.blit(
            txt,
            (
                btn_rect.x + (btn_rect.width - txt.get_width()) // 2,
                btn_rect.y + (btn_rect.height - txt.get_height()) // 2,
            ),
        )
        self._equation_copy_hits.append((btn_rect.copy(), str(equation_text)))
        return int(btn_rect.width + 8)

    def _handle_equation_copy_click(self, mx: int, my: int) -> bool:
        for item in reversed(self._equation_copy_hits):
            if (not isinstance(item, tuple)) or len(item) != 2:
                continue
            rect, eq_text = item
            if not isinstance(rect, self.pg.Rect):
                continue
            if not rect.collidepoint(mx, my):
                continue
            ok = hr._copy_to_clipboard(str(eq_text))
            if ok:
                self._set_export_status(True, "Equation copied to clipboard")
            else:
                self._set_export_status(False, "Failed to copy equation to clipboard")
            return True
        return False

    def _is_3d_mode(self, mode: str) -> bool:
        return str(mode) in ("hub_3d", "hub_3d_evo_fit_env", "range_hub_3d_fit", "master_fit_lines_3d")

    def _graph3d_export_payload(self, mode: str) -> dict | None:
        m = str(mode)
        if m == "hub_3d":
            return {
                "axis_keys": ("x", "y", "fitness"),
                "points": list(self.graph_points),
            }
        if m == "hub_3d_evo_fit_env":
            return {
                "axis_keys": ("y", "fitness", "x"),
                "points": list(self.graph_points),
            }
        if m == "range_hub_3d_fit":
            graph_points, _, _ = self._range_hub_graph_data()
            return {
                "axis_keys": ("x", "y", "fitness"),
                "points": list(graph_points),
            }
        if m == "master_fit_lines_3d":
            graph_points, _ = self._master_fit_lines_3d_data()
            return {
                "axis_keys": ("x", "y", "fitness"),
                "points": list(graph_points),
            }
        return None

    def _triangle_normal(
        self,
        a: tuple[float, float, float],
        b: tuple[float, float, float],
        c: tuple[float, float, float],
    ) -> tuple[float, float, float]:
        ux = b[0] - a[0]
        uy = b[1] - a[1]
        uz = b[2] - a[2]
        vx = c[0] - a[0]
        vy = c[1] - a[1]
        vz = c[2] - a[2]
        nx = (uy * vz) - (uz * vy)
        ny = (uz * vx) - (ux * vz)
        nz = (ux * vy) - (uy * vx)
        mag = math.sqrt((nx * nx) + (ny * ny) + (nz * nz))
        if mag <= 1e-12:
            return (0.0, 0.0, 0.0)
        return (nx / mag, ny / mag, nz / mag)

    def _surface_triangles_for_stl(
        self,
        points: list[dict],
        axis_keys: tuple[str, str, str],
        grid_n: int = 38,
        nearest_k: int = 8,
    ) -> tuple[list[tuple[tuple[float, float, float], tuple[float, float, float], tuple[float, float, float]]], str | None]:
        key_x, key_y, key_z = axis_keys
        coords = []
        for item in points:
            if not isinstance(item, dict):
                continue
            x_val = item.get(key_x)
            y_val = item.get(key_y)
            z_val = item.get(key_z)
            if hr._is_number(x_val) and hr._is_number(y_val) and hr._is_number(z_val):
                coords.append((float(x_val), float(y_val), float(z_val)))
        if len(coords) < 3:
            return [], "not enough 3D points"

        # Keep STL generation responsive on large hubs.
        if len(coords) > 3200:
            step = max(1, int(len(coords) / 3200))
            coords = coords[::step]

        xs = [p[0] for p in coords]
        ys = [p[1] for p in coords]
        min_x = min(xs)
        max_x = max(xs)
        min_y = min(ys)
        max_y = max(ys)
        if (max_x - min_x) <= 1e-12 or (max_y - min_y) <= 1e-12:
            return [], "degenerate range"

        n = max(8, int(grid_n))
        k = max(3, int(nearest_k))
        eps = 1e-12

        def _interp_z(xg: float, yg: float) -> float:
            nearest: list[tuple[float, float]] = []
            for px, py, pz in coords:
                dx = px - xg
                dy = py - yg
                d2 = (dx * dx) + (dy * dy)
                if d2 <= 1e-14:
                    return pz
                if len(nearest) < k:
                    nearest.append((d2, pz))
                    nearest.sort(key=lambda t: t[0])
                    continue
                if d2 < nearest[-1][0]:
                    nearest[-1] = (d2, pz)
                    nearest.sort(key=lambda t: t[0])
            num = 0.0
            den = 0.0
            for d2, z in nearest:
                w = 1.0 / max(eps, d2)
                num += (w * z)
                den += w
            return num / max(eps, den)

        grid = [[(0.0, 0.0, 0.0) for _ in range(n)] for _ in range(n)]
        for iy in range(n):
            y = min_y + ((max_y - min_y) * float(iy) / float(n - 1))
            for ix in range(n):
                x = min_x + ((max_x - min_x) * float(ix) / float(n - 1))
                z = _interp_z(x, y)
                grid[iy][ix] = (float(x), float(y), float(z))

        triangles = []
        for iy in range(n - 1):
            for ix in range(n - 1):
                v00 = grid[iy][ix]
                v10 = grid[iy][ix + 1]
                v01 = grid[iy + 1][ix]
                v11 = grid[iy + 1][ix + 1]
                triangles.append((v00, v10, v01))
                triangles.append((v10, v11, v01))
        return triangles, None

    def _point_cloud_triangles_for_stl(
        self,
        points: list[dict],
        axis_keys: tuple[str, str, str],
        max_points: int = 700,
    ) -> tuple[
        list[tuple[tuple[float, float, float], tuple[float, float, float], tuple[float, float, float]]],
        str | None,
    ]:
        key_x, key_y, key_z = axis_keys
        coords = []
        for item in points:
            if not isinstance(item, dict):
                continue
            x_val = item.get(key_x)
            y_val = item.get(key_y)
            z_val = item.get(key_z)
            if hr._is_number(x_val) and hr._is_number(y_val) and hr._is_number(z_val):
                coords.append((float(x_val), float(y_val), float(z_val)))
        if not coords:
            return [], "no numeric points"
        if len(coords) > int(max_points):
            step = max(1, int(math.ceil(float(len(coords)) / float(max_points))))
            coords = coords[::step]

        xs = [c[0] for c in coords]
        ys = [c[1] for c in coords]
        zs = [c[2] for c in coords]
        span_x = max(1e-9, max(xs) - min(xs))
        span_y = max(1e-9, max(ys) - min(ys))
        span_z = max(1e-9, max(zs) - min(zs))
        scale = max(span_x, span_y, span_z)
        half_size = max(1e-9, scale * 0.0045)

        triangles = []
        for cx, cy, cz in coords:
            x0 = cx - half_size
            x1 = cx + half_size
            y0 = cy - half_size
            y1 = cy + half_size
            z0 = cz - half_size
            z1 = cz + half_size
            v000 = (x0, y0, z0)
            v100 = (x1, y0, z0)
            v110 = (x1, y1, z0)
            v010 = (x0, y1, z0)
            v001 = (x0, y0, z1)
            v101 = (x1, y0, z1)
            v111 = (x1, y1, z1)
            v011 = (x0, y1, z1)
            # Bottom/Top
            triangles.append((v000, v110, v100))
            triangles.append((v000, v010, v110))
            triangles.append((v001, v101, v111))
            triangles.append((v001, v111, v011))
            # Front/Back
            triangles.append((v000, v100, v101))
            triangles.append((v000, v101, v001))
            triangles.append((v010, v111, v110))
            triangles.append((v010, v011, v111))
            # Left/Right
            triangles.append((v000, v001, v011))
            triangles.append((v000, v011, v010))
            triangles.append((v100, v110, v111))
            triangles.append((v100, v111, v101))
        if not triangles:
            return [], "no triangles"
        return triangles, None

    def _write_ascii_stl(
        self,
        output_path: Path,
        triangles: list[tuple[tuple[float, float, float], tuple[float, float, float], tuple[float, float, float]]],
        solid_name: str,
    ) -> None:
        with output_path.open("w", encoding="ascii") as out:
            out.write(f"solid {solid_name}\n")
            for a, b, c in triangles:
                nx, ny, nz = self._triangle_normal(a, b, c)
                out.write(f"  facet normal {nx:.6e} {ny:.6e} {nz:.6e}\n")
                out.write("    outer loop\n")
                out.write(f"      vertex {a[0]:.6e} {a[1]:.6e} {a[2]:.6e}\n")
                out.write(f"      vertex {b[0]:.6e} {b[1]:.6e} {b[2]:.6e}\n")
                out.write(f"      vertex {c[0]:.6e} {c[1]:.6e} {c[2]:.6e}\n")
                out.write("    endloop\n")
                out.write("  endfacet\n")
            out.write(f"endsolid {solid_name}\n")

    def _export_graph_stl(self, mode: str) -> tuple[bool, str]:
        payload = self._graph3d_export_payload(mode)
        if not isinstance(payload, dict):
            return False, "unsupported 3D mode"
        axis_keys = payload.get("axis_keys")
        source_points = payload.get("points")
        if not isinstance(axis_keys, tuple) or len(axis_keys) != 3:
            return False, "invalid axis map"
        if not isinstance(source_points, list):
            return False, "invalid graph points"

        points = [item for item in source_points if isinstance(item, dict) and self._env_rate_in_view(item.get("x"))]
        if len(points) < 1:
            return False, "not enough points"

        mesh_type = "surface"
        triangles, err = self._surface_triangles_for_stl(points, axis_keys)
        if err is not None or (not triangles):
            triangles, fallback_err = self._point_cloud_triangles_for_stl(points, axis_keys)
            if fallback_err is not None or (not triangles):
                if err is not None:
                    return False, f"{err}; fallback failed ({fallback_err})"
                return False, f"fallback failed ({fallback_err})"
            mesh_type = "point-cloud"
        if not triangles:
            return False, "no triangles"

        export_dir = self._export_root_dir()
        output_path = self._next_export_path(export_dir, f"{self.hub_dir.name}_{mode}.stl")
        try:
            self._write_ascii_stl(output_path, triangles, solid_name=f"{self.hub_dir.name}_{mode}")
            return True, f"{output_path.name} ({mesh_type})"
        except Exception as exc:
            return False, f"stl export failed ({exc})"

    def _export_graph_png(self, mode: str) -> tuple[bool, str]:
        export_dir = self._export_root_dir()
        output_path = self._next_export_path(export_dir, f"{self.hub_dir.name}_{mode}.png")
        try:
            self._draw(include_status=False)
            self.pg.image.save(self.screen, str(output_path))
            return True, output_path.name
        except Exception as exc:
            return False, f"png export failed ({exc})"

    def _export_timeline_mov(self) -> tuple[bool, str]:
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            return False, "ffmpeg not found"

        export_dir = self._export_root_dir()
        frame_dir = export_dir / f"{self.hub_dir.name}_timeline_frames_{int(time.time() * 1000)}"
        frame_dir.mkdir(parents=True, exist_ok=True)

        prev_progress = float(self.timeline_progress)
        prev_playing = bool(self.timeline_playing)
        self.timeline_playing = False
        try:
            self._draw(include_status=False)
            frame_total = max(2, int(self.timeline_frame_count))
            for idx in range(frame_total):
                if frame_total <= 1:
                    self.timeline_progress = 0.0
                else:
                    self.timeline_progress = float(idx) / float(frame_total - 1)
                self._draw(include_status=False)
                frame_path = frame_dir / f"frame_{idx:06d}.png"
                self.pg.image.save(self.screen, str(frame_path))

            output_path = self._next_export_path(export_dir, f"{self.hub_dir.name}_timeline.mov")
            cmd = [
                ffmpeg,
                "-y",
                "-framerate",
                str(max(1, int(round(float(self.timeline_play_frames_per_sec))))),
                "-i",
                str(frame_dir / "frame_%06d.png"),
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-movflags",
                "+faststart",
                str(output_path),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                return False, "ffmpeg export failed"
            return True, output_path.name
        except Exception as exc:
            return False, f"mov export failed ({exc})"
        finally:
            self.timeline_progress = prev_progress
            self.timeline_playing = prev_playing
            shutil.rmtree(frame_dir, ignore_errors=True)
            self._draw(include_status=False)

    def _export_active_mode(self) -> None:
        mode = self._active_graph_mode()
        if mode == "timeline_hub":
            ok, msg = self._export_timeline_mov()
            if ok:
                self._set_export_status(True, f"Exported .mov: {msg}")
            else:
                self._set_export_status(False, f"Export failed (.mov): {msg}")
            return
        if self._is_3d_mode(mode):
            stl_ok, stl_msg = self._export_graph_stl(mode)
            png_ok, png_msg = self._export_graph_png(mode)
            if stl_ok and png_ok:
                self._set_export_status(True, f"Exported 3D files: .stl {stl_msg} | .png {png_msg}")
            elif stl_ok and (not png_ok):
                self._set_export_status(False, f"3D export partial: .stl {stl_msg} | .png failed ({png_msg})")
            elif png_ok and (not stl_ok):
                self._set_export_status(False, f"3D export partial: .png {png_msg} | .stl failed ({stl_msg})")
            else:
                self._set_export_status(False, f"Export failed (3D): .stl {stl_msg} | .png {png_msg}")
            return
        ok, msg = self._export_graph_png(mode)
        if ok:
            self._set_export_status(True, f"Exported .png: {msg}")
        else:
            self._set_export_status(False, f"Export failed (.png): {msg}")

    def _draw_table(self, rect) -> None:
        pg = self.pg
        self._table_rect = rect
        pg.draw.rect(self.screen, (19, 22, 28), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)

        cols = [
            ("step", 46),
            ("rate", 60),
            ("master", 82),
            ("status", 74),
            ("species", 82),
            ("frames", 68),
            ("dur", 64),
        ]
        x = rect.x + 8
        y = rect.y + 8
        col_layout = []
        for name, width in cols:
            col_layout.append((x, width))
            surf = self.tiny.render(name, True, (188, 194, 205))
            self.screen.blit(surf, (x, y))
            x += width

        divider_y = y + 18
        pg.draw.line(self.screen, (58, 62, 74), (rect.x + 6, divider_y), (rect.right - 6, divider_y), 1)

        table_top = divider_y + 4
        table_bottom = rect.bottom - 28
        rows_h = max(0, table_bottom - table_top)
        visible = max(1, rows_h // self.table_row_h)
        total = len(self.rows)
        max_scroll = max(0, total - visible)
        self.table_scroll = max(0.0, min(float(max_scroll), float(self.table_scroll)))
        start_idx = int(self.table_scroll)
        self.table_row_hits = []

        for local_i in range(visible):
            row_idx = start_idx + local_i
            if row_idx >= total:
                break
            row = self.rows[row_idx]
            ry = table_top + (local_i * self.table_row_h)
            if (local_i % 2) == 0:
                pg.draw.rect(self.screen, (15, 18, 24), (rect.x + 4, ry, rect.width - 8, self.table_row_h))
            if self.selected_row_index is not None and row_idx == int(self.selected_row_index):
                pg.draw.rect(self.screen, (39, 56, 78), (rect.x + 3, ry, rect.width - 6, self.table_row_h))

            status = str(row.get("status", "pending"))
            status_color = {
                "ok": (150, 232, 166),
                "failed": (255, 140, 140),
                "pending": (170, 170, 170),
                "running": (255, 220, 122),
                "stopped": (255, 182, 120),
                "aborted": (255, 130, 130),
                "no_master": (255, 160, 120),
            }.get(status, (205, 205, 205))

            values = [
                str(int(row.get("step_index", 0)) + 1),
                (f"{float(row.get('env_rate', 0.0)):.2f}" if hr._is_number(row.get("env_rate")) else "--"),
                (
                    f"m{int(row.get('master_run_num'))}"
                    if row.get("master_run_num") is not None
                    else "--"
                ),
                status,
                (
                    f"{float(row.get('total_species')):.0f}"
                    if hr._is_number(row.get("total_species"))
                    else (
                        f"{float(row.get('max_species')):.0f}"
                        if hr._is_number(row.get("max_species"))
                        else "--"
                    )
                ),
                (f"{float(row.get('max_frames')):.0f}" if hr._is_number(row.get("max_frames")) else "--"),
                hr._fmt_duration(row.get("duration_s")),
            ]
            for ci, (cx, width) in enumerate(col_layout):
                color = status_color if ci == 3 else (212, 212, 212)
                txt = self.tiny.render(hr._fit_text(self.tiny, values[ci], max(8, width - 6)), True, color)
                self.screen.blit(txt, (cx, ry + 4))
            self.table_row_hits.append((row_idx, ry, ry + self.table_row_h))

        info = self.tiny.render(
            f"Rows {min(total, start_idx + 1)}-{min(total, start_idx + visible)} / {total}    U: refresh fits",
            True,
            (152, 160, 176),
        )
        self.screen.blit(info, (rect.x + 8, rect.bottom - 22))

        if total > visible:
            bar_x = rect.right - 10
            bar_y = table_top
            bar_h = max(16, table_bottom - table_top)
            pg.draw.rect(self.screen, (44, 48, 58), (bar_x, bar_y, 4, bar_h))
            thumb_h = max(20, int((visible / max(1, total)) * bar_h))
            ratio = float(start_idx) / max(1.0, float(max_scroll))
            thumb_y = bar_y + int((bar_h - thumb_h) * ratio)
            pg.draw.rect(self.screen, (126, 134, 151), (bar_x - 1, thumb_y, 6, thumb_h))
            self._table_scrollbar_track_rect = pg.Rect(bar_x - 4, bar_y, 12, bar_h)
            self._table_scrollbar_thumb_rect = pg.Rect(bar_x - 1, thumb_y, 6, thumb_h)
        else:
            self._table_scrollbar_track_rect = None
            self._table_scrollbar_thumb_rect = None
            self._table_scrollbar_dragging = False

    def _set_table_scroll_from_thumb_mouse(self, my: int) -> None:
        track = self._table_scrollbar_track_rect
        thumb = self._table_scrollbar_thumb_rect
        if track is None or thumb is None:
            return
        total = len(self.rows)
        visible = self._table_visible_rows()
        max_scroll = max(0, total - visible)
        if max_scroll <= 0:
            self.table_scroll = 0.0
            return
        thumb_h = max(1, int(thumb.height))
        travel = max(1, int(track.height) - thumb_h)
        thumb_top = int(my) - int(self._table_scrollbar_drag_offset)
        thumb_top = max(int(track.y), min(int(track.bottom) - thumb_h, int(thumb_top)))
        ratio = float(thumb_top - int(track.y)) / float(travel)
        self.table_scroll = max(0.0, min(float(max_scroll), ratio * float(max_scroll)))

    def _draw_hub_graph(self, rect) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (18, 20, 26), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)

        header_x = rect.x + 10
        copy_reserved = 0
        graph_points = [
            point for point in self.graph_points if self._env_rate_in_view(point.get("x"))
        ]
        graph_points, _ = self._normalize_graph_points_for_display(graph_points, group_key="row_index")
        fit_report = self._fit_report_for_graph_points(graph_points, scope="hub_2d")
        best = fit_report.get("best_model") if isinstance(fit_report, dict) else None
        if best and hr._is_number(best.get("r2")):
            equation = str(best.get("equation", ""))
            copy_reserved = self._draw_equation_copy_button(rect, equation, rect.y + 8, margin=10)
            header_w = max(60, rect.width - 20 - copy_reserved)
            line_1 = hr._fit_text(self.tiny, f"Equation: {equation}", header_w)
            line_2 = hr._fit_text(self.tiny, f"R^2: {float(best.get('r2')):.4f}", header_w)
            col = (235, 210, 146)
        else:
            header_w = max(60, rect.width - 20)
            line_1 = "Equation: not enough data"
            line_2 = "R^2: --"
            col = (170, 170, 170)
        self.screen.blit(self.tiny.render(line_1, True, col), (header_x, rect.y + 8))
        self.screen.blit(self.tiny.render(line_2, True, col), (header_x, rect.y + 24))

        if not graph_points:
            msg = self.small.render("No hub graph points in selected env range.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 12, rect.y + 50))
            self.hub_dot_hits = []
            return

        plot_top = rect.y + 48
        plot = pg.Rect(rect.x + 48, plot_top, rect.width - 64, rect.height - ((plot_top - rect.y) + 24))
        if plot.width <= 24 or plot.height <= 24:
            return
        pg.draw.rect(self.screen, (12, 14, 19), plot)
        pg.draw.rect(self.screen, (60, 66, 78), plot, 1)

        xs = [float(p["x"]) for p in graph_points if hr._is_number(p.get("x"))]
        ys = [float(p["y"]) for p in graph_points if hr._is_number(p.get("y"))]
        fits = [float(p["fitness"]) for p in graph_points if hr._is_number(p.get("fitness"))]
        if not xs or not ys:
            return

        raw_min_x = min(xs)
        raw_max_x = max(xs)
        raw_min_y = min(ys)
        raw_max_y = max(ys)
        min_x, max_x = (raw_min_x, raw_max_x) if raw_max_x > raw_min_x else (raw_min_x - 0.05, raw_max_x + 0.05)
        min_y, max_y = (raw_min_y, raw_max_y) if raw_max_y > raw_min_y else (raw_min_y - 0.05, raw_max_y + 0.05)
        if raw_max_x > raw_min_x:
            pad = (raw_max_x - raw_min_x) * 0.04
            min_x -= pad
            max_x += pad
        if raw_max_y > raw_min_y:
            pad = (raw_max_y - raw_min_y) * 0.04
            min_y -= pad
            max_y += pad
        fit_min = min(fits) if fits else 0.0
        fit_max = max(fits) if fits else 1.0
        fit_den = max(1e-9, fit_max - fit_min)

        def _to_px(xv: float, yv: float) -> tuple[int, int]:
            px = plot.x + int(((xv - min_x) / (max_x - min_x)) * plot.width)
            py = plot.y + plot.height - int(((yv - min_y) / (max_y - min_y)) * plot.height)
            return px, py

        fit_segments = []
        if best:
            sample = []
            y_span = max(1e-9, max_y - min_y)
            for idx in range(220):
                xv = min_x + ((max_x - min_x) * float(idx) / 219.0)
                yv = hr._eval_hub_model(best, xv)
                if not hr._is_number(yv):
                    continue
                y_float = float(yv)
                if y_float < (min_y - (2.0 * y_span)) or y_float > (max_y + (2.0 * y_span)):
                    continue
                sample.append((xv, y_float))
            if len(sample) >= 2:
                for i in range(1, len(sample)):
                    fit_segments.append((sample[i - 1], sample[i]))

        self.hub_dot_hits = []
        selected_idx = int(self.selected_row_index) if self.selected_row_index is not None else -1
        for point in graph_points:
            px, py = _to_px(float(point["x"]), float(point["y"]))
            n = max(0.0, min(1.0, (float(point["fitness"]) - fit_min) / fit_den))
            radius = max(1, int(round(4 * n)))
            color = (35 + int(220 * n), 128 + int(95 * n), 236 - int(166 * n))
            if int(point.get("row_index", -1)) == selected_idx:
                pg.draw.circle(self.screen, (255, 255, 255), (px, py), radius + 3, 1)
            pg.draw.circle(self.screen, color, (px, py), radius)
            self.hub_dot_hits.append(
                {
                    "px": px,
                    "py": py,
                    "radius": max(6, radius + 4),
                    "row_index": int(point.get("row_index", -1)),
                }
            )
        for start, end in fit_segments:
            pg.draw.line(self.screen, (248, 196, 92), _to_px(start[0], start[1]), _to_px(end[0], end[1]), 2)

        min_y_txt = self.tiny.render(f"{raw_min_y:.3f}", True, (150, 150, 150))
        max_y_txt = self.tiny.render(f"{raw_max_y:.3f}", True, (150, 150, 150))
        min_x_txt = self.tiny.render(f"{raw_min_x:.3f}", True, (150, 150, 150))
        max_x_txt = self.tiny.render(f"{raw_max_x:.3f}", True, (150, 150, 150))
        x_lab = self.tiny.render("env change rate", True, (155, 155, 155))
        y_lab = self.tiny.render("evo speed", True, (155, 155, 155))
        self.screen.blit(max_y_txt, (plot.x - 40, plot.y - 2))
        self.screen.blit(min_y_txt, (plot.x - 40, plot.bottom - 14))
        self.screen.blit(min_x_txt, (plot.x, plot.bottom + 3))
        self.screen.blit(max_x_txt, (plot.right - max_x_txt.get_width(), plot.bottom + 3))
        self.screen.blit(x_lab, (plot.x + 6, plot.bottom + 18))
        self.screen.blit(y_lab, (plot.x - 42, plot.y + 8))

    def _draw_hub_graph_3d(
        self,
        rect,
        axis_keys: tuple[str, str, str] = ("x", "y", "fitness"),
        axis_labels: tuple[str, str, str] = ("env rate", "evo speed", "fitness"),
        mapping_text: str = "x=env rate, y=evo speed, z=fitness",
        source_points: list[dict] | None = None,
        best_model: dict | None = None,
        fit_scope_label: str = "Hub fit",
        draw_best_fit_line: bool = False,
        draw_bell_curves: bool = False,
        bell_curve_rows: list[dict] | None = None,
        draw_points: bool = True,
        show_hub_equation: bool = True,
        title_text: str = "Hub 3D",
    ) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (18, 20, 26), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)

        key_x, key_y, key_z = axis_keys
        label_x, label_y, label_z = axis_labels
        header_x = rect.x + 10
        copy_reserved = 0
        if isinstance(source_points, list):
            points_source = source_points
        else:
            points_source = self.graph_points
        graph_points = [
            point for point in points_source if self._env_rate_in_view(point.get("x"))
        ]
        graph_points, display_fit_bounds = self._normalize_graph_points_for_display(
            graph_points,
            group_key="row_index",
        )
        best = best_model
        if best is None:
            fit_report = self._fit_report_for_graph_points(graph_points, scope="hub_3d")
            best = fit_report.get("best_model") if isinstance(fit_report, dict) else None
        fit_axes = (key_x, key_y) == ("x", "y")
        has_fit_equation = fit_axes and bool(best) and hr._is_number(best.get("r2"))
        if has_fit_equation:
            equation = str(best.get("equation", "")).strip()
            copy_reserved = self._draw_equation_copy_button(rect, equation, rect.y + 8, margin=10)
        if show_hub_equation and has_fit_equation:
            header_w = max(60, rect.width - 20 - copy_reserved)
            fit_line = hr._fit_text(
                self.tiny,
                f"{fit_scope_label}: {equation} | R^2: {float(best.get('r2')):.4f}",
                header_w,
            )
            col = (235, 210, 146)
        else:
            header_w = max(60, rect.width - 20 - copy_reserved)
            if show_hub_equation and fit_axes:
                fit_line = "Equation: not enough data"
            elif fit_axes:
                fit_line = "Equation hidden (use Copy button)"
            else:
                fit_line = "Remapped 3D view (2D fit disabled)"
            col = (170, 170, 170)

        def _wrap_line(text: str, max_w: int) -> list[str]:
            raw = str(text).strip()
            if (not raw) or max_w <= 8:
                return [raw]
            lines = []
            remaining = raw
            while remaining:
                if self.tiny.size(remaining)[0] <= max_w:
                    lines.append(remaining)
                    break
                cut = len(remaining)
                while cut > 1 and self.tiny.size(remaining[:cut])[0] > max_w:
                    cut -= 1
                split = remaining.rfind(" ", 0, cut)
                if split <= 0:
                    split = cut
                chunk = remaining[:split].rstrip()
                if chunk:
                    lines.append(chunk)
                remaining = remaining[split:].lstrip()
            return lines if lines else [raw]

        header_items = [
            (str(title_text), (210, 228, 252)),
            (f"Axes: {mapping_text}", (190, 206, 230)),
            (str(fit_line), col),
            ("Drag to rotate | wheel or +/- to zoom", (178, 188, 205)),
        ]
        if draw_bell_curves:
            header_items.insert(3, ("Bell curves: per-master stitched gaussian fits", (168, 226, 196)))
        if self.normalize_display:
            header_items.insert(3, (self._normalize_mode_description(), (176, 214, 244)))
        header_y = rect.y + 8
        line_h = max(12, int(self.tiny.get_linesize()))
        line_gap = 2
        for text, color in header_items:
            wrapped = _wrap_line(text, header_w)
            for line in wrapped:
                self.screen.blit(self.tiny.render(line, True, color), (header_x, int(header_y)))
                header_y += line_h + line_gap
        plot_top = int(header_y) + 6

        if not graph_points:
            msg = self.small.render("No hub graph points in selected env range.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 12, plot_top + 2))
            self.hub_dot_hits = []
            return

        plot = pg.Rect(rect.x + 30, plot_top, rect.width - 44, rect.height - ((plot_top - rect.y) + 22))
        if plot.width <= 24 or plot.height <= 24:
            return
        pg.draw.rect(self.screen, (12, 14, 19), plot)
        pg.draw.rect(self.screen, (60, 66, 78), plot, 1)

        xs = [float(p.get(key_x)) for p in graph_points if hr._is_number(p.get(key_x))]
        ys = [float(p.get(key_y)) for p in graph_points if hr._is_number(p.get(key_y))]
        zs = [float(p.get(key_z)) for p in graph_points if hr._is_number(p.get(key_z))]
        if not xs or not ys or not zs:
            self.hub_dot_hits = []
            return
        fit_values = [float(p.get("fitness")) for p in graph_points if hr._is_number(p.get("fitness"))]
        if not fit_values:
            fit_values = list(ys)

        min_x = min(xs)
        max_x = max(xs)
        min_y = min(ys)
        max_y = max(ys)
        min_z = min(zs)
        max_z = max(zs)
        den_x = max(1e-9, max_x - min_x)
        den_y = max(1e-9, max_y - min_y)
        den_z = max(1e-9, max_z - min_z)
        fit_min = min(fit_values)
        fit_max = max(fit_values)
        fit_den = max(1e-9, fit_max - fit_min)

        def _norm(val: float, low: float, den: float) -> float:
            return ((float(val) - float(low)) / float(den)) * 2.0 - 1.0

        yaw = float(self._graph3d_yaw)
        pitch = float(self._graph3d_pitch)
        cos_y = math.cos(yaw)
        sin_y = math.sin(yaw)
        cos_p = math.cos(pitch)
        sin_p = math.sin(pitch)
        cam_dist = 3.4
        zoom = float(self._graph3d_zoom)
        scale = float(min(plot.width, plot.height)) * 0.40 * zoom
        center_x = plot.x + (plot.width / 2.0)
        center_y = plot.y + (plot.height / 2.0)

        def _project_3d(xv: float, yv: float, zv: float):
            x1 = (xv * cos_y) + (zv * sin_y)
            z1 = (-xv * sin_y) + (zv * cos_y)
            y2 = (yv * cos_p) - (z1 * sin_p)
            z2 = (yv * sin_p) + (z1 * cos_p)
            denom = cam_dist - z2
            if denom <= 0.2:
                return None
            perspective = cam_dist / denom
            px = int(round(center_x + (x1 * scale * perspective)))
            py = int(round(center_y - (y2 * scale * perspective)))
            return (px, py, z2, perspective)

        fit_line_segments = []
        if draw_best_fit_line and isinstance(best, dict):
            y_span = max(1e-9, max_y - min_y)

            def _fit_line_z_for_x(x_val: float) -> float | None:
                weighted = []
                for gp in graph_points:
                    if not (hr._is_number(gp.get("x")) and hr._is_number(gp.get(key_z))):
                        continue
                    gx = float(gp.get("x"))
                    gz = float(gp.get(key_z))
                    dist = abs(gx - float(x_val))
                    weight = 1.0 / max(1e-6, 0.02 + dist)
                    weighted.append((weight, gz))
                if not weighted:
                    return None
                weighted.sort(key=lambda t: t[0], reverse=True)
                top = weighted[: min(12, len(weighted))]
                den = sum(w for w, _ in top)
                if den <= 1e-12:
                    return None
                return sum((w * z) for w, z in top) / den

            fit_projected = []
            for idx in range(220):
                xv = min_x + ((max_x - min_x) * float(idx) / 219.0)
                yv = hr._eval_hub_model(best, xv)
                if not hr._is_number(yv):
                    continue
                y_float = float(yv)
                if y_float < (min_y - (2.0 * y_span)) or y_float > (max_y + (2.0 * y_span)):
                    continue
                z_float = _fit_line_z_for_x(xv)
                if not hr._is_number(z_float):
                    continue
                nx = _norm(float(xv), min_x, den_x)
                ny = _norm(y_float, min_y, den_y)
                nz = _norm(float(z_float), min_z, den_z)
                pr = _project_3d(nx, ny, nz)
                if pr is None:
                    continue
                fit_projected.append(pr)
            if len(fit_projected) >= 2:
                for idx in range(1, len(fit_projected)):
                    fit_line_segments.append((fit_projected[idx - 1], fit_projected[idx]))

        bell_curve_segments = []
        if (
            draw_bell_curves
            and isinstance(bell_curve_rows, list)
            and (key_x, key_y, key_z) == ("x", "y", "fitness")
        ):
            z_span = max(1e-9, max_z - min_z)
            for row_fit in bell_curve_rows:
                if not isinstance(row_fit, dict):
                    continue
                env_rate = row_fit.get("env_rate")
                fit = row_fit.get("fit")
                if (not hr._is_number(env_rate)) or (not isinstance(fit, dict)):
                    continue
                row_idx = row_fit.get("row_index")
                apex_x = fit.get("apex_x")
                apex_y = fit.get("apex_y")
                sigma_left = fit.get("sigma_left")
                sigma_right = fit.get("sigma_right")
                if not (
                    hr._is_number(apex_x)
                    and hr._is_number(apex_y)
                    and hr._is_number(sigma_left)
                    and hr._is_number(sigma_right)
                ):
                    continue
                env_float = float(env_rate)
                projected_curve = []
                for idx in range(180):
                    evo_val = min_y + ((max_y - min_y) * float(idx) / 179.0)
                    fit_val = hr._predict_piecewise_gaussian(
                        float(evo_val),
                        float(apex_x),
                        float(apex_y),
                        float(sigma_left),
                        float(sigma_right),
                    )
                    if not hr._is_number(fit_val):
                        continue
                    fit_float = float(fit_val)
                    if self.normalize_display:
                        fit_float = self._normalize_value_for_display(
                            fit_float,
                            display_fit_bounds.get(row_idx),
                        )
                    if fit_float < (min_z - (2.0 * z_span)) or fit_float > (max_z + (2.0 * z_span)):
                        continue
                    nx = _norm(env_float, min_x, den_x)
                    ny = _norm(evo_val, min_y, den_y)
                    nz = _norm(fit_float, min_z, den_z)
                    pr = _project_3d(nx, ny, nz)
                    if pr is not None:
                        projected_curve.append(pr)
                if len(projected_curve) < 2:
                    continue
                curve_color = self._env_color(env_float, min_x, max_x)
                for idx in range(1, len(projected_curve)):
                    p0 = projected_curve[idx - 1]
                    p1 = projected_curve[idx]
                    depth = (float(p0[2]) + float(p1[2])) * 0.5
                    bell_curve_segments.append(
                        {
                            "p0": p0,
                            "p1": p1,
                            "depth": depth,
                            "color": curve_color,
                        }
                    )

        # Floor grid for depth cues.
        for t in (-0.5, 0.0, 0.5):
            a = _project_3d(-1.0, -1.0, t)
            b = _project_3d(1.0, -1.0, t)
            c = _project_3d(t, -1.0, -1.0)
            d = _project_3d(t, -1.0, 1.0)
            if a is not None and b is not None:
                pg.draw.line(self.screen, (40, 44, 54), (a[0], a[1]), (b[0], b[1]), 1)
            if c is not None and d is not None:
                pg.draw.line(self.screen, (40, 44, 54), (c[0], c[1]), (d[0], d[1]), 1)

        axis_origin = _project_3d(-1.0, -1.0, -1.0)
        axis_x = _project_3d(1.0, -1.0, -1.0)
        axis_y = _project_3d(-1.0, 1.0, -1.0)
        axis_z = _project_3d(-1.0, -1.0, 1.0)
        if axis_origin is not None:
            if axis_x is not None:
                pg.draw.line(self.screen, (128, 210, 255), (axis_origin[0], axis_origin[1]), (axis_x[0], axis_x[1]), 2)
                x_label = self.tiny.render(f"x: {label_x}", True, (128, 210, 255))
                self.screen.blit(x_label, (axis_x[0] + 5, axis_x[1] - 10))
            if axis_y is not None:
                pg.draw.line(self.screen, (168, 235, 176), (axis_origin[0], axis_origin[1]), (axis_y[0], axis_y[1]), 2)
                y_label = self.tiny.render(f"y: {label_y}", True, (168, 235, 176))
                self.screen.blit(y_label, (axis_y[0] + 5, axis_y[1] - 10))
            if axis_z is not None:
                pg.draw.line(self.screen, (255, 208, 136), (axis_origin[0], axis_origin[1]), (axis_z[0], axis_z[1]), 2)
                z_label = self.tiny.render(f"z: {label_z}", True, (255, 208, 136))
                self.screen.blit(z_label, (axis_z[0] + 5, axis_z[1] - 10))

        projected = []
        if draw_points:
            selected_idx = int(self.selected_row_index) if self.selected_row_index is not None else -1
            for item in graph_points:
                if not (
                    hr._is_number(item.get(key_x))
                    and hr._is_number(item.get(key_y))
                    and hr._is_number(item.get(key_z))
                ):
                    continue
                nx = _norm(float(item.get(key_x)), min_x, den_x)
                ny = _norm(float(item.get(key_y)), min_y, den_y)
                nz = _norm(float(item.get(key_z)), min_z, den_z)
                pr = _project_3d(nx, ny, nz)
                if pr is None:
                    continue
                px, py, depth, perspective = pr
                if not plot.inflate(40, 40).collidepoint(px, py):
                    continue
                if hr._is_number(item.get("fitness")):
                    fitness_val = float(item.get("fitness"))
                else:
                    fitness_val = float(item.get(key_y))
                fitness_norm = max(0.0, min(1.0, (fitness_val - fit_min) / fit_den))
                base_radius = 2.0 + (4.0 * fitness_norm)
                radius = max(1, min(12, int(round(base_radius * max(0.65, min(2.2, perspective))))))
                color = (35 + int(220 * fitness_norm), 128 + int(95 * fitness_norm), 236 - int(166 * fitness_norm))
                projected.append(
                    {
                        "depth": float(depth),
                        "px": int(px),
                        "py": int(py),
                        "radius": int(radius),
                        "color": color,
                        "row_index": int(item.get("row_index", -1)),
                    }
                )

        projected.sort(key=lambda d: float(d["depth"]))
        bell_curve_segments.sort(key=lambda d: float(d["depth"]))
        for seg in bell_curve_segments:
            p0 = seg["p0"]
            p1 = seg["p1"]
            c = seg["color"]
            pg.draw.line(
                self.screen,
                (int(c[0]), int(c[1]), int(c[2])),
                (int(p0[0]), int(p0[1])),
                (int(p1[0]), int(p1[1])),
                2,
            )
            pg.draw.line(
                self.screen,
                (255, 255, 255),
                (int(p0[0]), int(p0[1])),
                (int(p1[0]), int(p1[1])),
                1,
            )
        for p0, p1 in fit_line_segments:
            pg.draw.line(
                self.screen,
                (248, 196, 92),
                (int(p0[0]), int(p0[1])),
                (int(p1[0]), int(p1[1])),
                3,
            )
            pg.draw.line(
                self.screen,
                (255, 235, 178),
                (int(p0[0]), int(p0[1])),
                (int(p1[0]), int(p1[1])),
                1,
            )
        self.hub_dot_hits = []
        if draw_points:
            selected_idx = int(self.selected_row_index) if self.selected_row_index is not None else -1
            for dot in projected:
                px = int(dot["px"])
                py = int(dot["py"])
                radius = int(dot["radius"])
                if int(dot["row_index"]) == selected_idx:
                    pg.draw.circle(self.screen, (255, 255, 255), (px, py), radius + 3, 1)
                pg.draw.circle(self.screen, dot["color"], (px, py), radius)
                self.hub_dot_hits.append(
                    {
                        "px": px,
                        "py": py,
                        "radius": max(6, radius + 4),
                        "row_index": int(dot["row_index"]),
                    }
                )

        range_text = (
            f"x({label_x}) {min_x:.3f}..{max_x:.3f}   "
            f"y({label_y}) {min_y:.3f}..{max_y:.3f}   "
            f"z({label_z}) {min_z:.3f}..{max_z:.3f}   "
            f"zoom {zoom:.2f}x"
        )
        self.screen.blit(
            self.tiny.render(hr._fit_text(self.tiny, range_text, rect.width - 20), True, (160, 168, 184)),
            (rect.x + 10, rect.bottom - 16),
        )

    def _draw_selected_scatter(self, rect, row: dict | None) -> None:
        pg = self.pg
        self._selected_scatter_plot_rect = None
        self._selected_scatter_point_hits = []
        pg.draw.rect(self.screen, (17, 19, 24), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)
        if not isinstance(row, dict):
            msg = self.small.render("No selected simulation.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 10, rect.y + 8))
            return

        step_label = int(row.get("step_index", 0)) + 1
        rate_label = row.get("env_rate")
        master_label = row.get("master_run_num")
        title = f"Selected Sim: step {step_label} | rate {float(rate_label):.2f}" if hr._is_number(rate_label) else f"Selected Sim: step {step_label}"
        if master_label is not None:
            title += f" | master_{int(master_label)}"
        self.screen.blit(self.tiny.render(hr._fit_text(self.tiny, title, rect.width - 20), True, (195, 210, 235)), (rect.x + 10, rect.y + 8))

        row_idx = _safe_int(row.get("step_index"))
        row_loaded = bool(row.get("_full_loaded"))
        if not row_loaded:
            if (
                row_idx is not None
                and self._selected_row_loading_idx is not None
                and int(row_idx) == int(self._selected_row_loading_idx)
            ):
                text = self._selected_row_loading_status or "Loading selected master points..."
                text = f"{text}  {self._selected_row_eta_text()}"
            else:
                text = "Waiting for selected master points..."
            msg = self.small.render(hr._fit_text(self.small, text, rect.width - 20), True, (182, 198, 230))
            self.screen.blit(msg, (rect.x + 10, rect.y + 30))
            return

        raw_points = []
        raw_points_src = row.get("points")
        if isinstance(raw_points_src, list):
            for pair in raw_points_src:
                if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                    continue
                if hr._is_number(pair[0]) and hr._is_number(pair[1]):
                    raw_points.append((float(pair[0]), float(pair[1])))
        if not raw_points:
            if (
                row_idx is not None
                and self._selected_row_loading_idx is not None
                and int(row_idx) == int(self._selected_row_loading_idx)
            ):
                text = self._selected_row_loading_status or "Loading selected master points..."
                text = f"{text}  {self._selected_row_eta_text()}"
                msg = self.small.render(hr._fit_text(self.small, text, rect.width - 20), True, (182, 198, 230))
            else:
                msg = self.small.render("No points available.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 10, rect.y + 30))
            return

        points = list(raw_points)
        fit_norm_context = None
        if self.normalize_display and points:
            vals = [float(p[1]) for p in points]
            fit_norm_context = self._normalization_context_for_values(vals)
            points = [
                (float(xv), self._normalize_value_for_display(float(yv), fit_norm_context))
                for xv, yv in points
            ]

        plot = pg.Rect(rect.x + 38, rect.y + 30, rect.width - 50, rect.height - 66)
        if plot.width <= 24 or plot.height <= 24:
            return
        self._selected_scatter_plot_rect = plot
        pg.draw.rect(self.screen, (12, 14, 19), plot)
        pg.draw.rect(self.screen, (60, 66, 78), plot, 1)

        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        raw_min_x = min(xs)
        raw_max_x = max(xs)
        raw_min_y = min(ys)
        raw_max_y = max(ys)
        min_x, max_x = (raw_min_x, raw_max_x) if raw_max_x > raw_min_x else (raw_min_x - 0.05, raw_max_x + 0.05)
        min_y, max_y = (raw_min_y, raw_max_y) if raw_max_y > raw_min_y else (raw_min_y - 0.05, raw_max_y + 0.05)
        if raw_max_x > raw_min_x:
            pad = (raw_max_x - raw_min_x) * 0.04
            min_x -= pad
            max_x += pad
        if raw_max_y > raw_min_y:
            pad = (raw_max_y - raw_min_y) * 0.04
            min_y -= pad
            max_y += pad

        def _to_px(xv: float, yv: float) -> tuple[int, int]:
            px = plot.x + int(((xv - min_x) / (max_x - min_x)) * plot.width)
            py = plot.y + plot.height - int(((yv - min_y) / (max_y - min_y)) * plot.height)
            return px, py

        fit_segments = []
        fit = row.get("fit")
        if isinstance(fit, dict):
            apex_x = fit.get("apex_x")
            apex_y = fit.get("apex_y")
            sigma_left = fit.get("sigma_left")
            sigma_right = fit.get("sigma_right")
            if (
                hr._is_number(apex_x)
                and hr._is_number(apex_y)
                and hr._is_number(sigma_left)
                and hr._is_number(sigma_right)
            ):
                curve = []
                for i in range(180):
                    xv = min_x + ((max_x - min_x) * float(i) / 179.0)
                    yv = hr._predict_piecewise_gaussian(
                        float(xv),
                        float(apex_x),
                        float(apex_y),
                        float(sigma_left),
                        float(sigma_right),
                    )
                    if hr._is_number(yv):
                        curve.append(
                            (
                                float(xv),
                                self._normalize_value_for_display(float(yv), fit_norm_context),
                            )
                        )
                if len(curve) >= 2:
                    for i in range(1, len(curve)):
                        fit_segments.append((curve[i - 1], curve[i]))

        selected_point = self._selected_scatter_selected if isinstance(self._selected_scatter_selected, dict) else None
        selected_row_idx = _safe_int(selected_point.get("row_idx")) if isinstance(selected_point, dict) else None
        selected_point_idx = _safe_int(selected_point.get("point_index")) if isinstance(selected_point, dict) else None
        y_min = min(ys)
        y_max = max(ys)
        den = max(1e-9, y_max - y_min)
        for point_idx, ((raw_x, raw_y), (x_val, y_val)) in enumerate(zip(raw_points, points)):
            n = max(0.0, min(1.0, (y_val - y_min) / den))
            radius = 1 + int(round(3 * n))
            color = (42 + int(210 * n), 128 + int(95 * n), 236 - int(166 * n))
            px, py = _to_px(x_val, y_val)
            pg.draw.circle(self.screen, color, (px, py), radius)
            is_selected = (
                row_idx is not None
                and selected_row_idx is not None
                and int(selected_row_idx) == int(row_idx)
                and selected_point_idx is not None
                and int(selected_point_idx) == int(point_idx)
            )
            if is_selected:
                pg.draw.circle(self.screen, (255, 255, 255), (px, py), radius + 3, 1)
            self._selected_scatter_point_hits.append(
                {
                    "px": int(px),
                    "py": int(py),
                    "hit_radius": max(6, int(radius) + 4),
                    "row_idx": row_idx,
                    "point_index": int(point_idx),
                    "x": float(raw_x),
                    "y": float(raw_y),
                    "display_y": float(y_val),
                }
            )
        for start, end in fit_segments:
            pg.draw.line(self.screen, (248, 196, 92), _to_px(start[0], start[1]), _to_px(end[0], end[1]), 2)

        self.screen.blit(self.tiny.render(f"{raw_max_y:.3f}", True, (150, 150, 150)), (plot.x - 34, plot.y - 2))
        self.screen.blit(self.tiny.render(f"{raw_min_y:.3f}", True, (150, 150, 150)), (plot.x - 34, plot.bottom - 14))
        fit_label = f"fitness ({self._normalize_mode_label().lower()})" if self.normalize_display else "fitness"
        self.screen.blit(self.tiny.render(fit_label, True, (155, 155, 155)), (plot.x - 34, plot.y + 14))
        self.screen.blit(self.tiny.render("evo speed", True, (155, 155, 155)), (plot.x + 5, plot.y + 4))
        coord_text = "Click a point to show coordinates."
        coord_color = (146, 156, 172)
        if (
            isinstance(selected_point, dict)
            and row_idx is not None
            and selected_row_idx is not None
            and int(selected_row_idx) == int(row_idx)
            and hr._is_number(selected_point.get("x"))
            and hr._is_number(selected_point.get("y"))
        ):
            x_val = float(selected_point.get("x"))
            y_val = float(selected_point.get("y"))
            coord_text = f"Clicked point: evo={x_val:.6g}, fitness={y_val:.6g}"
            if self.normalize_display and hr._is_number(selected_point.get("display_y")):
                coord_text += f" (display y={float(selected_point.get('display_y')):.6g})"
            coord_color = (196, 214, 236)
        coord_text = hr._fit_text(self.tiny, coord_text, max(24, plot.width - 4))
        self.screen.blit(
            self.tiny.render(coord_text, True, coord_color),
            (plot.x + 2, plot.bottom + 8),
        )

    def _handle_selected_scatter_click(self, mx: int, my: int) -> bool:
        plot = self._selected_scatter_plot_rect
        if plot is None or (not plot.collidepoint(int(mx), int(my))):
            return False
        best_hit = None
        best_dist_sq = None
        for hit in self._selected_scatter_point_hits:
            if not isinstance(hit, dict):
                continue
            px = _safe_int(hit.get("px"))
            py = _safe_int(hit.get("py"))
            hit_radius = _safe_float(hit.get("hit_radius"))
            if px is None or py is None or hit_radius is None or hit_radius <= 0:
                continue
            dx = float(int(mx) - int(px))
            dy = float(int(my) - int(py))
            dist_sq = (dx * dx) + (dy * dy)
            if dist_sq > (float(hit_radius) * float(hit_radius)):
                continue
            if best_dist_sq is None or dist_sq < float(best_dist_sq):
                best_hit = hit
                best_dist_sq = float(dist_sq)
        if isinstance(best_hit, dict):
            self._selected_scatter_selected = {
                "row_idx": _safe_int(best_hit.get("row_idx")),
                "point_index": _safe_int(best_hit.get("point_index")),
                "x": _safe_float(best_hit.get("x")),
                "y": _safe_float(best_hit.get("y")),
                "display_y": _safe_float(best_hit.get("display_y")),
            }
        else:
            self._selected_scatter_selected = None
        return True

    def _draw_timeline(self, rect, row: dict | None) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (17, 19, 24), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)
        self.screen.blit(
            self.tiny.render(
                "Timeline (normalized 0%-100%; per-sim lines + master average)",
                True,
                (190, 206, 230),
            ),
            (rect.x + 10, rect.y + 8),
        )
        if not isinstance(row, dict):
            msg = self.small.render("No selected simulation.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 10, rect.y + 28))
            return

        payload = self._timeline_payload_for_row(row)
        series = payload.get("series") if isinstance(payload, dict) else None
        fit = payload.get("fit") if isinstance(payload, dict) else None
        master_points = payload.get("master_points") if isinstance(payload, dict) else None
        master_fit = payload.get("master_fit") if isinstance(payload, dict) else None
        if not isinstance(series, list) or not series:
            msg = self.small.render("No timeline data found for this simulation.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 10, rect.y + 28))
            return
        if self.normalize_display:
            normalized_series = []
            for item in series:
                if not isinstance(item, dict):
                    continue
                pts = item.get("points")
                if not isinstance(pts, list):
                    continue
                numeric_pts = []
                for pair in pts:
                    if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                        continue
                    xv, yv = pair[0], pair[1]
                    if hr._is_number(xv) and hr._is_number(yv):
                        numeric_pts.append((float(xv), float(yv)))
                if not numeric_pts:
                    continue
                y_vals = [p[1] for p in numeric_pts]
                fit_norm_context = self._normalization_context_for_values(y_vals)
                norm_pts = [
                    (float(xv), self._normalize_value_for_display(float(yv), fit_norm_context))
                    for xv, yv in numeric_pts
                ]
                norm_item = dict(item)
                norm_item["points"] = norm_pts
                normalized_series.append(norm_item)
            series = normalized_series
            master_points = _timeline_master_average(series)
            all_series_points = []
            for item in series:
                if isinstance(item, dict) and isinstance(item.get("points"), list):
                    all_series_points.extend(item.get("points"))
            fit = _timeline_fit(all_series_points)
            master_fit = _timeline_fit(master_points)
            if not series:
                msg = self.small.render("No timeline data found for this simulation.", True, (170, 170, 170))
                self.screen.blit(msg, (rect.x + 10, rect.y + 28))
                return
        series_max_len = max(
            [len(item.get("points", [])) for item in series if isinstance(item, dict) and isinstance(item.get("points"), list)]
            or [2]
        )
        master_len = len(master_points) if isinstance(master_points, list) else 0
        self.timeline_frame_count = max(2, int(max(master_len, series_max_len)))

        plot = pg.Rect(rect.x + 44, rect.y + 28, rect.width - 58, rect.height - 44)
        if plot.width <= 24 or plot.height <= 24:
            return
        pg.draw.rect(self.screen, (12, 14, 19), plot)
        pg.draw.rect(self.screen, (60, 66, 78), plot, 1)

        all_points = []
        for item in series:
            pts = item.get("points", []) if isinstance(item, dict) else []
            for point in pts:
                if isinstance(point, (tuple, list)) and len(point) >= 2 and hr._is_number(point[1]):
                    all_points.append((float(point[0]), float(point[1])))
        if not all_points:
            return

        min_x = 0.0
        max_x = 1.0
        ys = [p[1] for p in all_points]
        raw_min_y = min(ys)
        raw_max_y = max(ys)
        min_y, max_y = (raw_min_y, raw_max_y) if raw_max_y > raw_min_y else (raw_min_y - 0.05, raw_max_y + 0.05)
        if raw_max_y > raw_min_y:
            pad = (raw_max_y - raw_min_y) * 0.06
            min_y -= pad
            max_y += pad

        def _to_px(xv: float, yv: float) -> tuple[int, int]:
            px = plot.x + int(((xv - min_x) / (max_x - min_x)) * plot.width)
            py = plot.y + plot.height - int(((yv - min_y) / (max_y - min_y)) * plot.height)
            return px, py

        cursor_norm = max(0.0, min(1.0, float(self.timeline_progress)))
        cursor_px = plot.x + int(cursor_norm * plot.width)
        pg.draw.line(self.screen, (110, 118, 134), (cursor_px, plot.y), (cursor_px, plot.bottom), 1)
        frame_idx = self._timeline_frame_index() + 1
        frame_total = max(2, int(self.timeline_frame_count))
        frame_lbl = self.tiny.render(f"frame {frame_idx}/{frame_total}", True, (170, 182, 202))
        self.screen.blit(frame_lbl, (rect.right - frame_lbl.get_width() - 10, rect.y + 10))

        for item in series:
            if not isinstance(item, dict):
                continue
            pts = item.get("points", [])
            color = item.get("color", (180, 180, 180))
            run_num = item.get("run_num")
            if not isinstance(pts, list) or len(pts) < 1:
                continue
            scaled = []
            for p in pts:
                if not isinstance(p, (tuple, list)) or len(p) < 2:
                    continue
                if not (hr._is_number(p[0]) and hr._is_number(p[1])):
                    continue
                scaled.append(_to_px(float(p[0]), float(p[1])))
            if len(scaled) >= 2:
                pg.draw.lines(self.screen, color, False, scaled, 2)
            for px, py in scaled:
                pg.draw.circle(self.screen, color, (px, py), 2)
            marker_y = _timeline_interp_y(pts, cursor_norm)
            if hr._is_number(marker_y):
                mpx, mpy = _to_px(cursor_norm, float(marker_y))
                pg.draw.circle(self.screen, color, (mpx, mpy), 3)
            if scaled:
                label = self.tiny.render(f"{run_num}", True, color)
                self.screen.blit(label, (scaled[-1][0] + 4, scaled[-1][1] - 6))

        if isinstance(master_points, list) and len(master_points) >= 2:
            master_scaled = []
            for p in master_points:
                if not isinstance(p, (tuple, list)) or len(p) < 2:
                    continue
                if not (hr._is_number(p[0]) and hr._is_number(p[1])):
                    continue
                master_scaled.append(_to_px(float(p[0]), float(p[1])))
            if len(master_scaled) >= 2:
                pg.draw.lines(self.screen, (235, 235, 235), False, master_scaled, 3)
                pg.draw.circle(self.screen, (235, 235, 235), master_scaled[-1], 3)
                master_tag = self.tiny.render("master avg", True, (235, 235, 235))
                self.screen.blit(master_tag, (master_scaled[-1][0] + 6, master_scaled[-1][1] + 2))
                master_y = _timeline_interp_y(master_points, cursor_norm)
                if hr._is_number(master_y):
                    mpx, mpy = _to_px(cursor_norm, float(master_y))
                    pg.draw.circle(self.screen, (245, 245, 245), (mpx, mpy), 5, 1)
                    val_txt = self.tiny.render(f"avg {float(master_y):.3f}", True, (235, 235, 235))
                    self.screen.blit(val_txt, (mpx + 6, mpy - 14))

        if isinstance(fit, dict) and hr._is_number(fit.get("slope")) and hr._is_number(fit.get("intercept")):
            slope = float(fit["slope"])
            intercept = float(fit["intercept"])
            p0 = _to_px(0.0, intercept)
            p1 = _to_px(1.0, slope + intercept)
            pg.draw.line(self.screen, (248, 196, 92), p0, p1, 2)
            eq = str(fit.get("equation", ""))
            r2 = fit.get("r2")
            r2_text = f"{float(r2):.4f}" if hr._is_number(r2) else "--"
            copy_reserved = self._draw_equation_copy_button(rect, eq, rect.y + 10, margin=10)
            text = hr._fit_text(
                self.tiny,
                f"Sim fit: {eq} | R^2={r2_text}",
                max(40, rect.width - 20 - copy_reserved),
            )
            self.screen.blit(self.tiny.render(text, True, (235, 210, 146)), (rect.x + 10, rect.y + 10))

        if isinstance(master_fit, dict) and hr._is_number(master_fit.get("slope")) and hr._is_number(master_fit.get("intercept")):
            slope = float(master_fit["slope"])
            intercept = float(master_fit["intercept"])
            p0 = _to_px(0.0, intercept)
            p1 = _to_px(1.0, slope + intercept)
            pg.draw.line(self.screen, (168, 246, 232), p0, p1, 2)
            eq = str(master_fit.get("equation", ""))
            r2 = master_fit.get("r2")
            r2_text = f"{float(r2):.4f}" if hr._is_number(r2) else "--"
            copy_reserved = self._draw_equation_copy_button(rect, eq, rect.y + 24, margin=10)
            text = hr._fit_text(
                self.tiny,
                f"Master fit: {eq} | R^2={r2_text}",
                max(40, rect.width - 20 - copy_reserved),
            )
            self.screen.blit(self.tiny.render(text, True, (168, 246, 232)), (rect.x + 10, rect.y + 24))

        self.screen.blit(self.tiny.render("0%", True, (150, 150, 150)), (plot.x - 6, plot.bottom + 2))
        end_label = self.tiny.render("100%", True, (150, 150, 150))
        self.screen.blit(end_label, (plot.right - end_label.get_width(), plot.bottom + 2))
        self.screen.blit(self.tiny.render("normalized timeline", True, (155, 155, 155)), (plot.x + 4, plot.y + 4))
        self.screen.blit(self.tiny.render(f"{raw_max_y:.3f}", True, (150, 150, 150)), (plot.x - 38, plot.y - 2))
        self.screen.blit(self.tiny.render(f"{raw_min_y:.3f}", True, (150, 150, 150)), (plot.x - 38, plot.bottom - 14))

    def _draw_hub_timeline(self, rect) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (17, 19, 24), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)
        self.screen.blit(
            self.tiny.render(
                "Hub Timeline (normal hub graph evolving over timeline frames)",
                True,
                (190, 206, 230),
            ),
            (rect.x + 10, rect.y + 8),
        )

        cached_timeline_rows, total_timeline_rows = self._timeline_cache_progress_counts()
        if (not self._loader_timeline_active) and total_timeline_rows > 0 and cached_timeline_rows <= 0:
            msg = self.small.render("Timeline cache is not loaded. Click Load.", True, (182, 198, 230))
            self.screen.blit(msg, (rect.x + 10, rect.y + 28))
            hint = self.tiny.render("No timeline files are read until you click Load.", True, (150, 165, 188))
            self.screen.blit(hint, (rect.x + 10, rect.y + 50))
            return

        cursor_norm = max(0.0, min(1.0, float(self.timeline_progress)))
        graph_points = []
        env_values = []
        fallback_count = 0
        used_rows = 0
        candidate_rows = 0
        max_samples = 2
        for row_idx, row in enumerate(self.rows):
            if not isinstance(row, dict):
                continue
            env_rate = row.get("env_rate")
            if (not hr._is_number(env_rate)) or (not self._env_rate_in_view(env_rate)):
                continue
            candidate_rows += 1
            payload = self._timeline_payload_for_row(row, build_if_missing=False)
            cloud_series = payload.get("cloud_series") if isinstance(payload, dict) else None
            if not isinstance(cloud_series, list) or not cloud_series:
                continue
            source = str(payload.get("timeline_source", "none")) if isinstance(payload, dict) else "none"
            if source != "snapshots":
                fallback_count += 1
            row_had_points = False
            for run_item in cloud_series:
                if not isinstance(run_item, dict):
                    continue
                samples = run_item.get("samples")
                if not isinstance(samples, list) or not samples:
                    continue
                max_samples = max(max_samples, len(samples))
                points = self._cloud_points_for_progress(samples, cursor_norm)
                if not points:
                    continue
                row_had_points = True
                env_float = float(env_rate)
                env_values.append(env_float)
                for evo_val, fit_val in points:
                    graph_points.append(
                        {
                            "row_index": int(row_idx),
                            "x": env_float,
                            "y": float(evo_val),
                            "fitness": float(fit_val),
                        }
                    )
            if row_had_points:
                used_rows += 1

        self.timeline_frame_count = max(2, int(max_samples))
        if not graph_points:
            msg = self.small.render("No timeline data available across hub.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 10, rect.y + 28))
            if self._loader_rows_active or self._loader_timeline_active:
                if self._loader_timeline_active:
                    pending = max(0, int(self._loader_timeline_total) - int(self._loader_timeline_done))
                    status = self._loader_timeline_status or "Loading timeline cache..."
                    eta_text = self._eta_text(
                        self._loader_timeline_done,
                        self._loader_timeline_total,
                        self._loader_timeline_started_at,
                    )
                    note_text = (
                        f"Timeline loading: {self._loader_timeline_progress_ratio() * 100.0:.1f}% "
                        f"({pending} pending)  {eta_text}  {status}"
                    )
                else:
                    pending = max(0, int(self._loader_rows_total) - int(self._loader_rows_done))
                    status = self._loader_rows_status or "Loading simulation rows..."
                    eta_text = self._eta_text(
                        self._loader_rows_done,
                        self._loader_rows_total,
                        self._loader_rows_started_at,
                    )
                    note_text = (
                        f"Simulation loading: {self._loader_rows_progress_ratio() * 100.0:.1f}% "
                        f"({pending} pending)  {eta_text}  {status}"
                    )
                note = self.tiny.render(
                    hr._fit_text(self.tiny, note_text, rect.width - 20),
                    True,
                    (158, 172, 196),
                )
                self.screen.blit(note, (rect.x + 10, rect.y + 48))
            return

        graph_points, _ = self._normalize_graph_points_for_display(graph_points, group_key="row_index")
        fit_report = hr._fit_hub_models_from_graph_points(graph_points)
        best = fit_report.get("best_model") if isinstance(fit_report, dict) else None

        frame_idx = self._timeline_frame_index() + 1
        frame_total = max(2, int(self.timeline_frame_count))
        frame_lbl = self.tiny.render(f"frame {frame_idx}/{frame_total}", True, (170, 182, 202))
        self.screen.blit(frame_lbl, (rect.right - frame_lbl.get_width() - 10, rect.y + 10))

        note = f"rows with data: {used_rows}/{candidate_rows} | points: {len(graph_points)}"
        if fallback_count > 0:
            note += f" | fallback rows: {fallback_count}"
        if self.normalize_display:
            note += f" | norm={self._normalize_mode_label().lower()}"
        self.screen.blit(
            self.tiny.render(hr._fit_text(self.tiny, note, rect.width - 20), True, (160, 175, 195)),
            (rect.x + 10, rect.y + 36),
        )

        plot_top = rect.y + 48
        plot = pg.Rect(rect.x + 48, plot_top, rect.width - 64, rect.height - ((plot_top - rect.y) + 24))
        if plot.width <= 24 or plot.height <= 24:
            return
        pg.draw.rect(self.screen, (12, 14, 19), plot)
        pg.draw.rect(self.screen, (60, 66, 78), plot, 1)

        xs = [float(item["x"]) for item in graph_points if hr._is_number(item.get("x"))]
        ys = [float(item["y"]) for item in graph_points if hr._is_number(item.get("y"))]
        fits = [float(item["fitness"]) for item in graph_points if hr._is_number(item.get("fitness"))]
        if not xs or not ys:
            return

        raw_min_x = min(xs)
        raw_max_x = max(xs)
        raw_min_y = min(ys)
        raw_max_y = max(ys)
        min_x, max_x = (raw_min_x, raw_max_x) if raw_max_x > raw_min_x else (raw_min_x - 0.05, raw_max_x + 0.05)
        min_y, max_y = (raw_min_y, raw_max_y) if raw_max_y > raw_min_y else (raw_min_y - 0.05, raw_max_y + 0.05)
        if raw_max_x > raw_min_x:
            pad = (raw_max_x - raw_min_x) * 0.04
            min_x -= pad
            max_x += pad
        if raw_max_y > raw_min_y:
            pad = (raw_max_y - raw_min_y) * 0.04
            min_y -= pad
            max_y += pad
        fit_min = min(fits) if fits else 0.0
        fit_max = max(fits) if fits else 1.0
        fit_den = max(1e-9, fit_max - fit_min)

        def _to_px(xv: float, yv: float) -> tuple[int, int]:
            px = plot.x + int(((xv - min_x) / (max_x - min_x)) * plot.width)
            py = plot.y + plot.height - int(((yv - min_y) / (max_y - min_y)) * plot.height)
            return px, py

        fit_segments = []
        if isinstance(best, dict):
            sample = []
            y_span = max(1e-9, max_y - min_y)
            for idx in range(220):
                xv = min_x + ((max_x - min_x) * float(idx) / 219.0)
                yv = hr._eval_hub_model(best, xv)
                if not hr._is_number(yv):
                    continue
                y_float = float(yv)
                if y_float < (min_y - (2.0 * y_span)) or y_float > (max_y + (2.0 * y_span)):
                    continue
                sample.append((xv, y_float))
            if len(sample) >= 2:
                for i in range(1, len(sample)):
                    fit_segments.append((sample[i - 1], sample[i]))

        selected_idx = int(self.selected_row_index) if self.selected_row_index is not None else -1
        for item in graph_points:
            px, py = _to_px(float(item["x"]), float(item["y"]))
            n = max(0.0, min(1.0, (float(item["fitness"]) - fit_min) / fit_den))
            radius = max(1, int(round(4 * n)))
            color = (35 + int(220 * n), 128 + int(95 * n), 236 - int(166 * n))
            if int(item.get("row_index", -1)) == selected_idx:
                pg.draw.circle(self.screen, (255, 255, 255), (px, py), radius + 3, 1)
            pg.draw.circle(self.screen, color, (px, py), radius)
        for start, end in fit_segments:
            pg.draw.line(
                self.screen,
                (248, 196, 92),
                _to_px(start[0], start[1]),
                _to_px(end[0], end[1]),
                2,
            )

        if isinstance(best, dict) and hr._is_number(best.get("r2")):
            equation = str(best.get("equation", ""))
            copy_reserved = self._draw_equation_copy_button(rect, equation, rect.y + 8, margin=10)
            text_w = max(60, rect.width - 20 - copy_reserved)
            line_1 = hr._fit_text(self.tiny, f"Equation: {equation}", text_w)
            line_2 = hr._fit_text(self.tiny, f"R^2: {float(best.get('r2')):.4f}", text_w)
            col = (235, 210, 146)
        else:
            line_1 = "Equation: not enough data"
            line_2 = "R^2: --"
            col = (170, 170, 170)
        self.screen.blit(self.tiny.render(line_1, True, col), (rect.x + 10, rect.y + 8))
        self.screen.blit(self.tiny.render(line_2, True, col), (rect.x + 10, rect.y + 24))

        min_y_txt = self.tiny.render(f"{raw_min_y:.3f}", True, (150, 150, 150))
        max_y_txt = self.tiny.render(f"{raw_max_y:.3f}", True, (150, 150, 150))
        min_x_txt = self.tiny.render(f"{raw_min_x:.3f}", True, (150, 150, 150))
        max_x_txt = self.tiny.render(f"{raw_max_x:.3f}", True, (150, 150, 150))
        x_lab = self.tiny.render("env change rate", True, (155, 155, 155))
        y_lab = self.tiny.render("evo speed", True, (155, 155, 155))
        self.screen.blit(max_y_txt, (plot.x - 40, plot.y - 2))
        self.screen.blit(min_y_txt, (plot.x - 40, plot.bottom - 14))
        self.screen.blit(min_x_txt, (plot.x, plot.bottom + 3))
        self.screen.blit(max_x_txt, (plot.right - max_x_txt.get_width(), plot.bottom + 3))
        self.screen.blit(x_lab, (plot.x + 6, plot.bottom + 18))
        self.screen.blit(y_lab, (plot.x - 42, plot.y + 8))

    def _env_color(self, env_rate: float, env_min: float, env_max: float) -> tuple[int, int, int]:
        if env_max <= env_min:
            norm = 0.5
        else:
            norm = (float(env_rate) - float(env_min)) / (float(env_max) - float(env_min))
            norm = max(0.0, min(1.0, norm))
        return (
            int(35 + (205 * norm)),
            int(170 - (65 * norm)),
            int(235 - (145 * norm)),
        )

    def _draw_master_cloud(
        self,
        rect,
        title: str,
        dual_color: bool,
        constant_radius: bool,
        top_n_per_master: int | None,
        overlay_curves: bool,
        layout_style: str = "cloud",
        render_alpha: float = 1.0,
    ) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (17, 19, 24), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)
        if str(layout_style) == "hub":
            line_1 = hr._fit_text(self.tiny, title, rect.width - 20)
            line_2 = hr._fit_text(
                self.tiny,
                "Hub-style layout",
                rect.width - 20,
            )
            self.screen.blit(self.tiny.render(line_1, True, (190, 206, 230)), (rect.x + 10, rect.y + 8))
            self.screen.blit(self.tiny.render(line_2, True, (160, 175, 195)), (rect.x + 10, rect.y + 24))
        else:
            self.screen.blit(
                self.tiny.render(hr._fit_text(self.tiny, title, rect.width - 20), True, (190, 206, 230)),
                (rect.x + 10, rect.y + 8),
            )

        masters = []
        all_points = []
        env_values = []
        for row_idx, row in enumerate(self.rows):
            if not isinstance(row, dict):
                continue
            env_rate = row.get("env_rate")
            points = row.get("points")
            if (not hr._is_number(env_rate)) or (not self._env_rate_in_view(env_rate)) or (not isinstance(points, list)):
                continue
            numeric_points = []
            for pair in points:
                if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                    continue
                xv, yv = pair[0], pair[1]
                if hr._is_number(xv) and hr._is_number(yv):
                    numeric_points.append((float(xv), float(yv)))
            if not numeric_points:
                continue
            if isinstance(top_n_per_master, int) and top_n_per_master > 0:
                numeric_points = sorted(numeric_points, key=lambda p: p[1], reverse=True)[: int(top_n_per_master)]
                numeric_points.sort(key=lambda p: p[0])
            fit_vals = [float(p[1]) for p in numeric_points]
            fit_norm_context = self._normalization_context_for_values(fit_vals)
            if self.normalize_display:
                display_points = [
                    (float(xv), self._normalize_value_for_display(float(yv), fit_norm_context))
                    for xv, yv in numeric_points
                ]
            else:
                display_points = [(float(xv), float(yv)) for xv, yv in numeric_points]
            masters.append(
                {
                    "row_index": int(row_idx),
                    "env_rate": float(env_rate),
                    "points": display_points,
                    "fit": row.get("fit"),
                    "fit_norm_context": fit_norm_context,
                }
            )
            all_points.extend((float(env_rate), p[0], p[1]) for p in display_points)
            env_values.append(float(env_rate))
        if not all_points:
            msg = self.small.render("No points available.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 10, rect.y + 28))
            return

        xs = [p[1] for p in all_points]
        ys = [p[2] for p in all_points]
        fit_min = min(ys)
        fit_max = max(ys)
        fit_den = max(1e-9, fit_max - fit_min)
        env_min = min(env_values) if env_values else 0.0
        env_max = max(env_values) if env_values else 1.0

        raw_min_x = min(xs)
        raw_max_x = max(xs)
        raw_min_y = min(ys)
        raw_max_y = max(ys)
        if raw_max_x <= raw_min_x:
            min_x = raw_min_x - 0.05
            max_x = raw_max_x + 0.05
        else:
            x_pad = (raw_max_x - raw_min_x) * 0.04
            min_x = raw_min_x - x_pad
            max_x = raw_max_x + x_pad
        if raw_max_y <= raw_min_y:
            min_y = raw_min_y - 0.05
            max_y = raw_max_y + 0.05
        else:
            y_pad = (raw_max_y - raw_min_y) * 0.04
            min_y = raw_min_y - y_pad
            max_y = raw_max_y + y_pad

        if str(layout_style) == "hub":
            plot_top = rect.y + 48
            plot = pg.Rect(
                rect.x + 48,
                plot_top,
                rect.width - 64,
                rect.height - ((plot_top - rect.y) + 24),
            )
        else:
            plot = pg.Rect(rect.x + 40, rect.y + 28, rect.width - 52, rect.height - 42)
        if plot.width <= 24 or plot.height <= 24:
            return
        pg.draw.rect(self.screen, (12, 14, 19), plot)
        pg.draw.rect(self.screen, (60, 66, 78), plot, 1)

        alpha = int(round(max(0.0, min(1.0, float(render_alpha))) * 255.0))
        plot_layer = pg.Surface((plot.width, plot.height), pg.SRCALPHA)

        def _to_px(xv: float, yv: float) -> tuple[int, int]:
            px = int(((xv - min_x) / (max_x - min_x)) * plot.width)
            py = plot.height - int(((yv - min_y) / (max_y - min_y)) * plot.height)
            return px, py

        curve_segments = []

        for master in masters:
            env_rate = float(master.get("env_rate", 0.0))
            env_color = self._env_color(env_rate, env_min, env_max)
            for xv, yv in master.get("points", []):
                fit_norm = max(0.0, min(1.0, (float(yv) - fit_min) / fit_den))
                if dual_color:
                    # Spectrum mode: color encodes environment change rate.
                    color = env_color
                else:
                    color = (
                        int((0.55 * env_color[0]) + (0.45 * (35 + (220 * fit_norm)))),
                        int((0.55 * env_color[1]) + (0.45 * (35 + (220 * fit_norm)))),
                        int((0.55 * env_color[2]) + (0.45 * (215 - (140 * fit_norm)))),
                    )
                radius = 2 if constant_radius else (1 + int(round(3 * fit_norm)))
                pg.draw.circle(
                    plot_layer,
                    (color[0], color[1], color[2], alpha),
                    _to_px(float(xv), float(yv)),
                    max(1, radius),
                )
        if overlay_curves:
            for master in masters:
                fit = master.get("fit")
                if not isinstance(fit, dict):
                    continue
                apex_x = fit.get("apex_x")
                apex_y = fit.get("apex_y")
                sigma_left = fit.get("sigma_left")
                sigma_right = fit.get("sigma_right")
                if not (
                    hr._is_number(apex_x)
                    and hr._is_number(apex_y)
                    and hr._is_number(sigma_left)
                    and hr._is_number(sigma_right)
                ):
                    continue
                env_rate = float(master.get("env_rate", 0.0))
                curve_color = self._env_color(env_rate, env_min, env_max)
                fit_norm_context = master.get("fit_norm_context")
                curve = []
                for i in range(180):
                    xv = min_x + ((max_x - min_x) * float(i) / 179.0)
                    yv = hr._predict_piecewise_gaussian(
                        float(xv),
                        float(apex_x),
                        float(apex_y),
                        float(sigma_left),
                        float(sigma_right),
                    )
                    if hr._is_number(yv):
                        curve.append(
                            (
                                float(xv),
                                self._normalize_value_for_display(
                                    float(yv),
                                    fit_norm_context if isinstance(fit_norm_context, dict) else None,
                                ),
                            )
                        )
                if len(curve) >= 2:
                    for i in range(1, len(curve)):
                        curve_segments.append((curve_color, curve[i - 1], curve[i]))
        for curve_color, start, end in curve_segments:
            pg.draw.line(
                plot_layer,
                (curve_color[0], curve_color[1], curve_color[2], alpha),
                _to_px(start[0], start[1]),
                _to_px(end[0], end[1]),
                2,
            )

        self.screen.blit(plot_layer, (plot.x, plot.y))

        self.screen.blit(self.tiny.render(f"{raw_max_y:.3f}", True, (150, 150, 150)), (plot.x - 34, plot.y - 2))
        self.screen.blit(self.tiny.render(f"{raw_min_y:.3f}", True, (150, 150, 150)), (plot.x - 34, plot.bottom - 14))
        self.screen.blit(self.tiny.render(f"{raw_min_x:.3f}", True, (150, 150, 150)), (plot.x, plot.bottom + 2))
        max_x_txt = self.tiny.render(f"{raw_max_x:.3f}", True, (150, 150, 150))
        self.screen.blit(max_x_txt, (plot.right - max_x_txt.get_width(), plot.bottom + 2))
        self.screen.blit(self.tiny.render("evo speed", True, (155, 155, 155)), (plot.x + 4, plot.y + 4))
        fit_label = f"fitness ({self._normalize_mode_label().lower()})" if self.normalize_display else "fitness"
        self.screen.blit(self.tiny.render(fit_label, True, (155, 155, 155)), (plot.x - 34, plot.y + 14))

    def _draw_spectrum_graph(self, rect) -> None:
        self._draw_master_cloud(
            rect,
            title="Spectrum Graph: env+fitness coloring with all master bell curves",
            dual_color=True,
            constant_radius=False,
            top_n_per_master=None,
            overlay_curves=True,
            render_alpha=0.5,
        )

    def _draw_range_graph(self, rect) -> None:
        self._draw_master_cloud(
            rect,
            title=f"Range Graph: constant dot size, top {int(self.range_top_n)} points/master by fitness",
            dual_color=False,
            constant_radius=True,
            top_n_per_master=int(self.range_top_n),
            overlay_curves=False,
        )

    def _range_hub_graph_data(self) -> tuple[list[dict], dict | None, list[dict]]:
        filtered_rows = []
        graph_points = []
        bell_curve_rows = []
        top_n = max(1, int(self.range_top_n))
        for row_idx, row in enumerate(self.rows):
            if not isinstance(row, dict):
                continue
            env_rate = row.get("env_rate")
            points = row.get("points")
            if (not hr._is_number(env_rate)) or (not self._env_rate_in_view(env_rate)) or (not isinstance(points, list)):
                continue
            numeric_points = []
            for pair in points:
                if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                    continue
                evo_val, fit_val = pair[0], pair[1]
                if hr._is_number(evo_val) and hr._is_number(fit_val):
                    numeric_points.append((float(evo_val), float(fit_val)))
            if not numeric_points:
                continue
            numeric_points = sorted(numeric_points, key=lambda p: p[1], reverse=True)[:top_n]
            numeric_points.sort(key=lambda p: p[0])
            filtered_rows.append({"env_rate": float(env_rate), "points": list(numeric_points)})
            bell_curve_rows.append(
                {
                    "row_index": int(row_idx),
                    "env_rate": float(env_rate),
                    "fit": row.get("fit"),
                }
            )
            for src_idx, (evo_val, fit_val) in enumerate(numeric_points, start=1):
                graph_points.append(
                    {
                        "row_index": int(row_idx),
                        "x": float(env_rate),
                        "y": float(evo_val),
                        "fitness": float(fit_val),
                        "source_row_index": int(src_idx),
                    }
                )
        fit_report = hr._fit_hub_models_from_rows(filtered_rows) if filtered_rows else {}
        best = fit_report.get("best_model") if isinstance(fit_report, dict) else None
        return graph_points, best, bell_curve_rows

    def _master_fit_lines_3d_data(self) -> tuple[list[dict], list[dict]]:
        valid_rows = []
        for row_idx, row in enumerate(self.rows):
            if not isinstance(row, dict):
                continue
            env_rate = row.get("env_rate")
            fit = row.get("fit")
            if (not hr._is_number(env_rate)) or (not self._env_rate_in_view(env_rate)) or (not isinstance(fit, dict)):
                continue
            apex_x = fit.get("apex_x")
            apex_y = fit.get("apex_y")
            sigma_left = fit.get("sigma_left")
            sigma_right = fit.get("sigma_right")
            if not (
                hr._is_number(apex_x)
                and hr._is_number(apex_y)
                and hr._is_number(sigma_left)
                and hr._is_number(sigma_right)
            ):
                continue
            valid_rows.append(
                {
                    "row_index": int(row_idx),
                    "env_rate": float(env_rate),
                    "fit": fit,
                    "apex_x": float(apex_x),
                    "apex_y": float(apex_y),
                    "sigma_left": max(1e-6, float(sigma_left)),
                    "sigma_right": max(1e-6, float(sigma_right)),
                }
            )
        if not valid_rows:
            return [], []

        x_candidates = []
        for item in valid_rows:
            x_candidates.append(float(item["apex_x"]) - (4.0 * float(item["sigma_left"])))
            x_candidates.append(float(item["apex_x"]) + (4.0 * float(item["sigma_right"])))
        min_x = min(x_candidates)
        max_x = max(x_candidates)
        if max_x <= min_x:
            min_x -= 0.05
            max_x += 0.05

        graph_points = []
        bell_curve_rows = []
        for item in valid_rows:
            env_rate = float(item["env_rate"])
            fit = item["fit"]
            bell_curve_rows.append(
                {
                    "row_index": int(item["row_index"]),
                    "env_rate": env_rate,
                    "fit": fit,
                }
            )
            for idx in range(180):
                evo_val = min_x + ((max_x - min_x) * float(idx) / 179.0)
                fit_val = hr._predict_piecewise_gaussian(
                    float(evo_val),
                    float(item["apex_x"]),
                    float(item["apex_y"]),
                    float(item["sigma_left"]),
                    float(item["sigma_right"]),
                )
                if not hr._is_number(fit_val):
                    continue
                graph_points.append(
                    {
                        "row_index": int(item["row_index"]),
                        "x": env_rate,
                        "y": float(evo_val),
                        "fitness": float(fit_val),
                    }
                )
        return graph_points, bell_curve_rows

    def _draw_range_hub_graph_3d(self, rect) -> None:
        graph_points, best, bell_curve_rows = self._range_hub_graph_data()
        if not graph_points:
            pg = self.pg
            pg.draw.rect(self.screen, (18, 20, 26), rect)
            pg.draw.rect(self.screen, (74, 80, 94), rect, 1)
            msg = self.small.render("No range hub points yet.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 12, rect.y + 50))
            self.hub_dot_hits = []
            return
        self._draw_hub_graph_3d(
            rect,
            axis_keys=("x", "y", "fitness"),
            axis_labels=("env rate", "evo speed", "fitness"),
            mapping_text="x=env rate, y=evo speed, z=fitness (range hub top-N)",
            source_points=graph_points,
            best_model=best,
            fit_scope_label="Best fit (all masters)",
            draw_best_fit_line=True,
            draw_bell_curves=True,
            bell_curve_rows=bell_curve_rows,
            title_text="Range Hub 3D Fit",
        )

    def _draw_master_fit_lines_3d(self, rect) -> None:
        graph_points, bell_curve_rows = self._master_fit_lines_3d_data()
        if not graph_points:
            pg = self.pg
            pg.draw.rect(self.screen, (18, 20, 26), rect)
            pg.draw.rect(self.screen, (74, 80, 94), rect, 1)
            msg = self.small.render("No master fits available in selected env range.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 12, rect.y + 50))
            self.hub_dot_hits = []
            return
        self._draw_hub_graph_3d(
            rect,
            axis_keys=("x", "y", "fitness"),
            axis_labels=("env rate", "evo speed", "fitness"),
            mapping_text="x=env rate, y=evo speed, z=fitness (master fit lines only)",
            source_points=graph_points,
            best_model=self.hub_best_fit if isinstance(self.hub_best_fit, dict) else None,
            fit_scope_label="Best fit (all masters)",
            draw_best_fit_line=False,
            draw_bell_curves=True,
            bell_curve_rows=bell_curve_rows,
            draw_points=False,
            show_hub_equation=False,
            title_text="Master Fit Lines 3D",
        )

    def _draw_range_hub_graph(self, rect) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (18, 20, 26), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)

        graph_points, best, _ = self._range_hub_graph_data()
        graph_points, _ = self._normalize_graph_points_for_display(graph_points, group_key="row_index")

        header_x = rect.x + 10
        copy_reserved = 0
        if isinstance(best, dict) and hr._is_number(best.get("r2")):
            equation = str(best.get("equation", ""))
            copy_reserved = self._draw_equation_copy_button(rect, equation, rect.y + 8, margin=10)
            header_w = max(60, rect.width - 20 - copy_reserved)
            line_1 = hr._fit_text(self.tiny, f"Equation: {equation}", header_w)
            line_2 = hr._fit_text(self.tiny, f"R^2: {float(best.get('r2')):.4f}", header_w)
            col = (235, 210, 146)
        else:
            header_w = max(60, rect.width - 20)
            line_1 = "Equation: not enough data"
            line_2 = "R^2: --"
            col = (170, 170, 170)
        self.screen.blit(self.tiny.render(line_1, True, col), (header_x, rect.y + 8))
        self.screen.blit(self.tiny.render(line_2, True, col), (header_x, rect.y + 24))

        if not graph_points:
            msg = self.small.render("No range hub points yet.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 12, rect.y + 50))
            self.hub_dot_hits = []
            return

        plot_top = rect.y + 48
        plot = pg.Rect(rect.x + 48, plot_top, rect.width - 64, rect.height - ((plot_top - rect.y) + 24))
        if plot.width <= 24 or plot.height <= 24:
            return
        pg.draw.rect(self.screen, (12, 14, 19), plot)
        pg.draw.rect(self.screen, (60, 66, 78), plot, 1)

        xs = [float(p["x"]) for p in graph_points if hr._is_number(p.get("x"))]
        ys = [float(p["y"]) for p in graph_points if hr._is_number(p.get("y"))]
        fits = [float(p["fitness"]) for p in graph_points if hr._is_number(p.get("fitness"))]
        if not xs or not ys:
            return

        raw_min_x = min(xs)
        raw_max_x = max(xs)
        raw_min_y = min(ys)
        raw_max_y = max(ys)
        min_x, max_x = (raw_min_x, raw_max_x) if raw_max_x > raw_min_x else (raw_min_x - 0.05, raw_max_x + 0.05)
        min_y, max_y = (raw_min_y, raw_max_y) if raw_max_y > raw_min_y else (raw_min_y - 0.05, raw_max_y + 0.05)
        if raw_max_x > raw_min_x:
            pad = (raw_max_x - raw_min_x) * 0.04
            min_x -= pad
            max_x += pad
        if raw_max_y > raw_min_y:
            pad = (raw_max_y - raw_min_y) * 0.04
            min_y -= pad
            max_y += pad
        fit_min = min(fits) if fits else 0.0
        fit_max = max(fits) if fits else 1.0
        fit_den = max(1e-9, fit_max - fit_min)

        def _to_px(xv: float, yv: float) -> tuple[int, int]:
            px = plot.x + int(((xv - min_x) / (max_x - min_x)) * plot.width)
            py = plot.y + plot.height - int(((yv - min_y) / (max_y - min_y)) * plot.height)
            return px, py

        fit_segments = []
        if isinstance(best, dict):
            sample = []
            y_span = max(1e-9, max_y - min_y)
            for idx in range(220):
                xv = min_x + ((max_x - min_x) * float(idx) / 219.0)
                yv = hr._eval_hub_model(best, xv)
                if not hr._is_number(yv):
                    continue
                y_float = float(yv)
                if y_float < (min_y - (2.0 * y_span)) or y_float > (max_y + (2.0 * y_span)):
                    continue
                sample.append((xv, y_float))
            if len(sample) >= 2:
                for i in range(1, len(sample)):
                    fit_segments.append((sample[i - 1], sample[i]))

        self.hub_dot_hits = []
        selected_idx = int(self.selected_row_index) if self.selected_row_index is not None else -1
        for point in graph_points:
            px, py = _to_px(float(point["x"]), float(point["y"]))
            n = max(0.0, min(1.0, (float(point["fitness"]) - fit_min) / fit_den))
            color = (35 + int(220 * n), 128 + int(95 * n), 236 - int(166 * n))
            radius = 3
            if int(point.get("row_index", -1)) == selected_idx:
                pg.draw.circle(self.screen, (255, 255, 255), (px, py), radius + 3, 1)
            pg.draw.circle(self.screen, color, (px, py), radius)
            self.hub_dot_hits.append(
                {
                    "px": px,
                    "py": py,
                    "radius": 7,
                    "row_index": int(point.get("row_index", -1)),
                }
            )
        for start, end in fit_segments:
            pg.draw.line(
                self.screen,
                (248, 196, 92),
                _to_px(start[0], start[1]),
                _to_px(end[0], end[1]),
                2,
            )

        min_y_txt = self.tiny.render(f"{raw_min_y:.3f}", True, (150, 150, 150))
        max_y_txt = self.tiny.render(f"{raw_max_y:.3f}", True, (150, 150, 150))
        min_x_txt = self.tiny.render(f"{raw_min_x:.3f}", True, (150, 150, 150))
        max_x_txt = self.tiny.render(f"{raw_max_x:.3f}", True, (150, 150, 150))
        x_lab = self.tiny.render("env change rate", True, (155, 155, 155))
        y_lab = self.tiny.render("evo speed", True, (155, 155, 155))
        self.screen.blit(max_y_txt, (plot.x - 40, plot.y - 2))
        self.screen.blit(min_y_txt, (plot.x - 40, plot.bottom - 14))
        self.screen.blit(min_x_txt, (plot.x, plot.bottom + 3))
        self.screen.blit(max_x_txt, (plot.right - max_x_txt.get_width(), plot.bottom + 3))
        self.screen.blit(x_lab, (plot.x + 6, plot.bottom + 18))
        self.screen.blit(y_lab, (plot.x - 42, plot.y + 8))

    def _set_range_slider_from_mouse(self, mx: int) -> None:
        if self._range_slider_rect is None:
            return
        self._range_top_n_auto_all = False
        if self.range_top_n_max <= self.range_top_n_min:
            self.range_top_n = int(self.range_top_n_min)
            return
        ratio = (float(mx) - float(self._range_slider_rect.x)) / max(1.0, float(self._range_slider_rect.width))
        ratio = max(0.0, min(1.0, ratio))
        value = int(round(float(self.range_top_n_min) + (ratio * float(self.range_top_n_max - self.range_top_n_min))))
        self.range_top_n = max(int(self.range_top_n_min), min(int(self.range_top_n_max), int(value)))

    def _draw_graph_controls(self, rect) -> None:
        pg = self.pg
        mode = self._active_graph_mode()
        pg.draw.rect(self.screen, (17, 19, 24), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)

        btn_w = 84
        btn_h = 28
        by = rect.y + 6
        self._back_button_rect = pg.Rect(rect.x + 10, by, btn_w, btn_h)
        self._next_button_rect = pg.Rect(self._back_button_rect.right + 8, by, btn_w, btn_h)
        self._export_button_rect = pg.Rect(self._next_button_rect.right + 8, by, btn_w, btn_h)
        self._settings_button_rect = pg.Rect(self._export_button_rect.right + 8, by, btn_w, btn_h)
        self._normalize_button_rect = pg.Rect(self._settings_button_rect.right + 8, by, 132, btn_h)
        self._increment_button_rect = pg.Rect(self._normalize_button_rect.right + 8, by, 118, btn_h)
        for button_rect, label in (
            (self._back_button_rect, "Back"),
            (self._next_button_rect, "Next"),
            (self._export_button_rect, "Export"),
            (self._settings_button_rect, "Settings"),
        ):
            pg.draw.rect(self.screen, (42, 42, 46), button_rect)
            pg.draw.rect(self.screen, (155, 155, 155), button_rect, 1)
            txt = self.small.render(label, True, (230, 230, 230))
            self.screen.blit(
                txt,
                (
                    button_rect.x + (button_rect.width - txt.get_width()) // 2,
                    button_rect.y + 5,
                ),
            )
        pg.draw.rect(self.screen, (42, 42, 46), self._normalize_button_rect)
        pg.draw.rect(self.screen, (155, 155, 155), self._normalize_button_rect, 1)
        normalize_label = f"Norm: {self._normalize_mode_label()}"
        normalize_color = (172, 226, 178) if self.normalize_display else (230, 230, 230)
        txt = self.small.render(normalize_label, True, normalize_color)
        self.screen.blit(
            txt,
            (
                self._normalize_button_rect.x + (self._normalize_button_rect.width - txt.get_width()) // 2,
                self._normalize_button_rect.y + 5,
            ),
        )
        pg.draw.rect(self.screen, (42, 42, 46), self._increment_button_rect)
        pg.draw.rect(self.screen, (155, 155, 155), self._increment_button_rect, 1)
        increment_label = f"Step: {self._increment_mode_label()}"
        inc_color = (172, 226, 178) if self._active_increment_mode() == "step0p01" else (230, 230, 230)
        inc_txt = self.small.render(increment_label, True, inc_color)
        self.screen.blit(
            inc_txt,
            (
                self._increment_button_rect.x + (self._increment_button_rect.width - inc_txt.get_width()) // 2,
                self._increment_button_rect.y + 5,
            ),
        )

        mode_labels = {
            "normal": "Normal",
            "hub_3d": "Hub 3D",
            "hub_3d_evo_fit_env": "Hub 3D (Evo/Fit/Env)",
            "timeline_hub": "Hub Timeline",
            "spectrum": "Spectrum+Curves",
            "range": "Range",
            "range_hub": "Range Hub",
            "range_hub_3d_fit": "Range Hub 3D Fit",
            "master_fit_lines_3d": "Master Fit Lines 3D",
        }
        mode_text = f"Mode {self.graph_mode_index + 1}/{len(self.graph_modes)}: {mode_labels.get(mode, mode)}"
        self.screen.blit(self.small.render(mode_text, True, (205, 215, 230)), (self._increment_button_rect.right + 12, by + 5))

        self._timeline_prev_button_rect = None
        self._timeline_play_button_rect = None
        self._timeline_next_button_rect = None
        self._timeline_load_button_rect = None
        self._timeline_step_mode_button_rect = None
        self._timeline_slider_rect = None
        self._range_slider_rect = None
        self._env_range_slider_rect = None
        if mode == "timeline_hub":
            btn_w = 92
            btn_h = 26
            btn_gap = 6
            slider_w = max(170, min(320, rect.width - 730))
            slider_h = 6
            slider_x = rect.right - slider_w - 16
            slider_y = by + (btn_h // 2) - (slider_h // 2)
            self._timeline_slider_rect = pg.Rect(slider_x, slider_y, slider_w, slider_h)
            pg.draw.rect(self.screen, (92, 92, 98), self._timeline_slider_rect)
            knob_x = self._timeline_slider_rect.x + int(float(self.timeline_progress) * self._timeline_slider_rect.width)
            pg.draw.circle(self.screen, (228, 228, 228), (knob_x, self._timeline_slider_rect.centery), 7)

            self._timeline_next_button_rect = pg.Rect(slider_x - btn_w - btn_gap, by + 1, btn_w, btn_h)
            self._timeline_play_button_rect = pg.Rect(
                self._timeline_next_button_rect.x - btn_w - btn_gap,
                by + 1,
                btn_w,
                btn_h,
            )
            self._timeline_prev_button_rect = pg.Rect(
                self._timeline_play_button_rect.x - btn_w - btn_gap,
                by + 1,
                btn_w,
                btn_h,
            )
            self._timeline_load_button_rect = pg.Rect(
                self._timeline_prev_button_rect.x - btn_w - btn_gap,
                by + 1,
                btn_w,
                btn_h,
            )
            cached_rows, total_rows = self._timeline_cache_progress_counts()
            cache_loaded = total_rows > 0 and cached_rows >= total_rows
            if self._loader_timeline_active:
                load_label = "Loading..."
                load_fill = (56, 70, 86)
                load_border = (125, 162, 198)
                load_color = (214, 228, 246)
            elif cache_loaded:
                load_label = "Loaded"
                load_fill = (40, 66, 54)
                load_border = (122, 176, 146)
                load_color = (198, 236, 210)
            else:
                load_label = "Load"
                load_fill = (42, 42, 46)
                load_border = (155, 155, 155)
                load_color = (230, 230, 230)
            pg.draw.rect(self.screen, load_fill, self._timeline_load_button_rect)
            pg.draw.rect(self.screen, load_border, self._timeline_load_button_rect, 1)
            load_txt = self.tiny.render(load_label, True, load_color)
            self.screen.blit(
                load_txt,
                (
                    self._timeline_load_button_rect.x + (self._timeline_load_button_rect.width - load_txt.get_width()) // 2,
                    self._timeline_load_button_rect.y + 6,
                ),
            )
            self._timeline_step_mode_button_rect = pg.Rect(
                self._timeline_load_button_rect.x - btn_w - btn_gap,
                by + 1,
                btn_w,
                btn_h,
            )
            point1_mode = str(self.timeline_step_mode) == "point1"
            step_label = "Step: 0.1" if point1_mode else "Step: Normal"
            step_fill = (40, 66, 54) if point1_mode else (42, 42, 46)
            step_border = (122, 176, 146) if point1_mode else (155, 155, 155)
            step_color = (198, 236, 210) if point1_mode else (230, 230, 230)
            pg.draw.rect(self.screen, step_fill, self._timeline_step_mode_button_rect)
            pg.draw.rect(self.screen, step_border, self._timeline_step_mode_button_rect, 1)
            step_txt = self.tiny.render(step_label, True, step_color)
            self.screen.blit(
                step_txt,
                (
                    self._timeline_step_mode_button_rect.x + (self._timeline_step_mode_button_rect.width - step_txt.get_width()) // 2,
                    self._timeline_step_mode_button_rect.y + 6,
                ),
            )
            play_label = "Stop" if self.timeline_playing else "Play"
            for button_rect, label in (
                (self._timeline_prev_button_rect, "Last Frame"),
                (self._timeline_play_button_rect, play_label),
                (self._timeline_next_button_rect, "Next Frame"),
            ):
                pg.draw.rect(self.screen, (42, 42, 46), button_rect)
                pg.draw.rect(self.screen, (155, 155, 155), button_rect, 1)
                txt = self.tiny.render(label, True, (230, 230, 230))
                self.screen.blit(
                    txt,
                    (
                        button_rect.x + (button_rect.width - txt.get_width()) // 2,
                        button_rect.y + 6,
                    ),
                )
            frame_idx = self._timeline_frame_index() + 1
            frame_total = max(2, int(self.timeline_frame_count))
            timeline_label = self.tiny.render(
                f"Frame: {frame_idx}/{frame_total} | step {self._timeline_step_mode_label()}",
                True,
                (190, 200, 218),
            )
            self.screen.blit(timeline_label, (slider_x, slider_y - 18))
        elif mode in ("range", "range_hub", "range_hub_3d_fit"):
            slider_x = self._next_button_rect.right + 240
            slider_w = max(120, rect.right - slider_x - 18)
            slider_h = 6
            slider_y = by + (btn_h // 2) - (slider_h // 2)
            self._range_slider_rect = pg.Rect(slider_x, slider_y, slider_w, slider_h)
            pg.draw.rect(self.screen, (92, 92, 98), self._range_slider_rect)
            if self.range_top_n_max <= self.range_top_n_min:
                knob_ratio = 0.0
            else:
                knob_ratio = (float(self.range_top_n) - float(self.range_top_n_min)) / float(
                    self.range_top_n_max - self.range_top_n_min
                )
            knob_x = self._range_slider_rect.x + int(knob_ratio * self._range_slider_rect.width)
            pg.draw.circle(self.screen, (228, 228, 228), (knob_x, self._range_slider_rect.centery), 7)
            slider_label = self.tiny.render(
                f"Top points/master: {int(self.range_top_n)} / {int(self.range_top_n_max)}",
                True,
                (190, 200, 218),
            )
            self.screen.blit(slider_label, (slider_x, slider_y - 18))

        env_slider_x = self._next_button_rect.right + 200
        env_slider_w = max(200, rect.right - env_slider_x - 16)
        env_slider_y = rect.bottom - 16
        env_slider_h = 6
        self._env_range_slider_rect = pg.Rect(env_slider_x, env_slider_y, env_slider_w, env_slider_h)
        pg.draw.rect(self.screen, (92, 92, 98), self._env_range_slider_rect)
        min_knob_x = self._env_slider_handle_x("min")
        max_knob_x = self._env_slider_handle_x("max")
        if max_knob_x < min_knob_x:
            min_knob_x, max_knob_x = max_knob_x, min_knob_x
        if max_knob_x > min_knob_x:
            pg.draw.rect(
                self.screen,
                (126, 166, 220),
                (min_knob_x, self._env_range_slider_rect.y, max(1, max_knob_x - min_knob_x), self._env_range_slider_rect.height),
            )
        pg.draw.circle(self.screen, (185, 225, 255), (min_knob_x, self._env_range_slider_rect.centery), 7)
        pg.draw.circle(self.screen, (236, 214, 164), (max_knob_x, self._env_range_slider_rect.centery), 7)
        low = min(float(self.env_view_min), float(self.env_view_max))
        high = max(float(self.env_view_min), float(self.env_view_max))
        env_label = self.tiny.render(
            f"Env range shown: {low:.3f} to {high:.3f}",
            True,
            (190, 200, 218),
        )
        self.screen.blit(env_label, (env_slider_x, env_slider_y - 18))

    def _draw_rows_loading_bar(self, x: int, y: int, width: int, height: int) -> None:
        pg = self.pg
        ratio = self._loader_rows_progress_ratio()
        eta_text = self._eta_text(self._loader_rows_done, self._loader_rows_total, self._loader_rows_started_at)
        filled_w = int(max(0.0, min(1.0, ratio)) * float(max(1, width)))
        track = pg.Rect(int(x), int(y), int(width), int(height))
        fill = pg.Rect(int(x), int(y), int(filled_w), int(height))
        pg.draw.rect(self.screen, (34, 38, 46), track)
        if self._loader_error:
            fill_color = (196, 82, 82)
        else:
            fill_color = (102, 170, 238)
        if fill.width > 0:
            pg.draw.rect(self.screen, fill_color, fill)
        pg.draw.rect(self.screen, (96, 108, 126), track, 1)
        percent_text = f"{ratio * 100.0:.1f}%"
        detail_text = self._loader_rows_status if self._loader_rows_status else "Loading simulation rows..."
        label = (
            f"Simulation loading: {percent_text} "
            f"({self._loader_rows_done}/{self._loader_rows_total})  {eta_text}  {detail_text}"
        )
        color = (230, 140, 140) if self._loader_error else (188, 206, 232)
        self.screen.blit(
            self.tiny.render(hr._fit_text(self.tiny, label, max(32, width)), True, color),
            (int(x), int(y) - 16),
        )

    def _draw_timeline_loading_bar(self, x: int, y: int, width: int, height: int) -> None:
        pg = self.pg
        ratio = self._loader_timeline_progress_ratio()
        eta_text = self._eta_text(
            self._loader_timeline_done,
            self._loader_timeline_total,
            self._loader_timeline_started_at,
        )
        filled_w = int(max(0.0, min(1.0, ratio)) * float(max(1, width)))
        track = pg.Rect(int(x), int(y), int(width), int(height))
        fill = pg.Rect(int(x), int(y), int(filled_w), int(height))
        pg.draw.rect(self.screen, (34, 38, 46), track)
        if self._loader_error:
            fill_color = (196, 82, 82)
        else:
            fill_color = (90, 182, 176)
        if fill.width > 0:
            pg.draw.rect(self.screen, fill_color, fill)
        pg.draw.rect(self.screen, (96, 108, 126), track, 1)
        percent_text = f"{ratio * 100.0:.1f}%"
        detail_text = self._loader_timeline_status if self._loader_timeline_status else "Loading timeline cache..."
        label = (
            f"Timeline loading: {percent_text} "
            f"({self._loader_timeline_done}/{self._loader_timeline_total})  {eta_text}  {detail_text}"
        )
        color = (230, 140, 140) if self._loader_error else (178, 224, 220)
        self.screen.blit(
            self.tiny.render(hr._fit_text(self.tiny, label, max(32, width)), True, color),
            (int(x), int(y) - 16),
        )

    def _draw_master_refresh_bar(self, x: int, y: int, width: int, height: int) -> None:
        pg = self.pg
        ratio = self._master_refresh_progress_ratio()
        eta_text = self._eta_text(
            self._master_refresh_done_steps,
            self._master_refresh_total_steps,
            self._master_refresh_started_at,
        )
        filled_w = int(max(0.0, min(1.0, ratio)) * float(max(1, width)))
        track = pg.Rect(int(x), int(y), int(width), int(height))
        fill = pg.Rect(int(x), int(y), int(filled_w), int(height))
        pg.draw.rect(self.screen, (34, 38, 46), track)
        fill_color = (196, 82, 82) if self._master_refresh_error else (130, 188, 110)
        if fill.width > 0:
            pg.draw.rect(self.screen, fill_color, fill)
        pg.draw.rect(self.screen, (96, 108, 126), track, 1)
        percent_text = f"{ratio * 100.0:.1f}%"
        detail_text = self._master_refresh_status if self._master_refresh_status else "Refreshing all master fits..."
        label = (
            f"Refresh all master fits: {percent_text} "
            f"({self._master_refresh_done_steps}/{self._master_refresh_total_steps})  {eta_text}  {detail_text}"
        )
        color = (230, 140, 140) if self._master_refresh_error else (186, 224, 178)
        self.screen.blit(
            self.tiny.render(hr._fit_text(self.tiny, label, max(32, width)), True, color),
            (int(x), int(y) - 16),
        )

    def _draw(self, include_status: bool = True) -> None:
        pg = self.pg
        self.screen.fill((10, 12, 16))
        self._equation_copy_hits = []

        margin = 16
        gap = 12
        show_loader_rows = bool(
            self._loader_error
            or self._loader_rows_active
            or (int(self._loader_rows_done) < int(self._loader_rows_total))
        )
        show_loader_timeline = bool(
            self._loader_timeline_active
            or (int(self._loader_timeline_done) < int(self._loader_timeline_total))
        )
        show_refresh = bool(
            self._master_refresh_error
            or self._master_refresh_active
            or (int(self._master_refresh_done_steps) < int(self._master_refresh_total_steps))
        )
        status_bar_count = int(show_loader_rows) + int(show_loader_timeline) + int(show_refresh)
        top_y = 72 + (24 * int(status_bar_count))
        left_w = 486
        left_rect = pg.Rect(margin, top_y, left_w, self.window_h - top_y - margin)

        right_x = left_rect.right + gap
        right_w = self.window_w - right_x - margin
        right_h = self.window_h - top_y - margin
        hub_h = max(350, int(right_h * 0.66))
        hub_rect = pg.Rect(right_x, top_y, right_w, hub_h)

        lower_y = hub_rect.bottom + gap
        lower_h = max(120, self.window_h - lower_y - margin)
        controls_h = 68
        controls_rect = pg.Rect(right_x, lower_y, right_w, controls_h)
        detail_h = max(100, lower_h - controls_h - 8)
        detail_rect = pg.Rect(right_x, controls_rect.bottom + 8, right_w, detail_h)

        hub_status = str(self.hub_meta.get("status", "--")) if isinstance(self.hub_meta, dict) else "--"
        complete_count = len([r for r in self.rows if str(r.get("status", "")) == "ok"])
        title = f"HUB VIEWER {self.hub_dir.name}    status: {hub_status.upper()}    sims: {complete_count}/{len(self.rows)}"
        self.screen.blit(self.font.render(title, True, (226, 226, 226)), (margin, 14))
        fps_val = self.clock.get_fps()
        if not hr._is_number(fps_val) or (not math.isfinite(float(fps_val))):
            fps_val = 0.0
        fps_text = f"FPS: {float(fps_val):.1f}"
        fps_surface = self.tiny.render(fps_text, True, (168, 176, 191))
        self.screen.blit(
            fps_surface,
            (self.window_w - margin - fps_surface.get_width(), 18),
        )
        subtitle = (
            f"Path: {self.hub_dir}    Last reload: {time.strftime('%H:%M:%S', time.localtime(self.last_reload))}    "
            "Controls: click row or Up/Down to select, wheel/Page scroll, U refresh all master fits, S selector, G/Settings button edits settings, N/Norm button cycles normalization (None/Range/Sum), I/Step button toggles increment (0.001/0.01), Left/Right or Back/Next to rotate graph modes, E/Export (2D=.png, 3D=.stl+.png, timeline=.mov), Hub 3D modes: drag rotate + wheel/+/− zoom, timeline has Load/Step/Last/Play/Next + slider, env range slider has two handles, copy buttons beside equations, Esc/Q quit"
        )
        self.screen.blit(self.tiny.render(hr._fit_text(self.tiny, subtitle, self.window_w - (2 * margin)), True, (168, 176, 191)), (margin, 44))
        if include_status and self.export_status:
            status_color = (172, 226, 178) if self._export_status_ok else (255, 164, 164)
            status_text = hr._fit_text(self.tiny, self.export_status, self.window_w - (2 * margin))
            self.screen.blit(self.tiny.render(status_text, True, status_color), (margin, 58))
        if include_status:
            bar_index = 0
            if show_loader_rows:
                self._draw_rows_loading_bar(margin, 76 + (16 * bar_index), self.window_w - (2 * margin), 12)
                bar_index += 1
            if show_loader_timeline:
                self._draw_timeline_loading_bar(margin, 76 + (16 * bar_index), self.window_w - (2 * margin), 12)
                bar_index += 1
            if show_refresh:
                self._draw_master_refresh_bar(margin, 76 + (16 * bar_index), self.window_w - (2 * margin), 12)

        self._draw_table(left_rect)
        selected = self._selected_row()
        mode = self._active_graph_mode()
        self._hub_graph_rect = hub_rect
        if mode == "timeline_hub":
            self._draw_hub_timeline(hub_rect)
        elif mode == "hub_3d":
            self._draw_hub_graph_3d(hub_rect, title_text="Hub 3D")
        elif mode == "hub_3d_evo_fit_env":
            self._draw_hub_graph_3d(
                hub_rect,
                axis_keys=("y", "fitness", "x"),
                axis_labels=("evo speed", "fitness", "env rate"),
                mapping_text="x=evo speed, y=fitness, z=env rate",
                title_text="Hub 3D (Evo/Fit/Env)",
            )
        elif mode == "range_hub_3d_fit":
            self._draw_range_hub_graph_3d(hub_rect)
        elif mode == "master_fit_lines_3d":
            self._draw_master_fit_lines_3d(hub_rect)
        elif mode == "spectrum":
            self._draw_spectrum_graph(hub_rect)
        elif mode == "range_hub":
            self._draw_range_hub_graph(hub_rect)
        elif mode == "range":
            self._draw_range_graph(hub_rect)
        else:
            self._draw_hub_graph(hub_rect)
        self._draw_selected_scatter(detail_rect, selected)
        self._draw_graph_controls(controls_rect)

        pg.display.flip()

    def _handle_click(self, pos: tuple[int, int]) -> None:
        mx, my = pos
        if self._handle_equation_copy_click(int(mx), int(my)):
            return
        if self._table_scrollbar_track_rect is not None and self._table_scrollbar_track_rect.collidepoint(mx, my):
            self._table_scrollbar_dragging = True
            if (
                self._table_scrollbar_thumb_rect is not None
                and self._table_scrollbar_thumb_rect.collidepoint(mx, my)
            ):
                self._table_scrollbar_drag_offset = int(my - self._table_scrollbar_thumb_rect.y)
            else:
                if self._table_scrollbar_thumb_rect is not None:
                    self._table_scrollbar_drag_offset = int(self._table_scrollbar_thumb_rect.height // 2)
                else:
                    self._table_scrollbar_drag_offset = 0
                self._set_table_scroll_from_thumb_mouse(int(my))
            return
        if self._table_rect is not None and self._table_rect.collidepoint(mx, my):
            for row_idx, y0, y1 in self.table_row_hits:
                if y0 <= my < y1:
                    next_idx = int(row_idx)
                    if self.selected_row_index != next_idx:
                        self._selected_scatter_selected = None
                    self.selected_row_index = next_idx
                    self._queue_selected_row_load()
                    return
            # Clicked selector panel but not on a row: clear selection.
            self.selected_row_index = None
            self._selected_scatter_selected = None
            self._stop_selected_loader(wait=False)
            return
        if self._back_button_rect is not None and self._back_button_rect.collidepoint(mx, my):
            self._rotate_graph_mode(-1)
            return
        if self._next_button_rect is not None and self._next_button_rect.collidepoint(mx, my):
            self._rotate_graph_mode(1)
            return
        if self._export_button_rect is not None and self._export_button_rect.collidepoint(mx, my):
            self._export_active_mode()
            return
        if self._settings_button_rect is not None and self._settings_button_rect.collidepoint(mx, my):
            self._open_settings_dialog()
            return
        if self._normalize_button_rect is not None and self._normalize_button_rect.collidepoint(mx, my):
            self._cycle_normalize_mode()
            self._selected_scatter_selected = None
            return
        if self._increment_button_rect is not None and self._increment_button_rect.collidepoint(mx, my):
            self._cycle_increment_mode()
            return
        if self._timeline_load_button_rect is not None and self._timeline_load_button_rect.collidepoint(mx, my):
            self._start_timeline_cache_loader()
            return
        if (
            self._timeline_step_mode_button_rect is not None
            and self._timeline_step_mode_button_rect.collidepoint(mx, my)
        ):
            self._toggle_timeline_step_mode()
            return
        if self._timeline_prev_button_rect is not None and self._timeline_prev_button_rect.collidepoint(mx, my):
            self.timeline_playing = False
            self._step_timeline_frame(-1)
            return
        if self._timeline_play_button_rect is not None and self._timeline_play_button_rect.collidepoint(mx, my):
            if self.timeline_playing:
                self.timeline_playing = False
            else:
                if self.timeline_progress >= 1.0:
                    self.timeline_progress = 0.0
                self.timeline_playing = True
            return
        if self._timeline_next_button_rect is not None and self._timeline_next_button_rect.collidepoint(mx, my):
            self.timeline_playing = False
            self._step_timeline_frame(1)
            return
        if self._timeline_slider_rect is not None and self._timeline_slider_rect.collidepoint(mx, my):
            self.timeline_playing = False
            self._timeline_slider_dragging = True
            self._set_timeline_slider_from_mouse(mx)
            return
        if self._range_slider_rect is not None and self._range_slider_rect.collidepoint(mx, my):
            self._range_slider_dragging = True
            self._set_range_slider_from_mouse(mx)
            return
        if self._env_range_slider_rect is not None:
            hit_rect = self._env_range_slider_rect.inflate(0, 18)
            if hit_rect.collidepoint(mx, my):
                min_knob_x = self._env_slider_handle_x("min")
                max_knob_x = self._env_slider_handle_x("max")
                center_y = self._env_range_slider_rect.centery
                if abs(mx - min_knob_x) <= 10 and abs(my - center_y) <= 12:
                    handle = "min"
                elif abs(mx - max_knob_x) <= 10 and abs(my - center_y) <= 12:
                    handle = "max"
                else:
                    handle = "min" if abs(mx - min_knob_x) <= abs(mx - max_knob_x) else "max"
                self._env_range_drag_handle = handle
                self._set_env_slider_from_mouse(mx, handle)
                return
        if self._handle_selected_scatter_click(int(mx), int(my)):
            return
        mode = self._active_graph_mode()
        if self._is_3d_mode(mode) and self._hub_graph_rect is not None and self._hub_graph_rect.collidepoint(mx, my):
            self._graph3d_dragging = True
            self._graph3d_last_mouse = (int(mx), int(my))
            return
        # Clicked outside selector rows/controls: clear selection.
        self.selected_row_index = None
        self._selected_scatter_selected = None
        self._stop_selected_loader(wait=False)

    def _open_selector(self) -> None:
        self._stop_master_refresh(wait=False)
        choice = hr._select_hub_run_ui(self.results_root)
        if not isinstance(choice, dict):
            return
        if str(choice.get("mode")) != "continue":
            return
        hub_dir = choice.get("hub_dir")
        hub_idx = _safe_int(choice.get("hub_idx"))
        if not isinstance(hub_dir, Path) or (not hub_dir.is_dir()):
            return
        self.hub_dir = hub_dir
        self.hub_idx = hub_idx
        self.selected_row_index = None
        self._selected_scatter_selected = None
        self._stop_selected_loader(wait=False)
        self.env_view_min = None
        self.env_view_max = None
        self.reload_from_disk()

    def _open_settings_dialog(self) -> None:
        try:
            import master_simulations as ms

            settings = load_settings()
            updated = ms._edit_settings_ui(settings, master_dir=None, write_global_on_confirm=True)
            if updated is None:
                self._set_export_status(False, "Settings not changed")
            else:
                self._set_export_status(True, "Settings saved to settings.json")
        except Exception as exc:
            self._set_export_status(False, f"Settings dialog failed ({exc})")
        # Restore hub viewer window after settings UI closes.
        self.screen = self.pg.display.set_mode((self.window_w, self.window_h))
        label = f"Hub Viewer - hub_{self.hub_idx}" if self.hub_idx is not None else f"Hub Viewer - {self.hub_dir.name}"
        self.pg.display.set_caption(label)

    def run(self) -> None:
        plus_keys = {self.pg.K_EQUALS, self.pg.K_KP_PLUS}
        k_plus = getattr(self.pg, "K_PLUS", None)
        if k_plus is not None:
            plus_keys.add(k_plus)
        minus_keys = {self.pg.K_MINUS, self.pg.K_KP_MINUS}
        while self.running:
            for event in self.pg.event.get():
                if event.type == self.pg.QUIT:
                    self.running = False
                elif event.type == self.pg.KEYDOWN:
                    if event.key in (self.pg.K_ESCAPE, self.pg.K_q):
                        self.running = False
                    elif event.key == self.pg.K_u:
                        self._refresh_all_master_fit_lines()
                    elif event.key == self.pg.K_s:
                        self._open_selector()
                    elif event.key == self.pg.K_g:
                        self._open_settings_dialog()
                    elif event.key == self.pg.K_n:
                        self._cycle_normalize_mode()
                    elif event.key == self.pg.K_i:
                        self._cycle_increment_mode()
                    elif event.key == self.pg.K_LEFT:
                        self._rotate_graph_mode(-1)
                    elif event.key == self.pg.K_RIGHT:
                        self._rotate_graph_mode(1)
                    elif event.key == self.pg.K_UP:
                        self._move_selected_row(-1)
                    elif event.key == self.pg.K_DOWN:
                        self._move_selected_row(1)
                    elif event.key == self.pg.K_PAGEUP:
                        self.table_scroll = max(0.0, self.table_scroll - 12.0)
                    elif event.key == self.pg.K_PAGEDOWN:
                        self.table_scroll += 12.0
                    elif event.key == self.pg.K_HOME:
                        self.table_scroll = 0.0
                    elif event.key == self.pg.K_END:
                        self.table_scroll = 1e9
                    elif event.key == self.pg.K_SPACE:
                        mode = self._active_graph_mode()
                        if mode == "timeline_hub":
                            if self.timeline_progress >= 1.0 and (not self.timeline_playing):
                                self.timeline_progress = 0.0
                            self.timeline_playing = not self.timeline_playing
                    elif event.key == self.pg.K_e:
                        self._export_active_mode()
                    elif event.key in plus_keys:
                        if self._is_3d_mode(self._active_graph_mode()):
                            self._adjust_graph3d_zoom(1.0)
                    elif event.key in minus_keys:
                        if self._is_3d_mode(self._active_graph_mode()):
                            self._adjust_graph3d_zoom(-1.0)
                elif event.type == self.pg.MOUSEWHEEL:
                    mode = self._active_graph_mode()
                    mx, my = self.pg.mouse.get_pos()
                    if (
                        self._is_3d_mode(mode)
                        and self._hub_graph_rect is not None
                        and self._hub_graph_rect.collidepoint(int(mx), int(my))
                        and event.y != 0
                    ):
                        self._adjust_graph3d_zoom(float(event.y))
                    elif event.y > 0:
                        self.table_scroll = max(0.0, self.table_scroll - 2.0)
                    elif event.y < 0:
                        self.table_scroll += 2.0
                elif event.type == self.pg.MOUSEBUTTONDOWN and event.button == 1:
                    self._handle_click(event.pos)
                elif event.type == self.pg.MOUSEBUTTONUP and event.button == 1:
                    self._range_slider_dragging = False
                    self._timeline_slider_dragging = False
                    self._env_range_drag_handle = None
                    self._graph3d_dragging = False
                    self._table_scrollbar_dragging = False
                    self._graph3d_last_mouse = None
                elif event.type == self.pg.MOUSEMOTION:
                    if self._range_slider_dragging:
                        self._set_range_slider_from_mouse(int(event.pos[0]))
                    if self._timeline_slider_dragging:
                        self._set_timeline_slider_from_mouse(int(event.pos[0]))
                    if isinstance(self._env_range_drag_handle, str):
                        self._set_env_slider_from_mouse(int(event.pos[0]), self._env_range_drag_handle)
                    if self._table_scrollbar_dragging:
                        self._set_table_scroll_from_thumb_mouse(int(event.pos[1]))
                    if self._graph3d_dragging:
                        if self._graph3d_last_mouse is None:
                            self._graph3d_last_mouse = (int(event.pos[0]), int(event.pos[1]))
                        else:
                            last_x, last_y = self._graph3d_last_mouse
                            dx = int(event.pos[0]) - int(last_x)
                            dy = int(event.pos[1]) - int(last_y)
                            self._graph3d_yaw += float(dx) * 0.012
                            self._graph3d_pitch += float(dy) * 0.010
                            self._graph3d_pitch = max(-1.25, min(1.25, float(self._graph3d_pitch)))
                            if self._graph3d_yaw > math.pi:
                                self._graph3d_yaw -= (2.0 * math.pi)
                            elif self._graph3d_yaw < -math.pi:
                                self._graph3d_yaw += (2.0 * math.pi)
                            self._graph3d_last_mouse = (int(event.pos[0]), int(event.pos[1]))

            self._drain_background_loader_updates()
            self._drain_selected_loader_updates()
            self._drain_master_refresh_updates()
            self._draw()
            dt_s = self.clock.tick(30) / 1000.0
            self._update_timeline_playback(dt_s)

        try:
            self._stop_master_refresh(wait=False)
            self._stop_background_loader(wait=False)
            self._stop_selected_loader(wait=False)
            self.pg.display.quit()
            self.pg.quit()
        except Exception:
            pass


def main() -> None:
    args = _parse_args()
    results_root = Path(args.results_root).resolve()
    results_root.mkdir(parents=True, exist_ok=True)
    hub_idx, hub_dir = _resolve_hub_dir(args, results_root)
    hub_meta = _load_hub_meta(hub_dir)
    viewer = HubViewer(
        hub_idx=hub_idx,
        hub_dir=hub_dir,
        hub_meta=hub_meta,
        results_root=results_root,
    )
    viewer.run()


if __name__ == "__main__":
    main()
