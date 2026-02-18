#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import hub_runner as hr


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


def _resolve_hub_dir(args: argparse.Namespace, results_root: Path) -> tuple[int | None, Path]:
    if args.hub_dir:
        path = Path(args.hub_dir).expanduser().resolve()
        if not path.is_dir():
            raise SystemExit(f"Hub directory not found: {path}")
        idx = hr._parse_hub_id(path)
        return idx, path

    if args.hub_index is not None:
        idx = max(0, int(args.hub_index))
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


def _refresh_row_from_disk(row: dict) -> None:
    env_dir_raw = row.get("env_dir")
    if not env_dir_raw:
        return
    env_dir = Path(str(env_dir_raw))
    if not env_dir.is_dir():
        return

    master_dir = hr._latest_master_dir(env_dir)
    run_nums = hr._master_run_nums(master_dir) if master_dir is not None else []
    if not run_nums:
        run_nums = hr._discover_env_run_nums(env_dir)
    if master_dir is None and not run_nums:
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
        points = hr._master_points(master_dir, run_nums)
        row["master_dir"] = str(master_dir)
    else:
        master_run_num = None
        points = []
        for run_num in run_nums:
            points.extend(
                hr._extract_points_from_csv(
                    env_dir / str(run_num) / f"parsedArithmeticMeanSimulatino{run_num}_Log.csv"
                )
            )

    if not points:
        points = hr._snapshot_points_from_runs(env_dir, run_nums)

    fit = hr._fit_stitched_gaussian(points)
    if isinstance(fit, dict):
        apex_x = fit.get("apex_x")
        apex_y = fit.get("apex_y")
    else:
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


def _build_rows(hub_dir: Path, hub_meta: dict) -> list[dict]:
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
            "apex_fitness": _safe_float(step_info.get("apex_fitness")) if isinstance(step_info, dict) else None,
            "apex_evolution_rate": (
                _safe_float(step_info.get("apex_evolution_rate")) if isinstance(step_info, dict) else None
            ),
            "duration_s": _safe_float(step_info.get("duration_s")) if isinstance(step_info, dict) else None,
            "env_dir": str(env_dir),
            "master_dir": step_info.get("master_dir") if isinstance(step_info, dict) else None,
            "fit": step_info.get("fit") if isinstance(step_info.get("fit"), dict) else None,
            "points": [],
            "run_nums": step_info.get("run_nums") if isinstance(step_info.get("run_nums"), list) else [],
        }
        _refresh_row_from_disk(row)
        if row.get("master_run_num") is not None and row.get("status") in ("pending", "running", ""):
            row["status"] = "ok"
        rows.append(row)
    return rows


def _compute_hub_graph_points(rows: list[dict]) -> list[dict]:
    graph_points = []
    for row_idx, row in enumerate(rows):
        env_rate = row.get("env_rate")
        points = row.get("points")
        if not (hr._is_number(env_rate) and isinstance(points, list)):
            continue
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
    return graph_points


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


def _run_parsed_arithmetic_points(run_dir: Path, run_num: int) -> list[tuple[float, float]]:
    candidates = [
        run_dir / f"parsedArithmeticMeanSimulatino{run_num}_Log.csv",
        run_dir / f"parsedArithmeticMeanSimulationo{run_num}_Log.csv",
        run_dir / f"parsedArithmeticMeanSimulation{run_num}_Log.csv",
        run_dir / f"parsedArithmeticMeanSimulatin{run_num}_Log.csv",
        run_dir / f"parsedArithmeticMeanSimulatino{run_num}_log.csv",
        run_dir / f"parsedArithmeticMeanSimulationo{run_num}_log.csv",
        run_dir / f"parsedArithmeticMeanSimulation{run_num}_log.csv",
        run_dir / f"parsedArithmeticMeanSimulatin{run_num}_log.csv",
    ]
    for path in candidates:
        points = hr._extract_points_from_csv(path)
        if points:
            return points
    for candidate in sorted(run_dir.glob(f"parsedArithmeticMean*{run_num}_*.csv")):
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
        self.selected_row_index = None
        self.table_scroll = 0.0
        self.table_row_h = 24
        self.timeline_cache = {}
        self.graph_modes = ["normal", "timeline_hub", "spectrum", "range", "range_hub"]
        self.graph_mode_index = 0
        self._back_button_rect = None
        self._next_button_rect = None
        self._range_slider_rect = None
        self._range_slider_dragging = False
        self._timeline_prev_button_rect = None
        self._timeline_play_button_rect = None
        self._timeline_next_button_rect = None
        self._timeline_slider_rect = None
        self._timeline_slider_dragging = False
        self.timeline_progress = 0.0
        self.timeline_playing = False
        self.timeline_frame_count = 101
        self.timeline_play_frames_per_sec = 12.0
        self.range_top_n = 80
        self.range_top_n_min = 1
        self.range_top_n_max = 1
        self.last_reload = 0.0
        self.running = True

        self.reload_from_disk()

    def reload_from_disk(self) -> None:
        self.hub_meta = _load_hub_meta(self.hub_dir)
        self.rows = _build_rows(self.hub_dir, self.hub_meta)
        self.graph_points = _compute_hub_graph_points(self.rows)
        self.hub_fit_report = hr._fit_hub_models_from_rows(self.rows)
        # Timeline data is rebuilt only on manual reload (U), not every frame.
        self.timeline_cache.clear()
        for row in self.rows:
            key = self._timeline_cache_key_for_row(row)
            if key is None:
                continue
            self.timeline_cache[key] = self._build_timeline_payload_for_row(row)
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
        self.hub_best_fit = (
            self.hub_fit_report.get("best_model") if isinstance(self.hub_fit_report, dict) else None
        )
        if self.rows:
            if self.selected_row_index is None:
                self.selected_row_index = 0
            else:
                self.selected_row_index = max(0, min(int(self.selected_row_index), len(self.rows) - 1))
        else:
            self.selected_row_index = None
        self.timeline_progress = max(0.0, min(1.0, float(self.timeline_progress)))
        self.timeline_playing = False
        self.last_reload = time.time()

    def _selected_row(self) -> dict | None:
        if not self.rows:
            return None
        if self.selected_row_index is None:
            return None
        idx = max(0, min(int(self.selected_row_index), len(self.rows) - 1))
        self.selected_row_index = idx
        return self.rows[idx]

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
            snap_files = sorted(snap_dir.glob("arith_mean_*.csv"))
            if not snap_files:
                continue
            snapshot_files_by_run[int(run_num)] = snap_files

        all_points = []
        series = []
        cloud_series = []
        used_snapshots = False
        used_fallback = False
        for idx, run_num in enumerate(sorted(run_dirs.keys())):
            run_dir = run_dirs[int(run_num)]
            snap_files = snapshot_files_by_run.get(int(run_num), [])
            norm_points = []
            norm_cloud_samples = []
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
                    used_snapshots = True
            if not norm_points:
                parsed_points = _run_parsed_arithmetic_points(run_dir, int(run_num))
                apex_x, _ = _apex_from_points(parsed_points)
                if hr._is_number(apex_x):
                    norm_points = [(0.0, float(apex_x)), (1.0, float(apex_x))]
                    used_fallback = True
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
            if not norm_points:
                continue
            all_points.extend(norm_points)
            series.append(
                {
                    "run_num": int(run_num),
                    "color": _SIM_COLORS[idx % len(_SIM_COLORS)],
                    "points": norm_points,
                }
            )
            if norm_cloud_samples:
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

    def _timeline_payload_for_row(self, row: dict | None) -> dict:
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

    def _timeline_frame_index(self) -> int:
        frames = max(2, int(self.timeline_frame_count))
        idx = int(round(float(self.timeline_progress) * float(frames - 1)))
        return max(0, min(frames - 1, idx))

    def _step_timeline_frame(self, delta_steps: int) -> None:
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
            f"Rows {min(total, start_idx + 1)}-{min(total, start_idx + visible)} / {total}    U: reload",
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

    def _draw_hub_graph(self, rect) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (18, 20, 26), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)

        header_x = rect.x + 10
        header_w = max(60, rect.width - 20)
        best = self.hub_best_fit if isinstance(self.hub_best_fit, dict) else None
        if best and hr._is_number(best.get("r2")):
            equation = str(best.get("equation", ""))
            line_1 = hr._fit_text(self.tiny, f"Equation: {equation}", header_w)
            line_2 = hr._fit_text(self.tiny, f"R^2: {float(best.get('r2')):.4f}", header_w)
            col = (235, 210, 146)
        else:
            line_1 = "Equation: not enough data"
            line_2 = "R^2: --"
            col = (170, 170, 170)
        self.screen.blit(self.tiny.render(line_1, True, col), (header_x, rect.y + 8))
        self.screen.blit(self.tiny.render(line_2, True, col), (header_x, rect.y + 24))

        if not self.graph_points:
            msg = self.small.render("No hub graph points yet.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 12, rect.y + 50))
            self.hub_dot_hits = []
            return

        plot_top = rect.y + 48
        plot = pg.Rect(rect.x + 48, plot_top, rect.width - 64, rect.height - ((plot_top - rect.y) + 24))
        if plot.width <= 24 or plot.height <= 24:
            return
        pg.draw.rect(self.screen, (12, 14, 19), plot)
        pg.draw.rect(self.screen, (60, 66, 78), plot, 1)

        xs = [float(p["x"]) for p in self.graph_points if hr._is_number(p.get("x"))]
        ys = [float(p["y"]) for p in self.graph_points if hr._is_number(p.get("y"))]
        fits = [float(p["fitness"]) for p in self.graph_points if hr._is_number(p.get("fitness"))]
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
                    pg.draw.line(self.screen, (248, 196, 92), _to_px(sample[i - 1][0], sample[i - 1][1]), _to_px(sample[i][0], sample[i][1]), 2)

        self.hub_dot_hits = []
        selected_idx = int(self.selected_row_index) if self.selected_row_index is not None else -1
        for point in self.graph_points:
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

    def _draw_selected_scatter(self, rect, row: dict | None) -> None:
        pg = self.pg
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

        points = []
        raw_points = row.get("points")
        if isinstance(raw_points, list):
            for pair in raw_points:
                if not isinstance(pair, (tuple, list)) or len(pair) < 2:
                    continue
                if hr._is_number(pair[0]) and hr._is_number(pair[1]):
                    points.append((float(pair[0]), float(pair[1])))
        if not points:
            msg = self.small.render("No points available.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 10, rect.y + 30))
            return

        plot = pg.Rect(rect.x + 38, rect.y + 30, rect.width - 50, rect.height - 42)
        if plot.width <= 24 or plot.height <= 24:
            return
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
                        curve.append((float(xv), float(yv)))
                if len(curve) >= 2:
                    for i in range(1, len(curve)):
                        pg.draw.line(self.screen, (248, 196, 92), _to_px(curve[i - 1][0], curve[i - 1][1]), _to_px(curve[i][0], curve[i][1]), 2)

        y_min = min(ys)
        y_max = max(ys)
        den = max(1e-9, y_max - y_min)
        for x_val, y_val in points:
            n = max(0.0, min(1.0, (y_val - y_min) / den))
            radius = 1 + int(round(3 * n))
            color = (42 + int(210 * n), 128 + int(95 * n), 236 - int(166 * n))
            pg.draw.circle(self.screen, color, _to_px(x_val, y_val), radius)

        self.screen.blit(self.tiny.render(f"{raw_max_y:.3f}", True, (150, 150, 150)), (plot.x - 34, plot.y - 2))
        self.screen.blit(self.tiny.render(f"{raw_min_y:.3f}", True, (150, 150, 150)), (plot.x - 34, plot.bottom - 14))
        self.screen.blit(self.tiny.render("fitness", True, (155, 155, 155)), (plot.x - 34, plot.y + 14))
        self.screen.blit(self.tiny.render("evo speed", True, (155, 155, 155)), (plot.x + 5, plot.y + 4))

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
            text = hr._fit_text(self.tiny, f"Sim fit: {eq} | R^2={r2_text}", max(40, rect.width - 20))
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
            text = hr._fit_text(self.tiny, f"Master fit: {eq} | R^2={r2_text}", max(40, rect.width - 20))
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

        cursor_norm = max(0.0, min(1.0, float(self.timeline_progress)))
        graph_points = []
        env_values = []
        fallback_count = 0
        used_rows = 0
        max_samples = 2
        for row_idx, row in enumerate(self.rows):
            if not isinstance(row, dict):
                continue
            env_rate = row.get("env_rate")
            if not hr._is_number(env_rate):
                continue
            payload = self._timeline_payload_for_row(row)
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
            return

        fit_report = hr._fit_hub_models_from_graph_points(graph_points)
        best = fit_report.get("best_model") if isinstance(fit_report, dict) else None

        frame_idx = self._timeline_frame_index() + 1
        frame_total = max(2, int(self.timeline_frame_count))
        frame_lbl = self.tiny.render(f"frame {frame_idx}/{frame_total}", True, (170, 182, 202))
        self.screen.blit(frame_lbl, (rect.right - frame_lbl.get_width() - 10, rect.y + 10))

        note = f"rows with data: {used_rows}/{len(self.rows)} | points: {len(graph_points)}"
        if fallback_count > 0:
            note += f" | fallback rows: {fallback_count}"
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
                    pg.draw.line(
                        self.screen,
                        (248, 196, 92),
                        _to_px(sample[i - 1][0], sample[i - 1][1]),
                        _to_px(sample[i][0], sample[i][1]),
                        2,
                    )

        selected_idx = int(self.selected_row_index) if self.selected_row_index is not None else -1
        for item in graph_points:
            px, py = _to_px(float(item["x"]), float(item["y"]))
            n = max(0.0, min(1.0, (float(item["fitness"]) - fit_min) / fit_den))
            radius = max(1, int(round(4 * n)))
            color = (35 + int(220 * n), 128 + int(95 * n), 236 - int(166 * n))
            if int(item.get("row_index", -1)) == selected_idx:
                pg.draw.circle(self.screen, (255, 255, 255), (px, py), radius + 3, 1)
            pg.draw.circle(self.screen, color, (px, py), radius)

        if isinstance(best, dict) and hr._is_number(best.get("r2")):
            equation = str(best.get("equation", ""))
            line_1 = hr._fit_text(self.tiny, f"Equation: {equation}", max(60, rect.width - 20))
            line_2 = hr._fit_text(self.tiny, f"R^2: {float(best.get('r2')):.4f}", max(60, rect.width - 20))
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
        for row in self.rows:
            if not isinstance(row, dict):
                continue
            env_rate = row.get("env_rate")
            points = row.get("points")
            if (not hr._is_number(env_rate)) or (not isinstance(points, list)):
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
            masters.append(
                {
                    "env_rate": float(env_rate),
                    "points": numeric_points,
                    "fit": row.get("fit"),
                }
            )
            all_points.extend((float(env_rate), p[0], p[1]) for p in numeric_points)
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
                        curve.append((float(xv), float(yv)))
                if len(curve) >= 2:
                    for i in range(1, len(curve)):
                        pg.draw.line(
                            plot_layer,
                            (curve_color[0], curve_color[1], curve_color[2], alpha),
                            _to_px(curve[i - 1][0], curve[i - 1][1]),
                            _to_px(curve[i][0], curve[i][1]),
                            2,
                        )

        for master in masters:
            env_rate = float(master.get("env_rate", 0.0))
            env_color = self._env_color(env_rate, env_min, env_max)
            env_norm = 0.5 if env_max <= env_min else max(0.0, min(1.0, (env_rate - env_min) / (env_max - env_min)))
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

        self.screen.blit(plot_layer, (plot.x, plot.y))

        self.screen.blit(self.tiny.render(f"{raw_max_y:.3f}", True, (150, 150, 150)), (plot.x - 34, plot.y - 2))
        self.screen.blit(self.tiny.render(f"{raw_min_y:.3f}", True, (150, 150, 150)), (plot.x - 34, plot.bottom - 14))
        self.screen.blit(self.tiny.render(f"{raw_min_x:.3f}", True, (150, 150, 150)), (plot.x, plot.bottom + 2))
        max_x_txt = self.tiny.render(f"{raw_max_x:.3f}", True, (150, 150, 150))
        self.screen.blit(max_x_txt, (plot.right - max_x_txt.get_width(), plot.bottom + 2))
        self.screen.blit(self.tiny.render("evo speed", True, (155, 155, 155)), (plot.x + 4, plot.y + 4))
        self.screen.blit(self.tiny.render("fitness", True, (155, 155, 155)), (plot.x - 34, plot.y + 14))

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

    def _draw_range_hub_graph(self, rect) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (18, 20, 26), rect)
        pg.draw.rect(self.screen, (74, 80, 94), rect, 1)

        filtered_rows = []
        graph_points = []
        top_n = max(1, int(self.range_top_n))
        for row_idx, row in enumerate(self.rows):
            if not isinstance(row, dict):
                continue
            env_rate = row.get("env_rate")
            points = row.get("points")
            if (not hr._is_number(env_rate)) or (not isinstance(points, list)):
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

        header_x = rect.x + 10
        header_w = max(60, rect.width - 20)
        if isinstance(best, dict) and hr._is_number(best.get("r2")):
            equation = str(best.get("equation", ""))
            line_1 = hr._fit_text(self.tiny, f"Equation: {equation}", header_w)
            line_2 = hr._fit_text(self.tiny, f"R^2: {float(best.get('r2')):.4f}", header_w)
            col = (235, 210, 146)
        else:
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
                    pg.draw.line(
                        self.screen,
                        (248, 196, 92),
                        _to_px(sample[i - 1][0], sample[i - 1][1]),
                        _to_px(sample[i][0], sample[i][1]),
                        2,
                    )

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
        by = rect.y + (rect.height - btn_h) // 2
        self._back_button_rect = pg.Rect(rect.x + 10, by, btn_w, btn_h)
        self._next_button_rect = pg.Rect(self._back_button_rect.right + 8, by, btn_w, btn_h)
        for button_rect, label in ((self._back_button_rect, "Back"), (self._next_button_rect, "Next")):
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

        mode_labels = {
            "normal": "Normal",
            "timeline_hub": "Hub Timeline",
            "spectrum": "Spectrum+Curves",
            "range": "Range",
            "range_hub": "Range Hub",
        }
        mode_text = f"Mode {self.graph_mode_index + 1}/{len(self.graph_modes)}: {mode_labels.get(mode, mode)}"
        self.screen.blit(self.small.render(mode_text, True, (205, 215, 230)), (self._next_button_rect.right + 14, by + 5))

        self._timeline_prev_button_rect = None
        self._timeline_play_button_rect = None
        self._timeline_next_button_rect = None
        self._timeline_slider_rect = None
        self._range_slider_rect = None
        if mode == "timeline_hub":
            btn_w = 92
            btn_h = 26
            btn_gap = 6
            slider_w = max(170, min(320, rect.width - 730))
            slider_h = 6
            slider_x = rect.right - slider_w - 16
            slider_y = rect.centery - (slider_h // 2)
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
                f"Frame: {frame_idx}/{frame_total}",
                True,
                (190, 200, 218),
            )
            self.screen.blit(timeline_label, (slider_x, slider_y - 18))
        elif mode in ("range", "range_hub"):
            slider_x = self._next_button_rect.right + 240
            slider_w = max(120, rect.right - slider_x - 18)
            slider_h = 6
            slider_y = rect.centery - (slider_h // 2)
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

    def _draw(self) -> None:
        pg = self.pg
        self.screen.fill((10, 12, 16))

        margin = 16
        gap = 12
        top_y = 72
        left_w = 486
        left_rect = pg.Rect(margin, top_y, left_w, self.window_h - top_y - margin)

        right_x = left_rect.right + gap
        right_w = self.window_w - right_x - margin
        right_h = self.window_h - top_y - margin
        hub_h = max(350, int(right_h * 0.66))
        hub_rect = pg.Rect(right_x, top_y, right_w, hub_h)

        lower_y = hub_rect.bottom + gap
        lower_h = max(120, self.window_h - lower_y - margin)
        controls_h = 42
        controls_rect = pg.Rect(right_x, lower_y, right_w, controls_h)
        detail_h = max(100, lower_h - controls_h - 8)
        detail_rect = pg.Rect(right_x, controls_rect.bottom + 8, right_w, detail_h)

        hub_status = str(self.hub_meta.get("status", "--")) if isinstance(self.hub_meta, dict) else "--"
        complete_count = len([r for r in self.rows if str(r.get("status", "")) == "ok"])
        title = f"HUB VIEWER {self.hub_dir.name}    status: {hub_status.upper()}    sims: {complete_count}/{len(self.rows)}"
        self.screen.blit(self.font.render(title, True, (226, 226, 226)), (margin, 14))
        subtitle = (
            f"Path: {self.hub_dir}    Last reload: {time.strftime('%H:%M:%S', time.localtime(self.last_reload))}    "
            "Controls: click row to select, wheel scroll, U reload, S selector, Left/Right or Back/Next to rotate graph modes, timeline has Last/Play/Next + slider, Esc/Q quit"
        )
        self.screen.blit(self.tiny.render(hr._fit_text(self.tiny, subtitle, self.window_w - (2 * margin)), True, (168, 176, 191)), (margin, 44))

        self._draw_table(left_rect)
        selected = self._selected_row()
        mode = self._active_graph_mode()
        if mode == "timeline_hub":
            self._draw_hub_timeline(hub_rect)
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
        if self._table_rect is not None and self._table_rect.collidepoint(mx, my):
            for row_idx, y0, y1 in self.table_row_hits:
                if y0 <= my < y1:
                    self.selected_row_index = int(row_idx)
                    return
            # Clicked selector panel but not on a row: clear selection.
            self.selected_row_index = None
            return
        if self._back_button_rect is not None and self._back_button_rect.collidepoint(mx, my):
            self._rotate_graph_mode(-1)
            return
        if self._next_button_rect is not None and self._next_button_rect.collidepoint(mx, my):
            self._rotate_graph_mode(1)
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
        # Clicked outside selector rows/controls: clear selection.
        self.selected_row_index = None

    def _open_selector(self) -> None:
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
        self.reload_from_disk()

    def run(self) -> None:
        while self.running:
            for event in self.pg.event.get():
                if event.type == self.pg.QUIT:
                    self.running = False
                elif event.type == self.pg.KEYDOWN:
                    if event.key in (self.pg.K_ESCAPE, self.pg.K_q):
                        self.running = False
                    elif event.key == self.pg.K_u:
                        self.reload_from_disk()
                    elif event.key == self.pg.K_s:
                        self._open_selector()
                    elif event.key == self.pg.K_LEFT:
                        self._rotate_graph_mode(-1)
                    elif event.key == self.pg.K_RIGHT:
                        self._rotate_graph_mode(1)
                    elif event.key == self.pg.K_UP:
                        self.table_scroll = max(0.0, self.table_scroll - 1.0)
                    elif event.key == self.pg.K_DOWN:
                        self.table_scroll += 1.0
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
                elif event.type == self.pg.MOUSEWHEEL:
                    if event.y > 0:
                        self.table_scroll = max(0.0, self.table_scroll - 2.0)
                    elif event.y < 0:
                        self.table_scroll += 2.0
                elif event.type == self.pg.MOUSEBUTTONDOWN and event.button == 1:
                    self._handle_click(event.pos)
                elif event.type == self.pg.MOUSEBUTTONUP and event.button == 1:
                    self._range_slider_dragging = False
                    self._timeline_slider_dragging = False
                elif event.type == self.pg.MOUSEMOTION:
                    if self._range_slider_dragging:
                        self._set_range_slider_from_mouse(int(event.pos[0]))
                    if self._timeline_slider_dragging:
                        self._set_timeline_slider_from_mouse(int(event.pos[0]))

            self._draw()
            dt_s = self.clock.tick(30) / 1000.0
            self._update_timeline_playback(dt_s)

        try:
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
