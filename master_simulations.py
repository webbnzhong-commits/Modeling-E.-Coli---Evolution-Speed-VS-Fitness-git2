import argparse
import csv
from collections import deque
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import pygame

from settings_manager import load_settings
from simulatino_parser import parse_run

_SIM_COLORS = [
    (0, 200, 255),
    (255, 180, 0),
    (0, 220, 120),
    (220, 80, 80),
    (180, 120, 255),
    (120, 200, 200),
    (255, 120, 200),
    (120, 255, 160),
    (255, 220, 120),
]
_DOT_ALPHA = 128

_MASTER_HEADER = [
    "evolution rate",
    "length lived",
    "species population time",
    "population",
]


def _sim_color(idx: int):
    return _SIM_COLORS[idx % len(_SIM_COLORS)]


def _format_duration(seconds: float) -> str:
    try:
        total = float(seconds)
    except Exception:
        return "--:--:--"
    if total < 0:
        total = 0.0
    hours = int(total // 3600)
    minutes = int((total % 3600) // 60)
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:05.2f}"


def _apply_draw_toggle(selected_row: int, draw_modes: list[int]) -> None:
    if not draw_modes:
        return
    if selected_row == 0:
        new_mode = (draw_modes[0] + 1) % 3
        for i in range(len(draw_modes)):
            draw_modes[i] = new_mode
    else:
        idx = selected_row - 1
        if 0 <= idx < len(draw_modes):
            draw_modes[idx] = (draw_modes[idx] + 1) % 3


def _apply_mode_toggle(selected_row: int, mode_values: list[int]) -> None:
    if not mode_values:
        return
    if selected_row == 0:
        new_mode = (mode_values[0] + 1) % 3
        for i in range(len(mode_values)):
            mode_values[i] = new_mode
    else:
        idx = selected_row - 1
        if 0 <= idx < len(mode_values):
            mode_values[idx] = (mode_values[idx] + 1) % 3


def _apply_update(selected_row: int, update_tokens: list[int]) -> None:
    if not update_tokens:
        return
    if selected_row == 0:
        for i in range(len(update_tokens)):
            update_tokens[i] += 1
    else:
        idx = selected_row - 1
        if 0 <= idx < len(update_tokens):
            update_tokens[idx] += 1


def _write_control(
    control_path: Path,
    active: int,
    enabled: list[bool],
    draw_modes: list[int],
    draw_every: list[int],
    mode_values: list[int],
    update_tokens: list[int],
) -> None:
    payload = {
        "active": int(active),
        "enabled": [bool(x) for x in enabled],
        "draw_mode": [int(x) for x in draw_modes],
        "draw_every": [int(x) for x in draw_every],
        "mode": [int(x) for x in mode_values],
        "update_tokens": [int(x) for x in update_tokens],
    }
    control_path.write_text(json.dumps(payload))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run multiple simulations and switch active render with Up/Down arrows."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of simulations to run (overrides settings.json)",
    )
    parser.add_argument(
        "--script",
        type=str,
        default="import pygame copy 6.py",
        help="Path to the simulation script",
    )
    return parser.parse_args()


def _allocate_run_numbers(count: int) -> list[int]:
    results_dir = Path("results")
    results_dir.mkdir(parents=True, exist_ok=True)
    counter_path = results_dir / "numTries"
    try:
        current = int(counter_path.read_text().strip())
    except Exception:
        current = -1
    base = current + 1
    run_nums = list(range(base, base + count))
    counter_path.write_text(str(run_nums[-1]))
    return run_nums


def _allocate_master_run_number(results_dir: Path) -> int:
    counter_path = results_dir / "numTriesMaster.csv"
    try:
        current = int(counter_path.read_text().strip())
    except Exception:
        current = -1
    new_val = current + 1
    counter_path.write_text(str(new_val))
    return new_val


def _combine_master_logs(
    results_dir: Path,
    master_dir: Path,
    master_label: str,
    run_nums: list[int],
) -> None:
    master_raw = master_dir / "raw_data"
    master_raw.mkdir(parents=True, exist_ok=True)
    master_log = master_raw / f"simulation_log_{master_label}.csv"
    with open(master_log, "w", newline="") as out_handle:
        writer = csv.writer(out_handle)
        writer.writerow(_MASTER_HEADER)
        for run_num in run_nums:
            raw_dir = results_dir / str(run_num) / "raw_data"
            base = raw_dir / f"simulation_log_{run_num}.csv"
            part_files = sorted(raw_dir.glob(f"simulation_log_{run_num}_part*.csv"))
            for path in [base] + part_files:
                if not path.exists():
                    continue
                with open(path, newline="") as in_handle:
                    reader = csv.reader(in_handle)
                    try:
                        next(reader)
                    except StopIteration:
                        continue
                    for row in reader:
                        writer.writerow(row)


def _combine_master_means(
    results_dir: Path,
    master_dir: Path,
    master_label: str,
    run_nums: list[int],
) -> None:
    def _combine(kind: str, fieldnames: list[str]) -> None:
        output_path = master_dir / f"combined{kind}MeanSimulatino{master_label}_Log.csv"
        rows: list[dict[str, str]] = []
        for run_num in run_nums:
            input_path = (
                results_dir
                / str(run_num)
                / f"parsed{kind}MeanSimulatino{run_num}_Log.csv"
            )
            if not input_path.exists():
                continue
            with open(input_path, newline="") as in_handle:
                reader = csv.DictReader(in_handle)
                for row in reader:
                    if not row:
                        continue
                    rows.append({name: row.get(name, "") for name in fieldnames})

        def _evo_key(row: dict[str, str]) -> float:
            try:
                return float(row.get("evolution rate", ""))
            except Exception:
                return float("inf")

        rows.sort(key=_evo_key)

        with open(output_path, "w", newline="") as out_handle:
            writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    _combine(
        "Arithmetic",
        [
            "evolution rate",
            "arithmetic mean length lived",
            "arithmetic mean species population time",
        ],
    )
    _combine(
        "Geometric",
        [
            "evolution rate",
            "geometric mean length lived",
            "geometric mean species population time",
        ],
    )


def _load_fps_points(path: Path, max_points: int = 200) -> list[float]:
    if not path.exists():
        return []
    points = deque(maxlen=max(1, max_points))
    try:
        with open(path, newline="") as handle:
            reader = csv.reader(handle)
            for row in reader:
                if not row:
                    continue
                if row[0].startswith("timestamp"):
                    continue
                try:
                    val = float(row[1]) if len(row) > 1 else float(row[0])
                except (ValueError, IndexError):
                    continue
                points.append(val)
    except Exception:
        return []
    return list(points)


def _load_arithmetic_points(results_dir: Path, run_num: int) -> list[tuple[float, float]]:
    run_dir = results_dir / str(run_num)
    parsed_path = run_dir / f"parsedArithmeticMeanSimulatino{run_num}_Log.csv"
    if not parsed_path.exists():
        return []
    points = []
    try:
        with open(parsed_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    x = float(row["evolution rate"])
                    y = float(row["arithmetic mean length lived"])
                except (ValueError, KeyError, TypeError):
                    continue
                points.append((x, y))
    except Exception:
        return []
    return points


def _get_cached_points(cache: dict, path: Path, loader, max_age: float = 0.5):
    now = time.time()
    try:
        mtime = path.stat().st_mtime
    except Exception:
        mtime = None
    entry = cache.get(path)
    if entry and entry["mtime"] == mtime and (now - entry["last"]) < max_age:
        return entry["points"]
    points = loader(path)
    cache[path] = {"points": points, "mtime": mtime, "last": now}
    return points


def _load_run_meta(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _draw_fps_chart(
    surface,
    font,
    rect: pygame.Rect,
    points: list[float],
    color=(0, 200, 255),
    max_seconds: float = 2.0,
) -> None:
    pygame.draw.rect(surface, (80, 80, 80), rect, 1)
    if not points:
        msg = font.render("No FPS data", True, (160, 160, 160))
        surface.blit(msg, (rect.x + 6, rect.y + 6))
        return
    mean_val = sum(points) / len(points)
    mean_text = font.render(f"Mean: {mean_val:.2f}s", True, (180, 180, 180))
    surface.blit(mean_text, (rect.x + 6, rect.y + 4))
    max_points = rect.width
    recent = points[-max_points:]
    overlay = pygame.Surface((rect.width, rect.height), pygame.SRCALPHA)
    color = (color[0], color[1], color[2], _DOT_ALPHA)
    for i, tval in enumerate(recent):
        t_clamped = max(0.0, min(max_seconds, tval))
        px = i
        py = rect.height - int((t_clamped / max_seconds) * rect.height)
        pygame.draw.circle(overlay, color, (px, py), 2)
    surface.blit(overlay, rect.topleft)


def _draw_arithmetic_chart(
    surface,
    font,
    rect: pygame.Rect,
    points: list[tuple[float, float]],
    color=(0, 220, 255),
) -> None:
    pygame.draw.rect(surface, (80, 80, 80), rect, 1)
    if not points:
        msg = font.render("No mean data", True, (160, 160, 160))
        surface.blit(msg, (rect.x + 6, rect.y + 6))
        return

    points_sorted = sorted(points, key=lambda p: p[0])
    xs = [p[0] for p in points_sorted]
    ys = [p[1] for p in points_sorted]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    if max_x == min_x:
        max_x = min_x + 1.0
    if max_y == min_y:
        max_y = min_y + 1.0

    x_pad = (max_x - min_x) * 0.05
    y_pad = (max_y - min_y) * 0.05
    min_x -= x_pad
    max_x += x_pad
    min_y -= y_pad
    max_y += y_pad

    plot_left = rect.x + 6
    plot_top = rect.y + 6
    plot_width = rect.width - 12
    plot_height = rect.height - 12

    def _scale_point(x, y):
        px = int(((x - min_x) / (max_x - min_x)) * plot_width)
        py = plot_height - int(((y - min_y) / (max_y - min_y)) * plot_height)
        return px, py

    overlay = pygame.Surface((plot_width, plot_height), pygame.SRCALPHA)
    color = (color[0], color[1], color[2], _DOT_ALPHA)
    for x, y in points_sorted:
        px, py = _scale_point(x, y)
        pygame.draw.circle(overlay, color, (px, py), 2)
    surface.blit(overlay, (plot_left, plot_top))


def _draw_multi_fps_chart(
    surface, font, rect: pygame.Rect, series: list[list[float]], max_seconds: float = 2.0
) -> None:
    pygame.draw.rect(surface, (80, 80, 80), rect, 1)
    if not any(series):
        msg = font.render("No FPS data", True, (160, 160, 160))
        surface.blit(msg, (rect.x + 6, rect.y + 6))
        return
    colors = _SIM_COLORS
    max_points = rect.width
    overlay = pygame.Surface((rect.width, rect.height), pygame.SRCALPHA)
    for idx, points in enumerate(series):
        if not points:
            continue
        base_color = colors[idx % len(colors)]
        color = (base_color[0], base_color[1], base_color[2], _DOT_ALPHA)
        recent = points[-max_points:]
        for i, tval in enumerate(recent):
            t_clamped = max(0.0, min(max_seconds, tval))
            px = i
            py = rect.height - int((t_clamped / max_seconds) * rect.height)
            pygame.draw.circle(overlay, color, (px, py), 2)
    surface.blit(overlay, rect.topleft)


def _draw_multi_arithmetic_chart(
    surface, font, rect: pygame.Rect, series: list[list[tuple[float, float]]]
) -> None:
    pygame.draw.rect(surface, (80, 80, 80), rect, 1)
    all_points = [pt for points in series for pt in points]
    if not all_points:
        msg = font.render("No mean data", True, (160, 160, 160))
        surface.blit(msg, (rect.x + 6, rect.y + 6))
        return
    xs = [p[0] for p in all_points]
    ys = [p[1] for p in all_points]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    if max_x == min_x:
        max_x = min_x + 1.0
    if max_y == min_y:
        max_y = min_y + 1.0
    x_pad = (max_x - min_x) * 0.05
    y_pad = (max_y - min_y) * 0.05
    min_x -= x_pad
    max_x += x_pad
    min_y -= y_pad
    max_y += y_pad

    plot_left = rect.x + 6
    plot_top = rect.y + 6
    plot_width = rect.width - 12
    plot_height = rect.height - 12

    def _scale_point(x, y):
        px = int(((x - min_x) / (max_x - min_x)) * plot_width)
        py = plot_height - int(((y - min_y) / (max_y - min_y)) * plot_height)
        return px, py

    colors = _SIM_COLORS
    overlay = pygame.Surface((plot_width, plot_height), pygame.SRCALPHA)
    for idx, points in enumerate(series):
        if not points:
            continue
        base_color = colors[idx % len(colors)]
        color = (base_color[0], base_color[1], base_color[2], _DOT_ALPHA)
        points_sorted = sorted(points, key=lambda p: p[0])
        for x, y in points_sorted:
            px, py = _scale_point(x, y)
            pygame.draw.circle(overlay, color, (px, py), 2)
    surface.blit(overlay, (plot_left, plot_top))


def main() -> None:
    args = _parse_args()
    settings = load_settings()
    try:
        settings_count = int(settings.get("simulations", {}).get("count", 3))
    except Exception:
        settings_count = 3
    count = args.count if args.count is not None else settings_count
    count = max(1, count)
    sim_path = Path(args.script)
    if not sim_path.exists():
        raise FileNotFoundError(f"Simulation script not found: {sim_path}")

    interpreter = sys.executable

    control_path = Path(tempfile.gettempdir()) / f"sim_master_active_{os.getpid()}.txt"
    selected_row = 0
    enabled = [True for _ in range(count)]
    draw_modes = [0 for _ in range(count)]
    draw_every = [500 for _ in range(count)]
    mode_values = [2 for _ in range(count)]
    update_tokens = [0 for _ in range(count)]
    master_active_index = -2
    active_sim_index = master_active_index
    _write_control(
        control_path,
        active_sim_index,
        enabled,
        draw_modes,
        draw_every,
        mode_values,
        update_tokens,
    )

    results_dir = Path("results")
    run_nums = _allocate_run_numbers(count)
    master_run_num = _allocate_master_run_number(results_dir)
    master_label = f"master_{master_run_num}"
    master_dir = results_dir / master_label
    master_dir.mkdir(parents=True, exist_ok=True)
    fps_paths = [results_dir / str(run_num) / "fps_log.csv" for run_num in run_nums]
    env_base = os.environ.copy()
    procs = []
    sim_start_times = []
    for idx in range(count):
        env = env_base.copy()
        env["SIM_CONTROL_FILE"] = str(control_path)
        env["SIM_INDEX"] = str(idx)
        env["SIM_TOTAL"] = str(count)
        env["SIM_ALL_ACTIVE"] = "1"
        env["SIM_RUN_NUM"] = str(run_nums[idx])
        env["SIM_FPS_PATH"] = str(fps_paths[idx])
        env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            [interpreter, str(sim_path)],
            env=env,
            cwd=os.getcwd(),
        )
        procs.append(proc)
        sim_start_times.append(time.perf_counter())

    pygame.init()
    header_top          = 30
    global_chart_h      = 90
    global_chart_gap    = 40
    master_line_offset  = 140
    header_h            = header_top + master_line_offset + global_chart_gap + global_chart_h + 30
    chart_h             = 70
    panel_h             = chart_h + 32
    window_w            = 1000
    content_h           = header_h + panel_h * count
    try:
        max_window_h = int(settings.get("screen", {}).get("height", 900))
    except Exception:
        max_window_h = 900
    max_window_h = max(360, max_window_h)
    window_h = min(content_h, max_window_h)
    if window_h <= header_h:
        window_h = header_h + 1
    screen = pygame.display.set_mode((window_w, window_h))
    pygame.display.set_caption("Simulation Master")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 16)
    clock = pygame.time.Clock()

    fps_cache = {}
    arithmetic_cache = {}
    fps_series = [[] for _ in range(count)]
    mean_series = [[] for _ in range(count)]
    meta_series = [{} for _ in range(count)]
    last_chart_refresh = 0.0
    base_chart_refresh_s = 2.0
    base_master_fps = 1
    uncapped_fps = 240
    chart_refresh_s = base_chart_refresh_s
    master_fps = base_master_fps
    master_capped = True


    max_scroll = max(0, content_h - window_h)
    scroll_offset = 0
    scroll_step = max(30, panel_h // 2)

    running = True
    while running:
        button_w = 110
        button_h = 26
        button_gap = 8
        button_x = window_w - button_w - 20
        button_y = header_top
        draw_btn = pygame.Rect(button_x, button_y, button_w, button_h)
        mode_btn = pygame.Rect(button_x, button_y + (button_h + button_gap), button_w, button_h)
        info_btn = pygame.Rect(button_x, button_y + 2 * (button_h + button_gap), button_w, button_h)
        fps_btn = pygame.Rect(button_x, button_y + 3 * (button_h + button_gap), button_w, button_h)
        exit_btn = pygame.Rect(button_x, button_y + 4 * (button_h + button_gap), button_w, button_h)

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE or event.key == pygame.K_q:
                    running = False
                elif event.key == pygame.K_UP:
                    selected_row = (selected_row - 1) % (count + 1)
                    active_sim_index = master_active_index if selected_row == 0 else selected_row - 1
                    if max_scroll > 0:
                        if selected_row == 0:
                            scroll_offset = 0
                        else:
                            row_top = header_h + (selected_row - 1) * panel_h
                            if row_top < scroll_offset:
                                scroll_offset = row_top
                            scroll_offset = max(0, min(max_scroll, scroll_offset))
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_DOWN:
                    selected_row = (selected_row + 1) % (count + 1)
                    active_sim_index = master_active_index if selected_row == 0 else selected_row - 1
                    if max_scroll > 0:
                        if selected_row == 0:
                            scroll_offset = 0
                        else:
                            row_bottom = header_h + selected_row * panel_h
                            if row_bottom > scroll_offset + window_h:
                                scroll_offset = row_bottom - window_h
                            scroll_offset = max(0, min(max_scroll, scroll_offset))
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_PAGEUP:
                    if max_scroll > 0:
                        scroll_offset = max(0, scroll_offset - window_h)
                elif event.key == pygame.K_PAGEDOWN:
                    if max_scroll > 0:
                        scroll_offset = min(max_scroll, scroll_offset + window_h)
                elif event.key == pygame.K_HOME:
                    scroll_offset = 0
                elif event.key == pygame.K_END:
                    scroll_offset = max_scroll
                elif event.key == pygame.K_LEFT or event.key == pygame.K_RIGHT:
                    if selected_row == 0:
                        new_state = not all(enabled)
                        for i in range(count):
                            enabled[i] = new_state
                    else:
                        idx = selected_row - 1
                        enabled[idx] = not enabled[idx]
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_d:
                    _apply_draw_toggle(selected_row, draw_modes)
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_m:
                    _apply_mode_toggle(selected_row, mode_values)
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_s:
                    _apply_update(selected_row, update_tokens)
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_f:
                    master_capped = not master_capped
                    if master_capped:
                        chart_refresh_s = base_chart_refresh_s
                        master_fps = base_master_fps
                    else:
                        chart_refresh_s = 0.0
                        master_fps = uncapped_fps
            elif event.type == pygame.MOUSEWHEEL:
                if max_scroll > 0:
                    scroll_offset = max(
                        0, min(max_scroll, scroll_offset - event.y * scroll_step)
                    )
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                content_y = my + scroll_offset
                if draw_btn.collidepoint(mx, content_y):
                    _apply_draw_toggle(selected_row, draw_modes)
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif mode_btn.collidepoint(mx, content_y):
                    _apply_mode_toggle(selected_row, mode_values)
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif info_btn.collidepoint(mx, content_y):
                    _apply_update(selected_row, update_tokens)
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif fps_btn.collidepoint(mx, content_y):
                    master_capped = not master_capped
                    if master_capped:
                        chart_refresh_s = base_chart_refresh_s
                        master_fps = base_master_fps
                    else:
                        chart_refresh_s = 0.0
                        master_fps = uncapped_fps
                elif exit_btn.collidepoint(mx, content_y):
                    running = False
                elif content_y < header_h:
                    selected_row = 0
                    active_sim_index = master_active_index
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                else:
                    row_start = header_h
                    row_h = panel_h
                    if content_y < row_start:
                        continue
                    idx = (content_y - row_start) // row_h
                    if 0 <= idx < count:
                        selected_row = idx + 1
                        active_sim_index = idx
                        enabled[idx] = not enabled[idx]
                        _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button in (4, 5):
                if max_scroll > 0:
                    if event.button == 4:
                        scroll_offset = max(0, scroll_offset - scroll_step)
                    else:
                        scroll_offset = min(max_scroll, scroll_offset + scroll_step)

        screen.fill((20, 20, 20))
        y_offset = -scroll_offset
        title = font.render("Simulation Master", True, (240, 240, 240))
        if selected_row == 0:
            status_text = "Selected: MASTER (0)"
        else:
            status_text = f"Selected: Sim {selected_row} / {count}"
        status = font.render(status_text, True, (0, 200, 255))
        hint1 = small_font.render("Up/Down: select sim", True, (200, 200, 200))
        hint2 = small_font.render("Left/Right or click: on/off", True, (200, 200, 200))
        cap_label = "CAPPED" if master_capped else "UNCAPPED"
        hint3 = small_font.render("D: draw  M: mode  S: update  F: cap  Esc/Q: quit", True, (200, 200, 200))
        hint4 = small_font.render(f"Master FPS: {cap_label}", True, (200, 200, 200))

        screen.blit(title, (20, header_top + y_offset))
        screen.blit(status, (20, header_top + 30 + y_offset))
        screen.blit(hint1, (20, header_top + 60 + y_offset))
        screen.blit(hint2, (20, header_top + 80 + y_offset))
        screen.blit(hint3, (20, header_top + 100 + y_offset))
        screen.blit(hint4, (20, header_top + 120 + y_offset))

        for rect, label in [
            (draw_btn, "Draw"),
            (mode_btn, "Mode"),
            (info_btn, "Info (S)"),
            (fps_btn, "FPS Cap"),
            (exit_btn, "Exit"),
        ]:
            draw_rect = rect.move(0, y_offset)
            pygame.draw.rect(screen, (40, 40, 40), draw_rect)
            pygame.draw.rect(screen, (120, 120, 120), draw_rect, 1)
            text = small_font.render(label, True, (220, 220, 220))
            screen.blit(text, (draw_rect.x + 8, draw_rect.y + 5))

        margin = 20
        gap = 20
        chart_w = (window_w - margin * 2 - gap) // 2
        master_line_y = header_top + master_line_offset
        global_chart_y = master_line_y + global_chart_gap
        fps_all_rect = pygame.Rect(margin, global_chart_y + y_offset, chart_w, global_chart_h)
        mean_all_rect = pygame.Rect(margin + chart_w + gap, global_chart_y + y_offset, chart_w, global_chart_h)

        now = time.time()
        if now - last_chart_refresh >= chart_refresh_s:
            last_chart_refresh = now
            for idx in range(count):
                fps_points = _get_cached_points(
                    fps_cache,
                    fps_paths[idx],
                    lambda p: _load_fps_points(p, max_points=chart_w),
                )
                fps_series[idx] = fps_points

                mean_path = (
                    results_dir
                    / str(run_nums[idx])
                    / f"parsedArithmeticMeanSimulatino{run_nums[idx]}_Log.csv"
                )
                mean_points = _get_cached_points(
                    arithmetic_cache,
                    mean_path,
                    lambda p: _load_arithmetic_points(results_dir, run_nums[idx]),
                    max_age=1.0,
                )
                mean_series[idx] = mean_points

                meta_path = results_dir / str(run_nums[idx]) / "run_meta.json"
                meta_series[idx] = _get_cached_points(
                    arithmetic_cache,
                    meta_path,
                    _load_run_meta,
                    max_age=1.0,
                )
        enabled_all = all(enabled)
        enabled_any = any(enabled)
        if enabled_all:
            master_state = "ALL ON"
        elif not enabled_any:
            master_state = "ALL OFF"
        else:
            master_state = "MIXED"
        if len(set(draw_modes)) == 1:
            master_draw_mode = draw_modes[0]
            if master_draw_mode == 0:
                master_draw = "DRAW"
            elif master_draw_mode == 1:
                master_draw = f"DRAW/{draw_every[0]}"
            else:
                master_draw = "NO-DRAW"
        else:
            master_draw = "MIXED"
        if len(set(mode_values)) == 1:
            master_mode = f"MODE {mode_values[0]}"
        else:
            master_mode = "MODE MIXED"

        master_color = (0, 200, 255) if selected_row == 0 else (200, 200, 200)
        frame_vals = [
            m.get("frame_count")
            for m in meta_series
            if isinstance(m, dict) and isinstance(m.get("frame_count"), (int, float))
        ]
        species_vals = [
            m.get("amnt_of_species")
            for m in meta_series
            if isinstance(m, dict) and isinstance(m.get("amnt_of_species"), (int, float))
        ]
        now_perf = time.perf_counter()
        elapsed_vals = []
        for idx in range(count):
            meta = meta_series[idx] if idx < len(meta_series) else {}
            elapsed = None
            if isinstance(meta, dict):
                meta_elapsed = meta.get("elapsed_seconds")
                if isinstance(meta_elapsed, (int, float)):
                    elapsed = float(meta_elapsed)
            if idx < len(sim_start_times):
                fallback = max(0.0, now_perf - sim_start_times[idx])
                elapsed = fallback if elapsed is None else max(elapsed, fallback)
            if elapsed is not None:
                elapsed_vals.append(elapsed)
        
        mean_frames = (sum(frame_vals) / len(frame_vals)) if frame_vals else 0.0
        mean_species = (sum(species_vals) / len(species_vals)) if species_vals else 0.0
        mean_elapsed = (sum(elapsed_vals) / len(elapsed_vals)) if elapsed_vals else 0.0

        master_line = small_font.render(
            f"MASTER (0): {master_state} | {master_draw} | {master_mode}",
            True,
            master_color,
        )
        master_stats = small_font.render(
            f"Frames mean: {mean_frames:.0f} | Species mean: {mean_species:.1f} | Runtime: {_format_duration(mean_elapsed)}",
            True,
            master_color,
        )
        screen.blit(master_line, (margin, master_line_y + y_offset))
        screen.blit(master_stats, (margin, master_line_y + 16 + y_offset))
        _draw_multi_fps_chart(screen, small_font, fps_all_rect, fps_series)
        _draw_multi_arithmetic_chart(screen, small_font, mean_all_rect, mean_series)

        y = header_h + y_offset
        for idx in range(count):
            if y + panel_h < 0:
                y += panel_h
                continue
            if y > window_h:
                break
            color = (0, 200, 255) if selected_row == idx + 1 else (200, 200, 200)
            state = "ON" if enabled[idx] else "OFF"
            if draw_modes[idx] == 0:
                draw_state = "DRAW"
            elif draw_modes[idx] == 1:
                draw_state = f"DRAW/{draw_every[idx]}"
            else:
                draw_state = "NO-DRAW"
            mode_state = f"MODE {mode_values[idx]}"
            label = f"Sim {idx + 1}"
            meta = meta_series[idx] if idx < len(meta_series) and isinstance(meta_series[idx], dict) else {}
            species_count = meta.get("amnt_of_species")
            big_species_count = meta.get("amnt_of_big_species")
            species_text = (
                f"{int(species_count)}" if isinstance(species_count, (int, float)) else "--"
            )
            big_species_text = (
                f"{int(big_species_count)}" if isinstance(big_species_count, (int, float)) else "--"
            )
            line = small_font.render(
                f"{label}: {state} | {draw_state} | {mode_state} | Species: {species_text} | Big: {big_species_text}",
                True,
                color,
            )
            screen.blit(line, (margin, y))

            chart_y = y + 20
            fps_rect = pygame.Rect(margin, chart_y, chart_w, chart_h)
            mean_rect = pygame.Rect(margin + chart_w + gap, chart_y, chart_w, chart_h)

            sim_color = _sim_color(idx)
            fps_points = fps_series[idx] if idx < len(fps_series) else []
            _draw_fps_chart(screen, small_font, fps_rect, fps_points, sim_color)

            mean_points = mean_series[idx] if idx < len(mean_series) else []
            _draw_arithmetic_chart(screen, small_font, mean_rect, mean_points, sim_color)

            y += panel_h
        pygame.display.flip()
        clock.tick(master_fps)

        for proc in procs:
            if proc.poll() is not None:
                running = False
                break

    try:
        _write_control(control_path, -1, enabled, draw_modes, draw_every, mode_values, update_tokens)
    except Exception:
        pass

    for proc in procs:
        if proc.poll() is None:
            proc.terminate()

    deadline = time.time() + 3
    for proc in procs:
        while proc.poll() is None and time.time() < deadline:
            time.sleep(0.05)
        if proc.poll() is None:
            proc.kill()

    try:
        _combine_master_logs(results_dir, master_dir, master_label, run_nums)
        parse_run(results_dir, master_label, quiet=True)
    except Exception as e:
        print(f"Failed to build master logs: {e}")
    try:
        _combine_master_means(results_dir, master_dir, master_label, run_nums)
    except Exception as e:
        print(f"Failed to build combined mean files: {e}")

    try:
        control_path.unlink(missing_ok=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()
