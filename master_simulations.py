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


def _sim_color(idx: int):
    return _SIM_COLORS[idx % len(_SIM_COLORS)]


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
    color = (color[0], color[1], color[2], 128)
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
    color = (color[0], color[1], color[2], 128)
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
        color = (base_color[0], base_color[1], base_color[2], 128)
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
        color = (base_color[0], base_color[1], base_color[2], 128)
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

    run_nums = _allocate_run_numbers(count)
    results_dir = Path("results")
    fps_paths = [results_dir / str(run_num) / "fps_log.csv" for run_num in run_nums]
    env_base = os.environ.copy()
    procs = []
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

    pygame.init()
    header_top = 30
    global_chart_h = 90
    global_chart_gap = 20
    master_line_offset = 150
    header_h = header_top + master_line_offset + global_chart_gap + global_chart_h + 30
    chart_h = 70
    panel_h = chart_h + 32
    window_w = 640
    screen = pygame.display.set_mode((window_w, header_h + panel_h * count))
    pygame.display.set_caption("Simulation Master")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 16)
    clock = pygame.time.Clock()

    fps_cache = {}
    arithmetic_cache = {}
    fps_series = [[] for _ in range(count)]
    mean_series = [[] for _ in range(count)]
    last_chart_refresh = 0.0
    base_chart_refresh_s = 2.0
    base_master_fps = 1
    chart_refresh_s = base_chart_refresh_s
    master_fps = base_master_fps
    master_capped = True


    running = True
    while running:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE or event.key == pygame.K_q:
                    running = False
                elif event.key == pygame.K_UP:
                    selected_row = (selected_row - 1) % (count + 1)
                    active_sim_index = master_active_index if selected_row == 0 else selected_row - 1
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_DOWN:
                    selected_row = (selected_row + 1) % (count + 1)
                    active_sim_index = master_active_index if selected_row == 0 else selected_row - 1
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
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
                    if selected_row == 0:
                        new_mode = (draw_modes[0] + 1) % 3
                        for i in range(count):
                            draw_modes[i] = new_mode
                    else:
                        idx = selected_row - 1
                        draw_modes[idx] = (draw_modes[idx] + 1) % 3
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_m:
                    if selected_row == 0:
                        new_mode = (mode_values[0] + 1) % 3
                        for i in range(count):
                            mode_values[i] = new_mode
                    else:
                        idx = selected_row - 1
                        mode_values[idx] = (mode_values[idx] + 1) % 3
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_s:
                    if selected_row == 0:
                        for i in range(count):
                            update_tokens[i] += 1
                    else:
                        idx = selected_row - 1
                        update_tokens[idx] += 1
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_f:
                    master_capped = not master_capped
                    if master_capped:
                        chart_refresh_s = base_chart_refresh_s
                        master_fps = base_master_fps
                    else:
                        chart_refresh_s = 0.0
                        master_fps = 0
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if my < header_h:
                    selected_row = 0
                    active_sim_index = master_active_index
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                else:
                    row_start = header_h
                    row_h = panel_h
                    idx = (my - row_start) // row_h
                    if 0 <= idx < count:
                        selected_row = idx + 1
                        active_sim_index = idx
                        enabled[idx] = not enabled[idx]
                        _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)

        screen.fill((20, 20, 20))
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

        screen.blit(title, (20, header_top))
        screen.blit(status, (20, header_top + 30))
        screen.blit(hint1, (20, header_top + 60))
        screen.blit(hint2, (20, header_top + 80))
        screen.blit(hint3, (20, header_top + 100))
        screen.blit(hint4, (20, header_top + 120))

        margin = 20
        gap = 20
        chart_w = (window_w - margin * 2 - gap) // 2
        master_line_y = header_top + master_line_offset
        global_chart_y = master_line_y + global_chart_gap
        fps_all_rect = pygame.Rect(margin, global_chart_y, chart_w, global_chart_h)
        mean_all_rect = pygame.Rect(margin + chart_w + gap, global_chart_y, chart_w, global_chart_h)

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
        master_line = small_font.render(
            f"MASTER (0): {master_state} | {master_draw} | {master_mode}",
            True,
            master_color,
        )
        screen.blit(master_line, (margin, master_line_y))
        _draw_multi_fps_chart(screen, small_font, fps_all_rect, fps_series)
        _draw_multi_arithmetic_chart(screen, small_font, mean_all_rect, mean_series)

        y = header_h
        for idx in range(count):
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
            line = small_font.render(
                f"{label}: {state} | {draw_state} | {mode_state}", True, color
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
        control_path.unlink(missing_ok=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()
