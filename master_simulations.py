import argparse
import csv
from collections import deque
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import pygame

from settings_manager import load_settings, save_settings
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
_DOT_ALPHA = 110
_DOT_ALPHA_HI = 220
_DOT_ALPHA_DIM = 60
_DOT_RADIUS = 3
_FPS_MODE_CAPPED = 0
_FPS_MODE_UNCAPPED = 1
_FPS_MODE_FULL_THROTTLE = 2


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_bool(value) -> bool:
    return isinstance(value, bool)


def _numeric_step(value: float) -> float:
    if isinstance(value, int) and not isinstance(value, bool):
        return 1.0
    try:
        abs_val = abs(float(value))
    except Exception:
        return 1.0
    if abs_val == 0.0:
        return 0.01
    exp = math.floor(math.log10(abs_val))
    decimals = max(2, -exp)
    return 10 ** (-decimals)


def _step_decimals(step: float) -> int:
    try:
        step = abs(float(step))
    except Exception:
        return 2
    if step == 0:
        return 2
    exp = math.floor(math.log10(step))
    return max(0, -exp)


def _collect_setting_items(settings: dict) -> list[dict]:
    items = []

    def _walk(obj, path):
        if isinstance(obj, dict):
            for key, value in obj.items():
                _walk(value, path + [key])
            return
        label = ".".join(path) if path else ""
        items.append(
            {
                "path": path,
                "label": label,
                "value": obj,
                "is_number": _is_number(obj),
                "is_bool": _is_bool(obj),
            }
        )

    _walk(settings, [])
    return items


def _set_setting_value(settings: dict, path: list[str], value) -> None:
    if not path:
        return
    node = settings
    for key in path[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    node[path[-1]] = value


def _parse_numeric(text: str, original):
    try:
        if isinstance(original, int) and not isinstance(original, bool):
            if any(ch in text for ch in [".", "e", "E"]):
                return int(float(text))
            return int(text)
        if isinstance(original, float):
            return float(text)
    except Exception:
        return None
    return None


def _edit_settings_ui(settings: dict, master_dir=None, write_global_on_confirm: bool = True):
    items = _collect_setting_items(settings)
    if not items:
        return settings
    priority = {"num_tries": 0, "num_tries_master": 1}
    items.sort(key=lambda item: (priority.get(item["label"], 2), item["label"]))

    screen_w = 900
    screen_h = 700
    screen = pygame.display.set_mode((screen_w, screen_h))
    pygame.display.set_caption("Master Settings")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 18)
    clock = pygame.time.Clock()
    pygame.key.set_repeat(250, 40)

    selected = 0
    scroll = 0
    editing = False
    edit_text = ""
    error_text = ""

    list_top = 70
    list_bottom = screen_h - 90
    line_h = small_font.get_height() + 6
    visible_count = max(1, (list_bottom - list_top) // line_h)

    confirm_rect = pygame.Rect(screen_w - 160, screen_h - 60, 140, 36)
    upload_rect = pygame.Rect(screen_w - 320, screen_h - 60, 140, 36)
    upload_notice = ""
    upload_notice_time = 0.0

    def _ensure_visible():
        nonlocal scroll
        if selected < scroll:
            scroll = selected
        elif selected >= scroll + visible_count:
            scroll = selected - visible_count + 1
        scroll = max(0, min(scroll, max(0, len(items) - visible_count)))

    _ensure_visible()

    while True:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return None
            if event.type == pygame.KEYDOWN:
                if editing:
                    if event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        original = items[selected]["value"]
                        new_val = _parse_numeric(edit_text.strip(), original)
                        if new_val is None:
                            error_text = "Invalid number"
                        else:
                            _set_setting_value(settings, items[selected]["path"], new_val)
                            items[selected]["value"] = new_val
                            editing = False
                            edit_text = ""
                            error_text = ""
                    elif event.key == pygame.K_ESCAPE:
                        editing = False
                        edit_text = ""
                        error_text = ""
                    elif event.key == pygame.K_BACKSPACE:
                        edit_text = edit_text[:-1]
                    else:
                        ch = event.unicode
                        if ch and (ch.isdigit() or ch in ".-+eE"):
                            edit_text += ch
                else:
                    if event.key == pygame.K_ESCAPE:
                        return None
                    if event.key == pygame.K_UP:
                        selected = (selected - 1) % len(items)
                        _ensure_visible()
                    elif event.key == pygame.K_DOWN:
                        selected = (selected + 1) % len(items)
                        _ensure_visible()
                    elif event.key == pygame.K_LEFT or event.key == pygame.K_RIGHT:
                        item = items[selected]
                        if item["is_bool"]:
                            new_val = not bool(item["value"])
                            _set_setting_value(settings, item["path"], new_val)
                            item["value"] = new_val
                        elif item["is_number"]:
                            step = _numeric_step(item["value"])
                            delta = step if event.key == pygame.K_RIGHT else -step
                            new_val = float(item["value"]) + delta
                            if isinstance(item["value"], int) and not isinstance(
                                item["value"], bool
                            ):
                                new_val = int(round(new_val))
                            else:
                                decimals = _step_decimals(step)
                                new_val = round(new_val, decimals)
                            _set_setting_value(settings, item["path"], new_val)
                            item["value"] = new_val
                    elif event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        if items[selected]["is_number"]:
                            editing = True
                            edit_text = str(items[selected]["value"])
                            error_text = ""
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if confirm_rect.collidepoint(mx, my):
                    if master_dir is not None:
                        try:
                            master_dir.mkdir(parents=True, exist_ok=True)
                            local_settings = _strip_local_settings(settings)
                            _master_settings_path(master_dir).write_text(
                                json.dumps(local_settings, indent=2)
                            )
                            meta = _load_master_meta(master_dir)
                            if isinstance(meta, dict):
                                meta["settings"] = local_settings
                                _master_meta_path(master_dir).write_text(
                                    json.dumps(meta, indent=2)
                                )
                        except Exception:
                            pass
                    elif write_global_on_confirm:
                        save_settings(settings)
                    return settings
                if upload_rect.collidepoint(mx, my):
                    try:
                        global_settings = load_settings()
                    except Exception:
                        global_settings = {}
                    try:
                        global_settings["num_tries"] = int(settings.get("num_tries", 0))
                    except Exception:
                        pass
                    try:
                        global_settings["num_tries_master"] = int(
                            settings.get("num_tries_master", 0)
                        )
                    except Exception:
                        pass
                    #save_settings(global_settings)
                    upload_notice = "Uploaded counters to global settings"
                    upload_notice_time = time.time()
                if list_top <= my <= list_bottom:
                    idx = (my - list_top) // line_h + scroll
                    if 0 <= idx < len(items):
                        selected = int(idx)
                        _ensure_visible()

        screen.fill((18, 18, 18))
        title = font.render("Master Settings (edit numbers, then confirm)", True, (230, 230, 230))
        screen.blit(title, (20, 20))

        for idx in range(scroll, min(len(items), scroll + visible_count)):
            item = items[idx]
            y = list_top + (idx - scroll) * line_h
            label = item["label"]
            value = item["value"]
            value_text = f"{value}"
            line = f"{label}: {value_text}"
            color = (230, 230, 230) if (item["is_number"] or item["is_bool"]) else (160, 160, 160)
            if idx == selected:
                pygame.draw.rect(screen, (35, 35, 35), (16, y - 2, screen_w - 32, line_h))
                color = (0, 200, 255) if (item["is_number"] or item["is_bool"]) else (180, 180, 180)
            text = small_font.render(line, True, color)
            screen.blit(text, (22, y))

        hint = small_font.render(
            "Up/Down: select  Left/Right: adjust  Enter: edit  Esc: cancel",
            True,
            (180, 180, 180),
        )
        screen.blit(hint, (20, screen_h - 80))

        if editing:
            edit_line = f"Edit {items[selected]['label']}: {edit_text}"
            edit_color = (255, 220, 160) if not error_text else (255, 160, 160)
            edit_text_surf = small_font.render(edit_line, True, edit_color)
            screen.blit(edit_text_surf, (20, screen_h - 55))
        if error_text:
            err_surf = small_font.render(error_text, True, (255, 160, 160))
            screen.blit(err_surf, (20, screen_h - 35))

        pygame.draw.rect(screen, (60, 60, 60), confirm_rect)
        pygame.draw.rect(screen, (160, 160, 160), confirm_rect, 1)
        confirm_text = small_font.render("Confirm", True, (230, 230, 230))
        screen.blit(
            confirm_text,
            (
                confirm_rect.x + (confirm_rect.width - confirm_text.get_width()) // 2,
                confirm_rect.y + 8,
            ),
        )
        pygame.draw.rect(screen, (60, 60, 60), upload_rect)
        pygame.draw.rect(screen, (160, 160, 160), upload_rect, 1)
        upload_text = small_font.render("Upload", True, (230, 230, 230))
        screen.blit(
            upload_text,
            (
                upload_rect.x + (upload_rect.width - upload_text.get_width()) // 2,
                upload_rect.y + 8,
            ),
        )
        if upload_notice and (time.time() - upload_notice_time) < 2.0:
            notice = small_font.render(upload_notice, True, (180, 220, 180))
            screen.blit(notice, (20, screen_h - 30))

        pygame.display.flip()
        clock.tick(30)


def _master_meta_path(master_dir: Path) -> Path:
    return master_dir / "master_meta.json"


def _master_settings_path(master_dir: Path) -> Path:
    return master_dir / "settings.json"


def _strip_local_settings(settings: dict) -> dict:
    cleaned = dict(settings) if isinstance(settings, dict) else {}
    cleaned.pop("num_tries", None)
    cleaned.pop("num_tries_master", None)
    return cleaned


def _load_master_meta(master_dir: Path) -> dict:
    path = _master_meta_path(master_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _save_master_meta(
    master_dir: Path, run_nums: list[int], settings: dict, update_global: bool = False
) -> None:
    local_settings = _strip_local_settings(settings)
    payload = {
        "run_nums": [int(n) for n in run_nums],
        "settings": local_settings,
        "updated_at": time.time(),
    }
    try:
        master_dir.mkdir(parents=True, exist_ok=True)
        _master_meta_path(master_dir).write_text(json.dumps(payload, indent=2))
        _master_settings_path(master_dir).write_text(json.dumps(local_settings, indent=2))
        if update_global:
            save_settings(settings)
    except Exception:
        pass


def _select_master_run_ui(results_dir: Path):
    masters = []
    for path in sorted(results_dir.glob("master_*")):
        try:
            run_num = int(path.name.split("_", 1)[1])
        except Exception:
            continue
        masters.append((run_num, path))
    if not masters:
        return None, None
    masters.sort(key=lambda item: item[0])

    screen_w = 800
    screen_h = 520
    screen = pygame.display.set_mode((screen_w, screen_h))
    pygame.display.set_caption("Select Master Run")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 18)
    clock = pygame.time.Clock()

    selected = 0
    scroll = 0
    list_top = 70
    list_bottom = screen_h - 80
    list_left = 20
    list_width = 320
    line_h = small_font.get_height() + 6
    visible_count = max(1, (list_bottom - list_top) // line_h)

    def _ensure_visible():
        nonlocal scroll
        if selected < scroll:
            scroll = selected
        elif selected >= scroll + visible_count:
            scroll = selected - visible_count + 1
        scroll = max(0, min(scroll, max(0, len(masters) - visible_count)))

    _ensure_visible()

    while True:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return None, None
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE:
                    return None, None
                if event.key == pygame.K_UP:
                    selected = (selected - 1) % len(masters)
                    _ensure_visible()
                elif event.key == pygame.K_DOWN:
                    selected = (selected + 1) % len(masters)
                    _ensure_visible()
                elif event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                    run_num, path = masters[selected]
                    settings_snapshot = None
                    settings_path = _master_settings_path(path)
                    if settings_path.exists():
                        try:
                            settings_snapshot = json.loads(settings_path.read_text())
                        except Exception:
                            settings_snapshot = None
                    if settings_snapshot is None:
                        meta = _load_master_meta(path)
                        settings_snapshot = (
                            meta.get("settings") if isinstance(meta, dict) else None
                        )
                    return run_num, settings_snapshot
            if event.type == pygame.MOUSEWHEEL:
                if event.y > 0:
                    selected = (selected - 1) % len(masters)
                elif event.y < 0:
                    selected = (selected + 1) % len(masters)
                _ensure_visible()
            if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if list_top <= my <= list_bottom:
                    idx = (my - list_top) // line_h + scroll
                    if 0 <= idx < len(masters):
                        selected = int(idx)
                        _ensure_visible()

        screen.fill((18, 18, 18))
        title = font.render("Select Master Run", True, (230, 230, 230))
        screen.blit(title, (20, 20))
        hint = small_font.render("Enter: select  Esc: cancel", True, (180, 180, 180))
        screen.blit(hint, (20, screen_h - 50))

        for idx in range(scroll, min(len(masters), scroll + visible_count)):
            run_num, _ = masters[idx]
            y = list_top + (idx - scroll) * line_h
            label = f"master_{run_num}"
            color = (0, 200, 255) if idx == selected else (220, 220, 220)
            if idx == selected:
                pygame.draw.rect(
                    screen, (35, 35, 35), (list_left - 4, y - 2, list_width, line_h)
                )
            text = small_font.render(label, True, color)
            screen.blit(text, (list_left, y))

        # Detail panel for selected master
        sel_run, sel_path = masters[selected]
        detail_x = list_left + list_width + 20
        detail_y = list_top
        detail_w = screen_w - detail_x - 20
        detail_h = list_bottom - list_top
        pygame.draw.rect(screen, (30, 30, 30), (detail_x, detail_y, detail_w, detail_h))
        pygame.draw.rect(screen, (80, 80, 80), (detail_x, detail_y, detail_w, detail_h), 1)

        master_label = f"master_{sel_run}"
        meta = _load_master_meta(sel_path)
        run_nums = []
        if isinstance(meta, dict):
            raw_runs = meta.get("run_nums", [])
            if isinstance(raw_runs, list):
                for val in raw_runs:
                    try:
                        run_nums.append(int(val))
                    except Exception:
                        continue
        run_nums.sort()

        elapsed_vals = []
        species_vals = []
        for run_num in run_nums:
            meta_path = results_dir / str(run_num) / "run_meta.json"
            run_meta = _load_run_meta(meta_path)
            if isinstance(run_meta, dict):
                elapsed = run_meta.get("elapsed_seconds")
                species = run_meta.get("amnt_of_species")
                if isinstance(elapsed, (int, float)):
                    elapsed_vals.append(float(elapsed))
                if isinstance(species, (int, float)):
                    species_vals.append(float(species))

        mean_elapsed = (sum(elapsed_vals) / len(elapsed_vals)) if elapsed_vals else 0.0
        mean_species = (sum(species_vals) / len(species_vals)) if species_vals else 0.0

        title_label = master_label
        if len(run_nums) == 0:
            title_label = f"{master_label} (new)"
        detail_lines = [
            title_label,
            f"Runs: {len(run_nums)}",
            f"Runtime mean: {_format_duration(mean_elapsed)}",
            f"Species mean: {mean_species:.1f}",
        ]
        text_y = detail_y + 10
        for line in detail_lines:
            text = small_font.render(line, True, (220, 220, 220))
            screen.blit(text, (detail_x + 10, text_y))
            text_y += line_h

        # Arithmetic mean preview
        preview_rect = pygame.Rect(detail_x + 10, text_y + 10, detail_w - 20, 180)
        pygame.draw.rect(screen, (60, 60, 60), preview_rect, 1)
        mean_path = sel_path / f"combinedArithmeticMeanSimulatino{master_label}_Log.csv"
        if not mean_path.exists():
            mean_path = sel_path / f"parsedArithmeticMeanSimulatino{master_label}_Log.csv"
        mean_points = _load_mean_points(mean_path, "arithmetic mean length lived")
        if not mean_points:
            msg = small_font.render("No arithmetic data", True, (160, 160, 160))
            screen.blit(msg, (preview_rect.x + 6, preview_rect.y + 6))
        else:
            points_sorted = sorted(mean_points, key=lambda p: p[0])
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
            for x, y in points_sorted:
                px = preview_rect.x + int(((x - min_x) / (max_x - min_x)) * preview_rect.width)
                py = preview_rect.y + preview_rect.height - int(
                    ((y - min_y) / (max_y - min_y)) * preview_rect.height
                )
                pygame.draw.circle(screen, (0, 200, 255), (px, py), 2)

        pygame.display.flip()
        clock.tick(30)


def _edit_startup_ui(settings: dict, results_dir: Path):
    screen_w = 700
    screen_h = 420
    screen = pygame.display.set_mode((screen_w, screen_h))
    pygame.display.set_caption("Startup Options")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 18)
    clock = pygame.time.Clock()
    pygame.key.set_repeat(250, 40)

    draw_value = bool(settings.get("draw", True))
    try:
        num_tries = int(settings.get("num_tries", 0))
    except Exception:
        num_tries = 0
    try:
        num_master = int(settings.get("num_tries_master", 0))
    except Exception:
        num_master = 0

    selected = 0
    editing = False
    edit_text = ""
    error_text = ""
    continue_master_run = None
    continue_settings = None

    confirm_rect = pygame.Rect(screen_w - 160, screen_h - 60, 140, 36)
    select_rect = pygame.Rect(screen_w - 160, 220, 140, 32)
    upload_rect = pygame.Rect(screen_w - 320, screen_h - 60, 140, 36)
    upload_notice = ""
    upload_notice_time = 0.0

    def _apply_edit():
        nonlocal num_tries, num_master, editing, edit_text, error_text
        target = "num_tries" if selected == 1 else "num_master"
        original = num_tries if target == "num_tries" else num_master
        new_val = _parse_numeric(edit_text.strip(), original)
        if new_val is None:
            error_text = "Invalid number"
            return
        if target == "num_tries":
            num_tries = max(0, int(new_val))
        else:
            num_master = max(0, int(new_val))
        editing = False
        edit_text = ""
        error_text = ""

    while True:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return None
            if event.type == pygame.KEYDOWN:
                if editing:
                    if event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        _apply_edit()
                    elif event.key == pygame.K_ESCAPE:
                        editing = False
                        edit_text = ""
                        error_text = ""
                    elif event.key == pygame.K_BACKSPACE:
                        edit_text = edit_text[:-1]
                    else:
                        ch = event.unicode
                        if ch and (ch.isdigit() or ch in ".-+eE"):
                            edit_text += ch
                else:
                    if event.key == pygame.K_ESCAPE:
                        return None
                    if event.key == pygame.K_UP:
                        selected = (selected - 1) % 3
                    elif event.key == pygame.K_DOWN:
                        selected = (selected + 1) % 3
                    elif event.key in (pygame.K_LEFT, pygame.K_RIGHT):
                        if selected == 0:
                            draw_value = not draw_value
                        elif selected == 1:
                            step = _numeric_step(num_tries)
                            delta = step if event.key == pygame.K_RIGHT else -step
                            num_tries = max(0, int(round(num_tries + delta)))
                        elif selected == 2:
                            step = _numeric_step(num_master)
                            delta = step if event.key == pygame.K_RIGHT else -step
                            num_master = max(0, int(round(num_master + delta)))
                            continue_master_run = None
                            continue_settings = None
                    elif event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        if selected in (1, 2):
                            editing = True
                            edit_text = str(num_tries if selected == 1 else num_master)
                            error_text = ""
            if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if confirm_rect.collidepoint(mx, my):
                    settings["draw"] = draw_value
                    settings["num_tries"] = num_tries
                    settings["num_tries_master"] = num_master
                    save_settings(settings)
                    return settings, continue_master_run, continue_settings
                if upload_rect.collidepoint(mx, my):
                    try:
                        global_settings = load_settings()
                    except Exception:
                        global_settings = {}
                    global_settings["num_tries"] = num_tries
                    global_settings["num_tries_master"] = num_master
                    save_settings(global_settings)
                    upload_notice = "Uploaded counters to global settings"
                    upload_notice_time = time.time()
                if select_rect.collidepoint(mx, my):
                    picked_run, picked_settings = _select_master_run_ui(results_dir)
                    if picked_run is not None:
                        continue_master_run = picked_run
                        continue_settings = picked_settings
                if 120 <= my <= 145:
                    selected = 0
                elif 170 <= my <= 195:
                    selected = 1
                elif 220 <= my <= 245:
                    selected = 2

        screen.fill((18, 18, 18))
        title = font.render("Startup Options", True, (230, 230, 230))
        screen.blit(title, (20, 20))

        draw_label = f"Draw: {'ON' if draw_value else 'OFF'}"
        try:
            sim_count = int(settings.get("simulations", {}).get("count", 3))
        except Exception:
            sim_count = 3
        sim_count = max(1, sim_count)
        range_start = num_tries
        range_end = num_tries + sim_count - 1
        num_label = f"num_tries (next {range_start}-{range_end}): {num_tries}"
        master_label = f"num_tries_master: {num_master}"
        if continue_master_run is None:
            master_label = f"{master_label} (new)"

        for idx, line in enumerate([draw_label, num_label, master_label]):
            y = 120 + idx * 50
            color = (0, 200, 255) if idx == selected else (220, 220, 220)
            if idx == selected:
                pygame.draw.rect(screen, (35, 35, 35), (16, y - 4, screen_w - 32, 32))
            text = small_font.render(line, True, color)
            screen.blit(text, (22, y))

        pygame.draw.rect(screen, (60, 60, 60), select_rect)
        pygame.draw.rect(screen, (160, 160, 160), select_rect, 1)
        select_text = small_font.render("Select", True, (230, 230, 230))
        screen.blit(
            select_text,
            (
                select_rect.x + (select_rect.width - select_text.get_width()) // 2,
                select_rect.y + 6,
            ),
        )

        if continue_master_run is not None:
            cont_text = small_font.render(
                f"Continuing master_{continue_master_run}", True, (180, 220, 180)
            )
            screen.blit(cont_text, (22, 280))
        else:
            cont_text = small_font.render(
                "Starting a new master run", True, (180, 180, 220)
            )
            screen.blit(cont_text, (22, 280))

        hint = small_font.render(
            "Up/Down select  Left/Right adjust  Enter edit  Esc cancel",
            True,
            (180, 180, 180),
        )
        screen.blit(hint, (20, screen_h - 80))

        if editing:
            edit_line = f"Edit: {edit_text}"
            edit_color = (255, 220, 160) if not error_text else (255, 160, 160)
            edit_text_surf = small_font.render(edit_line, True, edit_color)
            screen.blit(edit_text_surf, (20, screen_h - 55))
        if error_text:
            err_surf = small_font.render(error_text, True, (255, 160, 160))
            screen.blit(err_surf, (20, screen_h - 35))

        pygame.draw.rect(screen, (60, 60, 60), confirm_rect)
        pygame.draw.rect(screen, (160, 160, 160), confirm_rect, 1)
        confirm_text = small_font.render("Continue", True, (230, 230, 230))
        screen.blit(
            confirm_text,
            (
                confirm_rect.x + (confirm_rect.width - confirm_text.get_width()) // 2,
                confirm_rect.y + 8,
            ),
        )
        pygame.draw.rect(screen, (60, 60, 60), upload_rect)
        pygame.draw.rect(screen, (160, 160, 160), upload_rect, 1)
        upload_text = small_font.render("Upload", True, (230, 230, 230))
        screen.blit(
            upload_text,
            (
                upload_rect.x + (upload_rect.width - upload_text.get_width()) // 2,
                upload_rect.y + 8,
            ),
        )
        if upload_notice and (time.time() - upload_notice_time) < 2.0:
            notice = small_font.render(upload_notice, True, (180, 220, 180))
            screen.blit(notice, (20, screen_h - 30))

        pygame.display.flip()
        clock.tick(30)

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


def _format_fps_label(sim_index: int, timestamp, interval, meta: dict, now_time: float) -> str:
    interval_label = "--"
    if isinstance(interval, (int, float)) and interval > 0:
        interval_label = f"{float(interval):.2f}s"
    since_start = "--:--:--"
    since_now = "--:--:--"
    if isinstance(timestamp, (int, float)):
        start_time = None
        if isinstance(meta, dict):
            st = meta.get("start_time")
            if isinstance(st, (int, float)):
                start_time = float(st)
            if start_time is None:
                elapsed = meta.get("elapsed_seconds")
                if isinstance(elapsed, (int, float)):
                    start_time = now_time - float(elapsed)
        if start_time is not None:
            since_start = _format_duration(max(0.0, float(timestamp) - start_time))
        since_now = _format_duration(max(0.0, now_time - float(timestamp)))
    return (
        f"Sim {sim_index + 1} | since start {since_start} | "
        f"since now {since_now} | 1000 iters {interval_label}"
    )


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
        default="simulation_entry.py",
        help="Path to the simulation script",
    )
    return parser.parse_args()


def _allocate_run_numbers(count: int) -> list[int]:
    results_dir = Path("results")
    results_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings()
    try:
        current = int(settings.get("num_tries", 0))
    except Exception:
        current = 0
    base = current
    run_nums = list(range(base, base + count))
    settings["num_tries"] = base + count
    save_settings(settings)
    return run_nums


def _allocate_master_run_number(results_dir: Path) -> int:
    settings = load_settings()
    try:
        current = int(settings.get("num_tries_master", 0))
    except Exception:
        current = 0
    new_val = current
    settings["num_tries_master"] = new_val + 1
    save_settings(settings)
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


def _load_fps_points(path: Path, max_points: int = 200) -> list[tuple]:
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
                    if len(row) > 1:
                        ts = float(row[0])
                        val = float(row[1])
                    else:
                        ts = None
                        val = float(row[0])
                except (ValueError, IndexError):
                    continue
                points.append((ts, val))
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


def _find_fps_point_index(points: list[tuple], selected_point):
    if not selected_point:
        return None
    ts = selected_point.get("timestamp")
    if ts is not None:
        for i, point in enumerate(points):
            if len(point) >= 1 and point[0] == ts:
                return i
    idx = selected_point.get("point_index")
    if isinstance(idx, int) and 0 <= idx < len(points):
        return idx
    return None


def _draw_fps_chart(
    surface,
    font,
    rect: pygame.Rect,
    points: list[tuple],
    color=(0, 200, 255),
    max_seconds: float = 2.0,
    selected_point=None,
    selected_idx=None,
    selected_label=None,
    label_font=None,
) -> None:
    pygame.draw.rect(surface, (80, 80, 80), rect, 1)
    if not points:
        msg = font.render("No FPS data", True, (160, 160, 160))
        surface.blit(msg, (rect.x + 6, rect.y + 6))
        return
    mean_val = sum(val for _, val in points) / len(points)
    mean_text = font.render(f"Mean: {mean_val:.2f}s", True, (180, 180, 180))
    surface.blit(mean_text, (rect.x + 6, rect.y + 4))
    max_points = rect.width
    recent = points[-max_points:]
    overlay = pygame.Surface((rect.width, rect.height), pygame.SRCALPHA)
    color = (color[0], color[1], color[2], _DOT_ALPHA)
    for i, (_, tval) in enumerate(recent):
        t_clamped = max(0.0, min(max_seconds, tval))
        px = i
        py = rect.height - int((t_clamped / max_seconds) * rect.height)
        pygame.draw.circle(overlay, color, (px, py), _DOT_RADIUS)
    surface.blit(overlay, rect.topleft)
    if selected_point and selected_idx is not None and selected_point.get("sim_index") == selected_idx:
        sel_i = _find_fps_point_index(recent, selected_point)
        if sel_i is not None:
            _, sel_val = recent[sel_i]
            t_clamped = max(0.0, min(max_seconds, sel_val))
            px = rect.x + sel_i
            py = rect.y + rect.height - int((t_clamped / max_seconds) * rect.height)
            pygame.draw.circle(surface, (255, 255, 255), (px, py), 4, 1)
            if selected_label:
                use_font = label_font if label_font is not None else font
                _draw_value_label(surface, use_font, rect, selected_label, (px + 6, py - 6))


def _draw_arithmetic_chart(
    surface,
    font,
    rect: pygame.Rect,
    points: list[tuple[float, float]],
    color=(0, 220, 255),
    selected_point=None,
    selected_idx=None,
    label_font=None,
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
    raw_min_y = min_y
    raw_max_y = max_y
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
        pygame.draw.circle(overlay, color, (px, py), _DOT_RADIUS)
    surface.blit(overlay, (plot_left, plot_top))

    max_label = font.render(f"max {raw_max_y:.2f}", True, (160, 160, 160))
    min_label = font.render(f"min {raw_min_y:.2f}", True, (160, 160, 160))
    surface.blit(max_label, (plot_left, rect.y + 2))
    surface.blit(min_label, (plot_left, rect.y + rect.height - min_label.get_height() - 2))

    if selected_point and selected_point.get("scope") == "sim":
        if selected_idx is None or selected_point.get("sim_index") == selected_idx:
            x = selected_point.get("x")
            y = selected_point.get("y")
            if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                px, py = _scale_point(x, y)
                px += plot_left
                py += plot_top
                pygame.draw.circle(surface, (255, 255, 255), (px, py), 4, 1)
                use_font = label_font if label_font is not None else font
                _draw_value_label(
                    surface,
                    use_font,
                    rect,
                    f"x:{x:.3f} y:{y:.2f}",
                    (px + 6, py - 6),
                )


def _draw_multi_fps_chart(
    surface,
    font,
    rect: pygame.Rect,
    series: list[list[tuple]],
    max_seconds: float = 2.0,
    selected_point=None,
    highlight_sim=None,
    meta_series=None,
    now_time=None,
    label_font=None,
) -> None:
    pygame.draw.rect(surface, (80, 80, 80), rect, 1)
    if not any(series):
        msg = font.render("No FPS data", True, (160, 160, 160))
        surface.blit(msg, (rect.x + 6, rect.y + 6))
        return
    colors = _SIM_COLORS
    max_points = rect.width
    overlay = pygame.Surface((rect.width, rect.height), pygame.SRCALPHA)
    order = list(range(len(series)))
    if highlight_sim is not None and 0 <= highlight_sim < len(series):
        order = [idx for idx in order if idx != highlight_sim] + [highlight_sim]
    for idx in order:
        points = series[idx]
        if not points:
            continue
        base_color = colors[idx % len(colors)]
        if highlight_sim is not None:
            alpha = _DOT_ALPHA_HI if idx == highlight_sim else _DOT_ALPHA_DIM
        else:
            alpha = _DOT_ALPHA
        color = (base_color[0], base_color[1], base_color[2], alpha)
        recent = points[-max_points:]
        for i, (_, tval) in enumerate(recent):
            t_clamped = max(0.0, min(max_seconds, tval))
            px = i
            py = rect.height - int((t_clamped / max_seconds) * rect.height)
            pygame.draw.circle(overlay, color, (px, py), _DOT_RADIUS)
    surface.blit(overlay, rect.topleft)
    if selected_point and isinstance(selected_point.get("sim_index"), int):
        sim_idx = int(selected_point.get("sim_index"))
        if 0 <= sim_idx < len(series):
            recent = series[sim_idx][-max_points:]
            sel_i = _find_fps_point_index(recent, selected_point)
            if sel_i is not None:
                _, sel_val = recent[sel_i]
                t_clamped = max(0.0, min(max_seconds, sel_val))
                px = rect.x + sel_i
                py = rect.y + rect.height - int((t_clamped / max_seconds) * rect.height)
                pygame.draw.circle(surface, (255, 255, 255), (px, py), 4, 1)
                if meta_series is None:
                    meta = {}
                else:
                    meta = meta_series[sim_idx] if sim_idx < len(meta_series) else {}
                label_now = now_time if now_time is not None else time.time()
                label = _format_fps_label(
                    sim_idx,
                    selected_point.get("timestamp"),
                    selected_point.get("interval"),
                    meta,
                    label_now,
                )
                use_font = label_font if label_font is not None else font
                _draw_value_label(surface, use_font, rect, label, (px + 6, py - 6))


def _draw_multi_arithmetic_chart(
    surface,
    font,
    rect: pygame.Rect,
    series: list[list[tuple[float, float]]],
    selected_point=None,
    highlight_sim=None,
    label_font=None,
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
    raw_min_y = min_y
    raw_max_y = max_y
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
    order = list(range(len(series)))
    if highlight_sim is not None and 0 <= highlight_sim < len(series):
        order = [idx for idx in order if idx != highlight_sim] + [highlight_sim]
    for idx in order:
        points = series[idx]
        if not points:
            continue
        base_color = colors[idx % len(colors)]
        if highlight_sim is not None:
            alpha = _DOT_ALPHA_HI if idx == highlight_sim else _DOT_ALPHA_DIM
        else:
            alpha = _DOT_ALPHA
        color = (base_color[0], base_color[1], base_color[2], alpha)
        points_sorted = sorted(points, key=lambda p: p[0])
        for x, y in points_sorted:
            px, py = _scale_point(x, y)
            pygame.draw.circle(overlay, color, (px, py), _DOT_RADIUS)
    surface.blit(overlay, (plot_left, plot_top))

    max_label = font.render(f"max {raw_max_y:.2f}", True, (160, 160, 160))
    min_label = font.render(f"min {raw_min_y:.2f}", True, (160, 160, 160))
    surface.blit(max_label, (plot_left, rect.y + 2))
    surface.blit(min_label, (plot_left, rect.y + rect.height - min_label.get_height() - 2))

    if selected_point and selected_point.get("scope") == "global":
        x = selected_point.get("x")
        y = selected_point.get("y")
        if isinstance(x, (int, float)) and isinstance(y, (int, float)):
            px, py = _scale_point(x, y)
            px += plot_left
            py += plot_top
            pygame.draw.circle(surface, (255, 255, 255), (px, py), 4, 1)
            label = f"x:{x:.3f} y:{y:.2f}"
            sim_idx = selected_point.get("sim_index")
            if isinstance(sim_idx, int):
                label = f"Sim {sim_idx + 1} {label}"
            use_font = label_font if label_font is not None else font
            _draw_value_label(
                surface,
                use_font,
                rect,
                label,
                (px + 6, py - 6),
            )


def _draw_value_label(surface, font, rect: pygame.Rect, text: str, pos: tuple[int, int]) -> None:
    label = font.render(text, True, (235, 235, 235))
    pad = 4
    box = pygame.Rect(pos[0], pos[1], label.get_width() + pad * 2, label.get_height() + pad * 2)
    if box.right > rect.right:
        box.x = rect.right - box.width
    if box.left < rect.left:
        box.x = rect.left
    if box.bottom > rect.bottom:
        box.y = rect.bottom - box.height
    if box.top < rect.top:
        box.y = rect.top
    pygame.draw.rect(surface, (10, 10, 10), box)
    pygame.draw.rect(surface, (200, 200, 200), box, 1)
    surface.blit(label, (box.x + pad, box.y + pad))


def _confirm_quit_layout(window_w: int, header_top: int, y_offset: int, font) -> dict:
    prompt_lines = [
        "Quit master?",
        "Press Y to quit or N to cancel.",
    ]
    pad = 10
    line_h = font.get_height()
    max_w = max(font.size(line)[0] for line in prompt_lines)
    btn_w = 80
    btn_h = 28
    btn_gap = 12
    box_w = max(max_w + pad * 2, btn_w * 2 + btn_gap + pad * 2)
    text_h = line_h * len(prompt_lines)
    box_h = text_h + pad * 2 + btn_h + 8
    box_x = (window_w - box_w) // 2
    box_y = header_top + 40 + y_offset
    btn_y = box_y + pad + text_h + 8
    yes_x = box_x + (box_w - (btn_w * 2 + btn_gap)) // 2
    yes_rect = pygame.Rect(yes_x, btn_y, btn_w, btn_h)
    no_rect = pygame.Rect(yes_x + btn_w + btn_gap, btn_y, btn_w, btn_h)
    return {
        "prompt_lines": prompt_lines,
        "pad": pad,
        "line_h": line_h,
        "box_rect": pygame.Rect(box_x, box_y, box_w, box_h),
        "yes_rect": yes_rect,
        "no_rect": no_rect,
    }


def _load_mean_points(path: Path, value_field: str) -> list[tuple[float, float]]:
    if not path.exists():
        return []
    points = []
    try:
        with open(path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    x = float(row["evolution rate"])
                    y = float(row[value_field])
                except (ValueError, KeyError, TypeError):
                    continue
                points.append((x, y))
    except Exception:
        return []
    return points


def _mean_value_field(mean_kind: str) -> str:
    kind = mean_kind.lower()
    return f"{kind} mean length lived"


def _pick_mean_point(
    click_pos: tuple[int, int],
    rect: pygame.Rect,
    series: list[list[tuple[float, float]]],
    max_distance: int = 8,
):
    all_points = [pt for points in series for pt in points]
    if not all_points:
        return None

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
        return px + plot_left, py + plot_top

    best = None
    best_d2 = max_distance * max_distance
    cx, cy = click_pos
    for s_idx, points in enumerate(series):
        for x, y in points:
            px, py = _scale_point(x, y)
            dx = px - cx
            dy = py - cy
            d2 = dx * dx + dy * dy
            if d2 <= best_d2:
                best_d2 = d2
                best = {"sim_index": s_idx, "x": x, "y": y}
    return best


def _pick_fps_point(
    click_pos: tuple[int, int],
    rect: pygame.Rect,
    series: list[list[tuple]],
    max_distance: int = 8,
    max_seconds: float = 2.0,
):
    if not any(series):
        return None
    max_points = rect.width
    cx, cy = click_pos
    best = None
    best_d2 = max_distance * max_distance
    for s_idx, points in enumerate(series):
        if not points:
            continue
        recent = points[-max_points:]
        for i, point in enumerate(recent):
            if not point:
                continue
            ts = point[0]
            tval = point[1] if len(point) > 1 else None
            if tval is None:
                continue
            t_clamped = max(0.0, min(max_seconds, float(tval)))
            px = rect.x + i
            py = rect.y + rect.height - int((t_clamped / max_seconds) * rect.height)
            dx = px - cx
            dy = py - cy
            d2 = dx * dx + dy * dy
            if d2 <= best_d2:
                best_d2 = d2
                best = {
                    "sim_index": s_idx,
                    "timestamp": ts,
                    "interval": tval,
                    "point_index": i,
                }
    return best


def main() -> None:
    args = _parse_args()
    settings = load_settings()
    pygame.init()
    startup = _edit_startup_ui(settings, Path("results"))
    if startup is None:
        pygame.quit()
        return
    settings, continue_master_run, continue_settings = startup
    if continue_master_run is not None:
        if isinstance(continue_settings, dict):
            preserved_draw = settings.get("draw", True)
            preserved_num_tries = settings.get("num_tries", 0)
            preserved_num_master = settings.get("num_tries_master", 0)
            settings = continue_settings
            settings["draw"] = preserved_draw
            settings["num_tries"] = preserved_num_tries
            settings["num_tries_master"] = preserved_num_master
            try:
                current_global = load_settings()
                settings["num_tries"] = max(
                    int(settings.get("num_tries", 0)),
                    int(current_global.get("num_tries", 0)),
                )
                settings["num_tries_master"] = max(
                    int(settings.get("num_tries_master", 0)),
                    int(current_global.get("num_tries_master", 0)),
                )
            except Exception:
                pass
    else:
        settings = _edit_settings_ui(settings)
        if settings is None:
            pygame.quit()
            return
    try:
        settings_count = int(settings.get("simulations", {}).get("count", 3))
    except Exception:
        settings_count = 3
    count = args.count if args.count is not None else settings_count
    if count < 0:
        count = 0
    sim_path = Path(args.script)
    if not sim_path.exists():
        raise FileNotFoundError(f"Simulation script not found: {sim_path}")

    interpreter = sys.executable

    results_dir = Path("results")
    if continue_master_run is not None:
        master_run_num = int(continue_master_run)
        master_label = f"master_{master_run_num}"
        master_dir = results_dir / master_label
        master_dir.mkdir(parents=True, exist_ok=True)
        master_meta = _load_master_meta(master_dir)
        existing_run_nums = []
        if isinstance(master_meta, dict):
            raw_runs = master_meta.get("run_nums", [])
            if isinstance(raw_runs, list):
                for val in raw_runs:
                    try:
                        existing_run_nums.append(int(val))
                    except Exception:
                        continue
        existing_run_nums = sorted(set(existing_run_nums))
        run_nums = existing_run_nums
        count = len(run_nums)
        master_run_nums = existing_run_nums
    else:
        run_nums = _allocate_run_numbers(count)
        master_run_num = _allocate_master_run_number(results_dir)
        master_label = f"master_{master_run_num}"
        master_dir = results_dir / master_label
        master_dir.mkdir(parents=True, exist_ok=True)
        master_run_nums = run_nums

    _save_master_meta(
        master_dir,
        master_run_nums,
        settings,
        update_global=(continue_master_run is None),
    )

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

    fps_paths = [results_dir / str(run_num) / "fps_log.csv" for run_num in run_nums]
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
    label_font = pygame.font.SysFont("Consolas", 14)
    clock = pygame.time.Clock()

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
    master_fps_mode = _FPS_MODE_CAPPED
    mean_kind = "Arithmetic"
    selected_mean_point = None
    selected_fps_point = None
    full_throttle_active = False
    saved_draw_modes = None
    saved_mode_values = None
    pressed_button = None
    confirm_quit = False


    max_scroll = max(0, content_h - window_h)
    scroll_offset = 0
    scroll_step = max(30, panel_h // 2)

    running = True

    def _open_settings_dialog() -> None:
        nonlocal settings, screen, font, small_font, label_font
        nonlocal max_window_h, window_h, max_scroll, scroll_offset
        updated = _edit_settings_ui(settings, master_dir=master_dir, write_global_on_confirm=False)
        if updated is None:
            return
        settings = updated
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
        label_font = pygame.font.SysFont("Consolas", 14)
        max_scroll = max(0, content_h - window_h)
        scroll_offset = max(0, min(scroll_offset, max_scroll))

    
    def _apply_master_fps_mode(new_mode: int) -> None:
        nonlocal master_fps_mode, chart_refresh_s, master_fps
        nonlocal full_throttle_active, saved_draw_modes, saved_mode_values
        master_fps_mode = new_mode
        if master_fps_mode == _FPS_MODE_FULL_THROTTLE:
            chart_refresh_s = float("inf")
            master_fps = 0
            if not full_throttle_active:
                saved_draw_modes = draw_modes.copy()
                saved_mode_values = mode_values.copy()
                for i in range(len(draw_modes)):
                    draw_modes[i] = 2
                for i in range(len(mode_values)):
                    mode_values[i] = 0
                _write_control(
                    control_path,
                    active_sim_index,
                    enabled,
                    draw_modes,
                    draw_every,
                    mode_values,
                    update_tokens,
                )
                full_throttle_active = True
        else:
            if full_throttle_active:
                if saved_draw_modes is not None:
                    draw_modes[:] = saved_draw_modes
                if saved_mode_values is not None:
                    mode_values[:] = saved_mode_values
                _write_control(
                    control_path,
                    active_sim_index,
                    enabled,
                    draw_modes,
                    draw_every,
                    mode_values,
                    update_tokens,
                )
                full_throttle_active = False
            if master_fps_mode == _FPS_MODE_CAPPED:
                chart_refresh_s = base_chart_refresh_s
                master_fps = base_master_fps
            else:
                chart_refresh_s = 0.0
                master_fps = uncapped_fps

    def _ensure_selected_visible() -> None:
        nonlocal scroll_offset
        if max_scroll <= 0:
            return
        if selected_row == 0:
            scroll_offset = 0
            return
        if selected_row == count:
            scroll_offset = max_scroll
            return
        row_top = header_h + (selected_row - 1) * panel_h
        row_bottom = row_top + panel_h
        if row_top < scroll_offset:
            scroll_offset = row_top
        if row_bottom > scroll_offset + window_h:
            scroll_offset = row_bottom - window_h
        scroll_offset = max(0, min(max_scroll, scroll_offset))

    while running:
        margin = 20
        gap = 20
        chart_w = (window_w - margin * 2 - gap) // 2
        master_line_y = header_top + master_line_offset
        global_chart_y = master_line_y + global_chart_gap
        fps_all_rect_content = pygame.Rect(
            margin, global_chart_y, chart_w, global_chart_h
        )
        mean_all_rect_content = pygame.Rect(
            margin + chart_w + gap, global_chart_y, chart_w, global_chart_h
        )
        y_offset = -scroll_offset

        button_w = 110
        button_h = 26
        button_gap = 8
        button_x = window_w - button_w - 20
        button_y = header_top
        draw_btn = pygame.Rect(button_x, button_y, button_w, button_h)
        mode_btn = pygame.Rect(button_x, button_y + (button_h + button_gap), button_w, button_h)
        info_btn = pygame.Rect(button_x, button_y + 2 * (button_h + button_gap), button_w, button_h)
        mean_btn = pygame.Rect(button_x, button_y + 3 * (button_h + button_gap), button_w, button_h)
        fps_btn = pygame.Rect(button_x, button_y + 4 * (button_h + button_gap), button_w, button_h)
        onoff_btn = pygame.Rect(button_x, button_y + 5 * (button_h + button_gap), button_w, button_h)
        settings_btn = pygame.Rect(button_x, button_y + 6 * (button_h + button_gap), button_w, button_h)
        exit_btn = pygame.Rect(button_x, button_y + 7 * (button_h + button_gap), button_w, button_h)
        confirm_layout = _confirm_quit_layout(window_w, header_top, y_offset, small_font)

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                if confirm_quit:
                    running = False
                else:
                    confirm_quit = True
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE or event.key == pygame.K_q:
                    if confirm_quit:
                        running = False
                    else:
                        confirm_quit = True
                elif event.key == pygame.K_y:
                    if confirm_quit:
                        running = False
                elif event.key == pygame.K_n:
                    confirm_quit = False
                elif event.key == pygame.K_UP:
                    selected_row = (selected_row - 1) % (count + 1)
                    active_sim_index = master_active_index if selected_row == 0 else selected_row - 1
                    _ensure_selected_visible()
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_DOWN:
                    selected_row = (selected_row + 1) % (count + 1)
                    active_sim_index = master_active_index if selected_row == 0 else selected_row - 1
                    _ensure_selected_visible()
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
                    if master_fps_mode != _FPS_MODE_FULL_THROTTLE:
                        _apply_draw_toggle(selected_row, draw_modes)
                        _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_m:
                    if master_fps_mode != _FPS_MODE_FULL_THROTTLE:
                        _apply_mode_toggle(selected_row, mode_values)
                        _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_s:
                    _apply_update(selected_row, update_tokens)
                    _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
                elif event.key == pygame.K_f:
                    _apply_master_fps_mode((master_fps_mode + 1) % 3)
            elif event.type == pygame.MOUSEBUTTONUP and event.button == 1:
                if pressed_button is not None:
                    mx, my = event.pos
                    content_y = my + scroll_offset
                    if pressed_button == "draw" and draw_btn.collidepoint(mx, content_y):
                        if master_fps_mode != _FPS_MODE_FULL_THROTTLE:
                            _apply_draw_toggle(selected_row, draw_modes)
                            _write_control(
                                control_path,
                                active_sim_index,
                                enabled,
                                draw_modes,
                                draw_every,
                                mode_values,
                                update_tokens,
                            )
                    elif pressed_button == "mode" and mode_btn.collidepoint(mx, content_y):
                        if master_fps_mode != _FPS_MODE_FULL_THROTTLE:
                            _apply_mode_toggle(selected_row, mode_values)
                            _write_control(
                                control_path,
                                active_sim_index,
                                enabled,
                                draw_modes,
                                draw_every,
                                mode_values,
                                update_tokens,
                            )
                    elif pressed_button == "info" and info_btn.collidepoint(mx, content_y):
                        _apply_update(selected_row, update_tokens)
                        _write_control(
                            control_path,
                            active_sim_index,
                            enabled,
                            draw_modes,
                            draw_every,
                            mode_values,
                            update_tokens,
                        )
                    elif pressed_button == "mean" and mean_btn.collidepoint(mx, content_y):
                        mean_kind = "Geometric" if mean_kind == "Arithmetic" else "Arithmetic"
                        selected_mean_point = None
                        last_chart_refresh = 0.0
                    elif pressed_button == "fps" and fps_btn.collidepoint(mx, content_y):
                        _apply_master_fps_mode((master_fps_mode + 1) % 3)
                    elif pressed_button == "onoff" and onoff_btn.collidepoint(mx, content_y):
                        if selected_row == 0:
                            new_state = not all(enabled)
                            for i in range(count):
                                enabled[i] = new_state
                        else:
                            idx = selected_row - 1
                            if 0 <= idx < count:
                                enabled[idx] = not enabled[idx]
                        _write_control(
                            control_path,
                            active_sim_index,
                            enabled,
                            draw_modes,
                            draw_every,
                            mode_values,
                            update_tokens,
                        )
                    elif pressed_button == "settings" and settings_btn.collidepoint(mx, content_y):
                        _open_settings_dialog()
                    elif pressed_button == "exit" and exit_btn.collidepoint(mx, content_y):
                        if confirm_quit:
                            running = False
                        else:
                            confirm_quit = True
                    pressed_button = None
                    continue
            elif event.type == pygame.MOUSEWHEEL:
                if max_scroll > 0:
                    scroll_offset = max(
                        0, min(max_scroll, scroll_offset - event.y * scroll_step)
                    )
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                content_y = my + scroll_offset
                handled_click = False
                pressed_button = None
                if confirm_quit:
                    if confirm_layout["yes_rect"].collidepoint(mx, my):
                        running = False
                        confirm_quit = False
                        continue
                    if confirm_layout["no_rect"].collidepoint(mx, my):
                        confirm_quit = False
                        continue
                if draw_btn.collidepoint(mx, content_y):
                    pressed_button = "draw"
                    handled_click = True
                elif mode_btn.collidepoint(mx, content_y):
                    pressed_button = "mode"
                    handled_click = True
                elif info_btn.collidepoint(mx, content_y):
                    pressed_button = "info"
                    handled_click = True
                elif mean_btn.collidepoint(mx, content_y):
                    pressed_button = "mean"
                    handled_click = True
                elif fps_btn.collidepoint(mx, content_y):
                    pressed_button = "fps"
                    handled_click = True
                elif onoff_btn.collidepoint(mx, content_y):
                    pressed_button = "onoff"
                    handled_click = True
                elif settings_btn.collidepoint(mx, content_y):
                    pressed_button = "settings"
                    handled_click = True
                elif exit_btn.collidepoint(mx, content_y):
                    pressed_button = "exit"
                    handled_click = True
                if pressed_button is not None:
                    continue
                else:
                    handled_chart_click = False
                    if fps_all_rect_content.collidepoint(mx, content_y):
                        picked = _pick_fps_point(
                            (mx, content_y),
                            fps_all_rect_content,
                            fps_series,
                        )
                        if picked:
                            picked["scope"] = "global"
                            selected_fps_point = picked
                        else:
                            selected_fps_point = None
                        handled_chart_click = True
                    elif mean_all_rect_content.collidepoint(mx, content_y):
                        picked = _pick_mean_point(
                            (mx, content_y),
                            mean_all_rect_content,
                            mean_series,
                        )
                        if picked:
                            picked["scope"] = "global"
                            selected_mean_point = picked
                        else:
                            selected_mean_point = None
                        handled_chart_click = True
                    else:
                        row_start = header_h
                        row_h = panel_h
                        if content_y >= row_start:
                            idx = (content_y - row_start) // row_h
                            if 0 <= idx < count:
                                chart_y = row_start + idx * row_h + 20
                                fps_rect = pygame.Rect(
                                    margin, chart_y, chart_w, chart_h
                                )
                                mean_rect = pygame.Rect(
                                    margin + chart_w + gap, chart_y, chart_w, chart_h
                                )
                                if fps_rect.collidepoint(mx, content_y):
                                    picked = _pick_fps_point(
                                        (mx, content_y),
                                        fps_rect,
                                        [fps_series[idx]] if idx < len(fps_series) else [[]],
                                    )
                                    if picked:
                                        picked["scope"] = "sim"
                                        picked["sim_index"] = idx
                                        selected_fps_point = picked
                                    else:
                                        selected_fps_point = None
                                    handled_chart_click = True
                                elif mean_rect.collidepoint(mx, content_y):
                                    picked = _pick_mean_point(
                                        (mx, content_y),
                                        mean_rect,
                                        [mean_series[idx]] if idx < len(mean_series) else [[]],
                                    )
                                    if picked:
                                        picked["scope"] = "sim"
                                        picked["sim_index"] = idx
                                        selected_mean_point = picked
                                    else:
                                        selected_mean_point = None
                                    handled_chart_click = True
                    if handled_chart_click:
                        continue
                if handled_click:
                    continue
                if content_y < header_h:
                    selected_row = 0
                    active_sim_index = master_active_index
                    _ensure_selected_visible()
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
                        _ensure_selected_visible()
                        _write_control(control_path, active_sim_index, enabled, draw_modes, draw_every, mode_values, update_tokens)
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button in (4, 5):
                if max_scroll > 0:
                    if event.button == 4:
                        scroll_offset = max(0, scroll_offset - scroll_step)
                    else:
                        scroll_offset = min(max_scroll, scroll_offset + scroll_step)
        
        if master_fps_mode != _FPS_MODE_FULL_THROTTLE:
            screen.fill((20, 20, 20))
            title = font.render("Simulation Master", True, (240, 240, 240))
            if selected_row == 0:
                status_text = f"Selected: MASTER (0) | master_{master_run_num}"
            else:
                run_label = "?"
                if 0 <= (selected_row - 1) < len(run_nums):
                    run_label = str(run_nums[selected_row - 1])
                status_text = (
                    f"Selected: Sim {selected_row} / {count} | run {run_label} | master_{master_run_num}"
                )
            status = font.render(status_text, True, (0, 200, 255))
            hint1 = small_font.render("Up/Down: select sim", True, (200, 200, 200))
            hint2 = small_font.render("Left/Right or button: on/off", True, (200, 200, 200))
            if master_fps_mode == _FPS_MODE_CAPPED:
                cap_label = "CAPPED"
            elif master_fps_mode == _FPS_MODE_UNCAPPED:
                cap_label = "UNCAPPED"
            else:
                cap_label = "FULL"
            hint3 = small_font.render(
                "D: draw  M: mode  S: update  F: fps mode  Esc/Q: quit",
                True,
                (200, 200, 200),
            )
            hint4 = small_font.render(
                f"Mean: {mean_kind} | Master FPS: {cap_label} | Click dot: value",
                True,
                (200, 200, 200),
            )

            screen.blit(title, (20, header_top + y_offset))
            screen.blit(status, (20, header_top + 30 + y_offset))
            screen.blit(hint1, (20, header_top + 60 + y_offset))
            screen.blit(hint2, (20, header_top + 80 + y_offset))
            screen.blit(hint3, (20, header_top + 100 + y_offset))
            screen.blit(hint4, (20, header_top + 120 + y_offset))

            for rect, label in [
                (draw_btn, "Draw"),
                (mode_btn, "Mode"),
                (info_btn, "Info"),
                (mean_btn, f"Mean {mean_kind[:4]}"),
                (fps_btn, "FPS Mode"),
                (onoff_btn, "On/Off"),
                (settings_btn, "Settings"),
                (exit_btn, "Exit"),
            ]:
                draw_rect = rect.move(0, y_offset)
                pygame.draw.rect(screen, (40, 40, 40), draw_rect)
                pygame.draw.rect(screen, (120, 120, 120), draw_rect, 1)
                text = small_font.render(label, True, (220, 220, 220))
                screen.blit(text, (draw_rect.x + 8, draw_rect.y + 5))

            fps_all_rect = pygame.Rect(margin, global_chart_y + y_offset, chart_w, global_chart_h)
            mean_all_rect = pygame.Rect(margin + chart_w + gap, global_chart_y + y_offset, chart_w, global_chart_h)

            now = time.time()
            if now - last_chart_refresh >= chart_refresh_s:
                last_chart_refresh = now
                mean_value_field = _mean_value_field(mean_kind)
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
                        / f"parsed{mean_kind}MeanSimulatino{run_nums[idx]}_Log.csv"
                    )
                    mean_points = _get_cached_points(
                        arithmetic_cache,
                        mean_path,
                        lambda p, field=mean_value_field: _load_mean_points(p, field),
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

            if run_nums:
                run_min = min(run_nums)
                run_max = max(run_nums)
                if run_min == run_max:
                    run_range = f"run {run_min}"
                else:
                    run_range = f"runs {run_min}-{run_max}"
            else:
                run_range = "runs --"

            master_line = small_font.render(
                f"MASTER (0) master_{master_run_num} ({run_range}): {master_state} | {master_draw} | {master_mode}",
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
            highlight_sim = None
            if selected_fps_point and isinstance(selected_fps_point.get("sim_index"), int):
                highlight_sim = int(selected_fps_point.get("sim_index"))
            elif selected_row > 0:
                highlight_sim = selected_row - 1
            _draw_multi_fps_chart(
                screen,
                small_font,
                fps_all_rect,
                fps_series,
                selected_point=selected_fps_point if selected_fps_point and selected_fps_point.get("scope") == "global" else None,
                highlight_sim=highlight_sim,
                meta_series=meta_series,
                now_time=time.time(),
                label_font=label_font,
            )
            _draw_multi_arithmetic_chart(
                screen,
                small_font,
                mean_all_rect,
                mean_series,
                selected_point=selected_mean_point,
                highlight_sim=(
                    selected_mean_point.get("sim_index")
                    if selected_mean_point
                    else (selected_row - 1 if selected_row > 0 else None)
                ),
                label_font=label_font,
            )

            if confirm_quit:
                prompt_lines = confirm_layout["prompt_lines"]
                pad = confirm_layout["pad"]
                line_h = confirm_layout["line_h"]
                box_rect = confirm_layout["box_rect"]
                yes_rect = confirm_layout["yes_rect"]
                no_rect = confirm_layout["no_rect"]
                overlay = pygame.Surface((box_rect.width, box_rect.height), pygame.SRCALPHA)
                overlay.fill((10, 10, 10, 230))
                screen.blit(overlay, (box_rect.x, box_rect.y))
                pygame.draw.rect(screen, (180, 180, 180), box_rect, 1)
                y_text = box_rect.y + pad
                for line in prompt_lines:
                    text = small_font.render(line, True, (230, 230, 230))
                    screen.blit(text, (box_rect.x + pad, y_text))
                    y_text += line_h
                pygame.draw.rect(screen, (60, 60, 60), yes_rect)
                pygame.draw.rect(screen, (160, 160, 160), yes_rect, 1)
                yes_text = small_font.render("Yes", True, (230, 230, 230))
                screen.blit(
                    yes_text,
                    (
                        yes_rect.x + (yes_rect.width - yes_text.get_width()) // 2,
                        yes_rect.y + 6,
                    ),
                )
                pygame.draw.rect(screen, (60, 60, 60), no_rect)
                pygame.draw.rect(screen, (160, 160, 160), no_rect, 1)
                no_text = small_font.render("No", True, (230, 230, 230))
                screen.blit(
                    no_text,
                    (
                        no_rect.x + (no_rect.width - no_text.get_width()) // 2,
                        no_rect.y + 6,
                    ),
                )

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
                run_label = "?"
                if 0 <= idx < len(run_nums):
                    run_label = str(run_nums[idx])
                label = f"Sim {idx + 1} (run {run_label})"
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
                fps_label = None
                if selected_fps_point and selected_fps_point.get("scope") == "sim":
                    if selected_fps_point.get("sim_index") == idx:
                        meta = meta_series[idx] if idx < len(meta_series) else {}
                        fps_label = _format_fps_label(
                            idx,
                            selected_fps_point.get("timestamp"),
                            selected_fps_point.get("interval"),
                            meta,
                            time.time(),
                        )
                _draw_fps_chart(
                    screen,
                    small_font,
                    fps_rect,
                    fps_points,
                    sim_color,
                    selected_point=selected_fps_point,
                    selected_idx=idx,
                    selected_label=fps_label,
                    label_font=label_font,
                )

                mean_points = mean_series[idx] if idx < len(mean_series) else []
                _draw_arithmetic_chart(
                    screen,
                    small_font,
                    mean_rect,
                    mean_points,
                    sim_color,
                    selected_point=selected_mean_point,
                    selected_idx=idx,
                    label_font=label_font,
                )

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
        _combine_master_logs(results_dir, master_dir, master_label, master_run_nums)
    except Exception as e:
        print(f"Failed to build master logs: {e}")
    else:
        try:
            parse_run(results_dir, master_label, quiet=True)
        except Exception as e:
            print(f"Failed to parse master logs (step 0.001): {e}")
        try:
            parse_run(
                results_dir,
                master_label,
                step=0.01,
                quiet=True,
                output_tag="step0p01",
            )
        except Exception as e:
            print(f"Failed to parse master logs (step 0.01): {e}")
    try:
        _combine_master_means(results_dir, master_dir, master_label, master_run_nums)
    except Exception as e:
        print(f"Failed to build combined mean files: {e}")

    try:
        control_path.unlink(missing_ok=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()
