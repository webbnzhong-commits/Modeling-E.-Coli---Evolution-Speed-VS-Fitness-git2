import argparse
import csv
from bisect import bisect_right
from collections import deque
import json
import math
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
from datetime import datetime, timezone, timedelta
import time

import pygame
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

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
_LA_TZ_NAME = "America/Los_Angeles"
if ZoneInfo is not None:
    try:
        _LA_TZ = ZoneInfo(_LA_TZ_NAME)
    except Exception:
        _LA_TZ = timezone(timedelta(hours=-8), "PST")
else:
    _LA_TZ = timezone(timedelta(hours=-8), "PST")


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


def _draw_text_with_caret(
    surface,
    font,
    text: str,
    pos: tuple[int, int],
    color: tuple[int, int, int],
    show_caret: bool = False,
    max_caret_x: int | None = None,
):
    text_surf = font.render(text, True, color)
    x, y = pos
    surface.blit(text_surf, (x, y))
    if not show_caret:
        return
    # Blink at ~2 Hz while editing.
    if int(time.time() * 2) % 2 != 0:
        return
    caret_x = x + text_surf.get_width() + 1
    if isinstance(max_caret_x, int):
        caret_x = min(caret_x, max_caret_x)
    top = y + 2
    bottom = y + max(2, text_surf.get_height() - 2)
    pygame.draw.line(surface, color, (caret_x, top), (caret_x, bottom), 1)


def _apply_master_graph_settings(settings: dict) -> None:
    global _DOT_ALPHA, _DOT_ALPHA_HI, _DOT_ALPHA_DIM, _DOT_RADIUS
    cfg = settings.get("master_graph", {}) if isinstance(settings, dict) else {}
    try:
        alpha = int(cfg.get("dot_alpha", _DOT_ALPHA))
    except Exception:
        alpha = _DOT_ALPHA
    alpha = max(10, min(255, alpha))
    try:
        radius = int(cfg.get("dot_radius", _DOT_RADIUS))
    except Exception:
        radius = _DOT_RADIUS
    radius = max(1, min(20, radius))
    _DOT_ALPHA = alpha
    _DOT_ALPHA_HI = min(255, int(alpha * 2))
    _DOT_ALPHA_DIM = max(5, int(alpha * 0.55))
    _DOT_RADIUS = radius


def _parse_stop_datetime(text: str):
    if not isinstance(text, str):
        return None
    value = text.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt_local = datetime.strptime(value, fmt)
            dt_la = dt_local.replace(tzinfo=_LA_TZ)
            return dt_la.timestamp()
        except Exception:
            continue
    return None


def _edit_stop_conditions_ui(settings: dict):
    cond = settings.get("stop_conditions", {}) if isinstance(settings, dict) else {}
    local = {
        "runtime_enabled": bool(cond.get("runtime_enabled", False)),
        "max_runtime_hours": float(cond.get("max_runtime_hours", 0) or 0),
        "frames_enabled": bool(cond.get("frames_enabled", False)),
        "max_frames": int(cond.get("max_frames", 0) or 0),
        "species_enabled": bool(cond.get("species_enabled", False)),
        "max_species": int(cond.get("max_species", 0) or 0),
        "datetime_enabled": bool(cond.get("datetime_enabled", False)),
        "stop_at_datetime": str(cond.get("stop_at_datetime", "") or ""),
    }
    items = [
        {"key": "runtime_enabled", "label": "Enable runtime limit", "type": "bool"},
        {"key": "max_runtime_hours", "label": "Max runtime hours", "type": "number"},
        {"key": "frames_enabled", "label": "Enable frames limit", "type": "bool"},
        {"key": "max_frames", "label": "Max frames", "type": "number"},
        {"key": "species_enabled", "label": "Enable species limit", "type": "bool"},
        {"key": "max_species", "label": "Max species", "type": "number"},
        {"key": "datetime_enabled", "label": "Enable stop at datetime", "type": "bool"},
        {"key": "stop_at_datetime", "label": "Stop at LA time (YYYY-MM-DD HH:MM[:SS])", "type": "text"},
    ]

    screen_w = 720
    screen_h = 420
    screen = pygame.display.set_mode((screen_w, screen_h))
    pygame.display.set_caption("Stop Conditions")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 18)
    clock = pygame.time.Clock()

    selected = 0
    editing = False
    edit_text = ""
    error_text = ""
    exit_rect = pygame.Rect(screen_w - 90, 16, 70, 26)
    confirm_rect = pygame.Rect(screen_w - 160, screen_h - 60, 140, 36)

    while True:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return None
            if event.type == pygame.KEYDOWN:
                if editing:
                    if event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        key = items[selected]["key"]
                        if items[selected]["type"] == "number":
                            new_val = _parse_numeric(edit_text.strip(), local[key])
                            if new_val is None:
                                error_text = "Invalid number"
                            else:
                                if key in ("max_frames", "max_species"):
                                    new_val = max(0, int(new_val))
                                else:
                                    new_val = max(0.0, float(new_val))
                                local[key] = new_val
                                editing = False
                                edit_text = ""
                                error_text = ""
                        elif items[selected]["type"] == "bool":
                            local[key] = bool(edit_text.strip().lower() in ("true", "1", "yes", "y", "on"))
                            editing = False
                            edit_text = ""
                            error_text = ""
                        else:
                            local[key] = edit_text.strip()
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
                        if ch and ch.isprintable():
                            edit_text += ch
                else:
                    if event.key == pygame.K_ESCAPE:
                        return None
                    if event.key == pygame.K_UP:
                        selected = (selected - 1) % len(items)
                    elif event.key == pygame.K_DOWN:
                        selected = (selected + 1) % len(items)
                    elif event.key in (pygame.K_LEFT, pygame.K_RIGHT):
                        item = items[selected]
                        if item["type"] == "bool":
                            local[item["key"]] = not bool(local[item["key"]])
                        elif item["type"] == "number":
                            key = item["key"]
                            step = _numeric_step(local[key])
                            delta = step if event.key == pygame.K_RIGHT else -step
                            new_val = float(local[key]) + delta
                            if key in ("max_frames", "max_species"):
                                new_val = max(0, int(round(new_val)))
                            else:
                                decimals = _step_decimals(step)
                                new_val = max(0.0, round(new_val, decimals))
                            local[key] = new_val
                    elif event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        editing = True
                        key = items[selected]["key"]
                        edit_text = str(local[key])
                        error_text = ""
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if exit_rect.collidepoint(mx, my):
                    return None
                if confirm_rect.collidepoint(mx, my):
                    if local.get("datetime_enabled"):
                        dt_text = local.get("stop_at_datetime", "")
                        if dt_text:
                            if _parse_stop_datetime(dt_text) is None:
                                error_text = "Invalid datetime"
                                continue
                    settings["stop_conditions"] = local
                    save_settings(settings)
                    return settings
                list_top = 90
                line_h = small_font.get_height() + 8
                for idx in range(len(items)):
                    rect = pygame.Rect(16, list_top + idx * line_h - 2, screen_w - 32, line_h)
                    if rect.collidepoint(mx, my):
                        selected = idx
                        editing = True
                        key = items[selected]["key"]
                        edit_text = str(local[key])
                        error_text = ""
                        break

        screen.fill((18, 18, 18))
        title = font.render("Stop Conditions", True, (230, 230, 230))
        screen.blit(title, (20, 20))
        pygame.draw.rect(screen, (60, 60, 60), exit_rect)
        pygame.draw.rect(screen, (160, 160, 160), exit_rect, 1)
        exit_text = small_font.render("Exit", True, (230, 230, 230))
        screen.blit(
            exit_text,
            (
                exit_rect.x + (exit_rect.width - exit_text.get_width()) // 2,
                exit_rect.y + 4,
            ),
        )

        list_top = 90
        line_h = small_font.get_height() + 8
        for idx, item in enumerate(items):
            key = item["key"]
            val = local[key]
            line = f"{item['label']}: {val}"
            color = (230, 230, 230) if idx == selected else (200, 200, 200)
            if idx == selected:
                pygame.draw.rect(screen, (35, 35, 35), (16, list_top + idx * line_h - 2, screen_w - 32, line_h))
            text = small_font.render(line, True, color)
            screen.blit(text, (22, list_top + idx * line_h))

        hint = small_font.render(
            "Up/Down select  Left/Right adjust  Enter edit  Esc cancel",
            True,
            (180, 180, 180),
        )
        screen.blit(hint, (20, screen_h - 80))

        if editing:
            edit_line = f"Edit: {edit_text}"
            edit_color = (255, 220, 160) if not error_text else (255, 160, 160)
            _draw_text_with_caret(
                screen,
                small_font,
                edit_line,
                (20, screen_h - 55),
                edit_color,
                show_caret=True,
                max_caret_x=screen_w - 12,
            )
        if error_text:
            err_surf = small_font.render(error_text, True, (255, 160, 160))
            screen.blit(err_surf, (20, screen_h - 35))

        now_label = _format_wall_time(time.time())
        parsed_ts = None
        parsed_label = "Off"
        if local.get("datetime_enabled"):
            parsed_ts = _parse_stop_datetime(local.get("stop_at_datetime", ""))
            parsed_label = _format_wall_time(parsed_ts) if parsed_ts else "Invalid"
        now_line = small_font.render(f"Now (LA): {now_label}", True, (180, 180, 180))
        parsed_line = small_font.render(f"Parsed (LA): {parsed_label}", True, (180, 180, 180))
        screen.blit(now_line, (20, screen_h - 110))
        screen.blit(parsed_line, (20, screen_h - 90))

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

        pygame.display.flip()
        clock.tick(30)


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
    editing_message = False
    message_text = ""
    message_value = ""

    list_top = 70
    list_bottom = screen_h - 120
    line_h = small_font.get_height() + 6
    visible_count = max(1, (list_bottom - list_top) // line_h)

    confirm_rect = pygame.Rect(screen_w - 160, screen_h - 60, 140, 36)
    upload_rect = pygame.Rect(screen_w - 320, screen_h - 60, 140, 36)
    exit_rect = pygame.Rect(screen_w - 90, 16, 70, 26)
    upload_notice = ""
    upload_notice_time = 0.0
    message_rect = pygame.Rect(20, list_bottom + 8, screen_w - 40, 24)

    if master_dir is not None:
        message_value = _read_master_message(master_dir)
        message_text = message_value

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
                if editing_message:
                    if event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        message_value = message_text.strip() or "debug run"
                        _write_master_message(master_dir, message_value)
                        editing_message = False
                    elif event.key == pygame.K_ESCAPE:
                        editing_message = False
                        message_text = message_value
                    elif event.key == pygame.K_BACKSPACE:
                        message_text = message_text[:-1]
                    else:
                        ch = event.unicode
                        if ch and ch.isprintable() and len(message_text) < 80:
                            message_text += ch
                elif editing:
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
                    if master_dir is not None and event.key == pygame.K_m:
                        editing_message = True
                        message_text = message_value
                        continue
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
                if exit_rect.collidepoint(mx, my):
                    return None
                if confirm_rect.collidepoint(mx, my):
                    if master_dir is not None:
                        if editing_message:
                            message_value = message_text.strip() or "debug run"
                            editing_message = False
                        _write_master_message(master_dir, message_value)
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
                if master_dir is not None and message_rect.collidepoint(mx, my):
                    editing_message = True
                    message_text = message_value
                    continue
                if list_top <= my <= list_bottom:
                    idx = (my - list_top) // line_h + scroll
                    if 0 <= idx < len(items):
                        selected = int(idx)
                        _ensure_visible()

        screen.fill((18, 18, 18))
        title = font.render("Master Settings (edit numbers, then confirm)", True, (230, 230, 230))
        screen.blit(title, (20, 20))
        pygame.draw.rect(screen, (60, 60, 60), exit_rect)
        pygame.draw.rect(screen, (160, 160, 160), exit_rect, 1)
        exit_text = small_font.render("Exit", True, (230, 230, 230))
        screen.blit(
            exit_text,
            (
                exit_rect.x + (exit_rect.width - exit_text.get_width()) // 2,
                exit_rect.y + 4,
            ),
        )

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

        hint_text = "Up/Down: select  Left/Right: adjust  Enter: edit  Esc: cancel"
        if master_dir is not None:
            hint_text += "  M: edit message"
        hint = small_font.render(hint_text, True, (180, 180, 180))
        screen.blit(hint, (20, screen_h - 80))

        if editing:
            edit_line = f"Edit {items[selected]['label']}: {edit_text}"
            edit_color = (255, 220, 160) if not error_text else (255, 160, 160)
            _draw_text_with_caret(
                screen,
                small_font,
                edit_line,
                (20, screen_h - 55),
                edit_color,
                show_caret=True,
                max_caret_x=screen_w - 12,
            )
        if error_text:
            err_surf = small_font.render(error_text, True, (255, 160, 160))
            screen.blit(err_surf, (20, screen_h - 35))

        if master_dir is not None:
            msg_display = message_text if editing_message else message_value
            msg_color = (255, 220, 160) if editing_message else (210, 210, 210)
            pygame.draw.rect(screen, (28, 28, 28), message_rect)
            pygame.draw.rect(screen, (120, 120, 120), message_rect, 1)
            msg_line_text = f"Message: {msg_display}"
            _draw_text_with_caret(
                screen,
                small_font,
                msg_line_text,
                (message_rect.x + 6, message_rect.y + 3),
                msg_color,
                show_caret=editing_message,
                max_caret_x=message_rect.right - 6,
            )

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
        clock.tick(0)


def _master_meta_path(master_dir: Path) -> Path:
    return master_dir / "master_meta.json"


def _master_settings_path(master_dir: Path) -> Path:
    return master_dir / "settings.json"


def _master_message_path(master_dir: Path) -> Path:
    return master_dir / "message"


def _ensure_master_message(master_dir: Path, default_message: str = "debug run") -> None:
    path = _master_message_path(master_dir)
    try:
        if path.exists() and path.stat().st_size > 0:
            return
        path.write_text(default_message)
    except Exception:
        pass


def _read_master_message(master_dir: Path, default_message: str = "debug run") -> str:
    _ensure_master_message(master_dir, default_message)
    path = _master_message_path(master_dir)
    try:
        text = path.read_text().strip()
        return text if text else default_message
    except Exception:
        return default_message


def _write_master_message(master_dir: Path, message: str, default_message: str = "debug run") -> None:
    text = str(message).strip()
    if not text:
        text = default_message
    try:
        _master_message_path(master_dir).write_text(text)
    except Exception:
        pass


def _maybe_autoupdate_master_message(
    master_dir: Path,
    elapsed_seconds: float,
    threshold_seconds: float = 20 * 60,
    default_message: str = "debug run",
    new_message: str = "prob good",
) -> None:
    try:
        elapsed_val = float(elapsed_seconds)
    except Exception:
        return
    if elapsed_val < threshold_seconds:
        return
    current = _read_master_message(master_dir, default_message=default_message)
    if current.strip().lower() == default_message.lower():
        _write_master_message(master_dir, new_message, default_message=default_message)


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
        _ensure_master_message(master_dir)
        if update_global:
            save_settings(settings)
    except Exception:
        pass


def _parse_hub_index(path: Path):
    try:
        return int(path.name.split("_", 1)[1])
    except Exception:
        return None


def _parse_env_rate_from_dir(path: Path):
    name = path.name
    if not name.startswith("env_"):
        return None
    token = name[4:].replace("p", ".")
    try:
        return float(token)
    except Exception:
        return None


def _master_search_root(results_dir: Path) -> Path:
    root = Path(results_dir)
    for candidate in [root, *root.parents]:
        if candidate.name == "results":
            return candidate
    return root


def _parse_master_run_from_dir(path: Path):
    try:
        name = Path(path).name
    except Exception:
        return None
    if not name.startswith("master_"):
        return None
    try:
        return int(name.split("_", 1)[1])
    except Exception:
        return None


def _resolve_continue_master(
    results_dir: Path,
    continue_master_run,
    continue_master_dir,
):
    run_num = None
    if continue_master_run is not None:
        try:
            run_num = int(continue_master_run)
        except Exception:
            run_num = None

    dir_path = None
    if continue_master_dir is not None:
        try:
            candidate = Path(continue_master_dir)
            if candidate.is_dir():
                dir_path = candidate
        except Exception:
            dir_path = None

    if dir_path is not None:
        parsed = _parse_master_run_from_dir(dir_path)
        if parsed is not None:
            # Directory identity is authoritative when both id+path are provided.
            run_num = int(parsed)
        if run_num is not None:
            return run_num, dir_path, dir_path.parent

    if run_num is None:
        return None, None, results_dir

    # Prefer masters in the current results scope before global search.
    direct_local = results_dir / f"master_{run_num}"
    if direct_local.is_dir():
        return run_num, direct_local, direct_local.parent

    for path in sorted(results_dir.rglob(f"master_{run_num}")):
        if path.is_dir():
            return run_num, path, path.parent

    search_root = _master_search_root(results_dir)
    direct_root = search_root / f"master_{run_num}"
    if direct_root.is_dir():
        return run_num, direct_root, direct_root.parent

    for path in sorted(search_root.rglob(f"master_{run_num}")):
        if path.is_dir():
            return run_num, path, path.parent

    return run_num, None, results_dir


def _collect_hub_env_dirs(results_dir: Path):
    entries = []
    for hub_dir in sorted(results_dir.glob("hub_*")):
        if not hub_dir.is_dir():
            continue
        hub_idx = _parse_hub_index(hub_dir)
        if hub_idx is None:
            continue
        for env_dir in sorted(hub_dir.iterdir()):
            if not env_dir.is_dir():
                continue
            if not env_dir.name.startswith("env_"):
                continue
            master_count = 0
            for master_path in env_dir.glob("master_*"):
                if master_path.is_dir():
                    master_count += 1
            if master_count <= 0:
                continue
            env_rate = _parse_env_rate_from_dir(env_dir)
            entries.append(
                {
                    "hub_idx": int(hub_idx),
                    "hub_dir": hub_dir,
                    "env_dir": env_dir,
                    "env_rate": env_rate,
                    "master_count": int(master_count),
                }
            )
    entries.sort(
        key=lambda e: (
            int(e["hub_idx"]),
            float("inf") if e["env_rate"] is None else float(e["env_rate"]),
            str(e["env_dir"]),
        )
    )
    return entries


def _collect_hub_dirs(results_dir: Path):
    hubs = []
    for hub_dir in sorted(results_dir.glob("hub_*")):
        if not hub_dir.is_dir():
            continue
        hub_idx = _parse_hub_index(hub_dir)
        if hub_idx is None:
            continue
        env_count = 0
        master_count = 0
        for env_dir in sorted(hub_dir.iterdir()):
            if not env_dir.is_dir() or not env_dir.name.startswith("env_"):
                continue
            local_masters = 0
            for master_path in env_dir.glob("master_*"):
                if master_path.is_dir():
                    local_masters += 1
            if local_masters <= 0:
                continue
            env_count += 1
            master_count += int(local_masters)
        if env_count <= 0:
            continue
        hubs.append(
            {
                "hub_idx": int(hub_idx),
                "hub_dir": hub_dir,
                "env_count": int(env_count),
                "master_count": int(master_count),
            }
        )
    hubs.sort(key=lambda e: int(e["hub_idx"]))
    return hubs


def _select_hub_ui(results_dir: Path):
    hub_entries = _collect_hub_dirs(results_dir)
    if not hub_entries:
        return None

    screen_w = 760
    screen_h = 520
    screen = pygame.display.set_mode((screen_w, screen_h))
    pygame.display.set_caption("Select Hub")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 18)
    clock = pygame.time.Clock()

    selected = 0
    scroll = 0
    list_top = 80
    list_bottom = screen_h - 90
    line_h = small_font.get_height() + 8
    visible = max(1, (list_bottom - list_top) // line_h)
    list_left = 20
    list_w = 300
    detail_x = list_left + list_w + 20
    detail_w = screen_w - detail_x - 20
    choose_rect = pygame.Rect(detail_x, screen_h - 70, detail_w, 30)
    exit_rect = pygame.Rect(screen_w - 90, 16, 70, 26)

    def _ensure_visible():
        nonlocal scroll
        if selected < scroll:
            scroll = selected
        elif selected >= scroll + visible:
            scroll = selected - visible + 1
        scroll = max(0, min(scroll, max(0, len(hub_entries) - visible)))

    _ensure_visible()
    while True:
        selected = max(0, min(selected, len(hub_entries) - 1))
        current = hub_entries[selected]
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return None
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE:
                    return None
                if event.key == pygame.K_UP:
                    selected = (selected - 1) % len(hub_entries)
                    _ensure_visible()
                elif event.key == pygame.K_DOWN:
                    selected = (selected + 1) % len(hub_entries)
                    _ensure_visible()
                elif event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                    return int(current["hub_idx"])
            if event.type == pygame.MOUSEWHEEL:
                if event.y > 0:
                    selected = (selected - 1) % len(hub_entries)
                elif event.y < 0:
                    selected = (selected + 1) % len(hub_entries)
                _ensure_visible()
            if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if exit_rect.collidepoint(mx, my):
                    return None
                if choose_rect.collidepoint(mx, my):
                    return int(current["hub_idx"])
                if list_left <= mx <= list_left + list_w and list_top <= my <= list_bottom:
                    idx = (my - list_top) // line_h + scroll
                    if 0 <= idx < len(hub_entries):
                        selected = int(idx)
                        _ensure_visible()

        screen.fill((18, 18, 18))
        title = font.render("Hub Selection", True, (230, 230, 230))
        screen.blit(title, (20, 20))
        pygame.draw.rect(screen, (60, 60, 60), exit_rect)
        pygame.draw.rect(screen, (160, 160, 160), exit_rect, 1)
        exit_text = small_font.render("Exit", True, (230, 230, 230))
        screen.blit(
            exit_text,
            (
                exit_rect.x + (exit_rect.width - exit_text.get_width()) // 2,
                exit_rect.y + 4,
            ),
        )

        list_title = small_font.render("Hubs", True, (200, 200, 200))
        screen.blit(list_title, (list_left, list_top - 26))
        for idx in range(scroll, min(len(hub_entries), scroll + visible)):
            entry = hub_entries[idx]
            y = list_top + (idx - scroll) * line_h
            if idx == selected:
                pygame.draw.rect(screen, (35, 35, 35), (list_left - 4, y - 2, list_w, line_h))
            color = (0, 200, 255) if idx == selected else (220, 220, 220)
            text = small_font.render(f"hub_{entry['hub_idx']}", True, color)
            screen.blit(text, (list_left, y))

        pygame.draw.rect(screen, (30, 30, 30), (detail_x, list_top, detail_w, list_bottom - list_top))
        pygame.draw.rect(screen, (80, 80, 80), (detail_x, list_top, detail_w, list_bottom - list_top), 1)
        detail_lines = [
            f"hub_{current['hub_idx']}",
            f"env buckets: {current['env_count']}",
            f"masters: {current['master_count']}",
            f"path: {current['hub_dir']}",
        ]
        y = list_top + 10
        for line in detail_lines:
            line_surf = small_font.render(line, True, (220, 220, 220))
            screen.blit(line_surf, (detail_x + 8, y))
            y += line_h

        pygame.draw.rect(screen, (40, 40, 40), choose_rect)
        pygame.draw.rect(screen, (180, 180, 180), choose_rect, 1)
        choose_text = small_font.render("Choose Hub", True, (230, 230, 230))
        screen.blit(
            choose_text,
            (
                choose_rect.x + (choose_rect.width - choose_text.get_width()) // 2,
                choose_rect.y + 6,
            ),
        )

        hint = small_font.render(
            "Up/Down select  Enter choose  Esc cancel",
            True,
            (180, 180, 180),
        )
        screen.blit(hint, (20, screen_h - 45))
        pygame.display.flip()
        clock.tick(30)


def _select_hub_env_ui(results_dir: Path, hub_idx_filter=None):
    env_entries = _collect_hub_env_dirs(results_dir)
    if hub_idx_filter is not None:
        env_entries = [
            entry for entry in env_entries if int(entry.get("hub_idx", -1)) == int(hub_idx_filter)
        ]
    if not env_entries:
        return None
    hubs = sorted({int(entry["hub_idx"]) for entry in env_entries})

    screen_w = 800
    screen_h = 520
    screen = pygame.display.set_mode((screen_w, screen_h))
    if len(hubs) == 1:
        pygame.display.set_caption(f"Select Env (hub_{hubs[0]})")
    else:
        pygame.display.set_caption("Select Hub")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 18)
    clock = pygame.time.Clock()

    selected_hub_idx = 0
    selected_env_idx = 0
    hub_scroll = 0
    env_scroll = 0

    list_top = 80
    list_bottom = screen_h - 80
    line_h = small_font.get_height() + 6
    visible_hubs = max(1, (list_bottom - list_top) // line_h)
    visible_envs = visible_hubs
    hub_left = 20
    hub_w = 180
    env_left = hub_left + hub_w + 20
    env_w = 340
    detail_x = env_left + env_w + 20
    detail_w = screen_w - detail_x - 20
    choose_rect = pygame.Rect(detail_x, screen_h - 70, detail_w, 30)
    exit_rect = pygame.Rect(screen_w - 90, 16, 70, 26)

    def _hub_envs(hub_idx):
        return [entry for entry in env_entries if int(entry["hub_idx"]) == int(hub_idx)]

    def _ensure_visible():
        nonlocal hub_scroll, env_scroll
        if selected_hub_idx < hub_scroll:
            hub_scroll = selected_hub_idx
        elif selected_hub_idx >= hub_scroll + visible_hubs:
            hub_scroll = selected_hub_idx - visible_hubs + 1
        hub_scroll = max(0, min(hub_scroll, max(0, len(hubs) - visible_hubs)))

        current_envs = _hub_envs(hubs[selected_hub_idx])
        if selected_env_idx < env_scroll:
            env_scroll = selected_env_idx
        elif selected_env_idx >= env_scroll + visible_envs:
            env_scroll = selected_env_idx - visible_envs + 1
        env_scroll = max(0, min(env_scroll, max(0, len(current_envs) - visible_envs)))

    _ensure_visible()
    while True:
        current_hub = hubs[selected_hub_idx]
        current_envs = _hub_envs(current_hub)
        if not current_envs:
            return None
        selected_env_idx = max(0, min(selected_env_idx, len(current_envs) - 1))
        selected_entry = current_envs[selected_env_idx]

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return None
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE:
                    return None
                if event.key == pygame.K_UP:
                    selected_env_idx = (selected_env_idx - 1) % len(current_envs)
                    _ensure_visible()
                elif event.key == pygame.K_DOWN:
                    selected_env_idx = (selected_env_idx + 1) % len(current_envs)
                    _ensure_visible()
                elif event.key == pygame.K_LEFT and len(hubs) > 1:
                    selected_hub_idx = (selected_hub_idx - 1) % len(hubs)
                    selected_env_idx = 0
                    _ensure_visible()
                elif event.key == pygame.K_RIGHT and len(hubs) > 1:
                    selected_hub_idx = (selected_hub_idx + 1) % len(hubs)
                    selected_env_idx = 0
                    _ensure_visible()
                elif event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                    return selected_entry["env_dir"]
            if event.type == pygame.MOUSEWHEEL:
                if event.y > 0:
                    selected_env_idx = (selected_env_idx - 1) % len(current_envs)
                elif event.y < 0:
                    selected_env_idx = (selected_env_idx + 1) % len(current_envs)
                _ensure_visible()
            if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if exit_rect.collidepoint(mx, my):
                    return None
                if choose_rect.collidepoint(mx, my):
                    return selected_entry["env_dir"]
                if list_top <= my <= list_bottom:
                    if len(hubs) > 1 and hub_left <= mx <= hub_left + hub_w:
                        idx = (my - list_top) // line_h + hub_scroll
                        if 0 <= idx < len(hubs):
                            selected_hub_idx = int(idx)
                            selected_env_idx = 0
                            _ensure_visible()
                    elif env_left <= mx <= env_left + env_w:
                        idx = (my - list_top) // line_h + env_scroll
                        if 0 <= idx < len(current_envs):
                            selected_env_idx = int(idx)
                            _ensure_visible()

        screen.fill((18, 18, 18))
        if len(hubs) == 1:
            title = font.render(f"hub_{hubs[0]} -> Env Selection", True, (230, 230, 230))
        else:
            title = font.render("Hub -> Env Selection", True, (230, 230, 230))
        screen.blit(title, (20, 20))
        pygame.draw.rect(screen, (60, 60, 60), exit_rect)
        pygame.draw.rect(screen, (160, 160, 160), exit_rect, 1)
        exit_text = small_font.render("Exit", True, (230, 230, 230))
        screen.blit(
            exit_text,
            (
                exit_rect.x + (exit_rect.width - exit_text.get_width()) // 2,
                exit_rect.y + 4,
            ),
        )

        hubs_title = small_font.render("Hubs", True, (200, 200, 200))
        envs_title = small_font.render("Env Buckets", True, (200, 200, 200))
        screen.blit(hubs_title, (hub_left, list_top - 26))
        screen.blit(envs_title, (env_left, list_top - 26))

        for idx in range(hub_scroll, min(len(hubs), hub_scroll + visible_hubs)):
            hub_idx = hubs[idx]
            y = list_top + (idx - hub_scroll) * line_h
            if idx == selected_hub_idx:
                pygame.draw.rect(screen, (35, 35, 35), (hub_left - 4, y - 2, hub_w, line_h))
            color = (0, 200, 255) if idx == selected_hub_idx else (220, 220, 220)
            text = small_font.render(f"hub_{hub_idx}", True, color)
            screen.blit(text, (hub_left, y))

        for idx in range(env_scroll, min(len(current_envs), env_scroll + visible_envs)):
            entry = current_envs[idx]
            y = list_top + (idx - env_scroll) * line_h
            if idx == selected_env_idx:
                pygame.draw.rect(screen, (35, 35, 35), (env_left - 4, y - 2, env_w, line_h))
            color = (0, 220, 160) if idx == selected_env_idx else (220, 220, 220)
            env_rate = entry["env_rate"]
            if env_rate is None:
                env_label = entry["env_dir"].name
            else:
                env_label = f"env {env_rate:.2f}"
            text = small_font.render(
                f"{env_label} | masters: {entry['master_count']}",
                True,
                color,
            )
            screen.blit(text, (env_left, y))

        pygame.draw.rect(screen, (30, 30, 30), (detail_x, list_top, detail_w, list_bottom - list_top))
        pygame.draw.rect(screen, (80, 80, 80), (detail_x, list_top, detail_w, list_bottom - list_top), 1)
        env_rate = selected_entry["env_rate"]
        detail_lines = [
            f"hub_{selected_entry['hub_idx']}",
            f"env: {selected_entry['env_dir'].name if env_rate is None else f'{env_rate:.2f}'}",
            f"masters: {selected_entry['master_count']}",
            f"path: {selected_entry['env_dir']}",
        ]
        y = list_top + 10
        for line in detail_lines:
            line_surf = small_font.render(line, True, (220, 220, 220))
            screen.blit(line_surf, (detail_x + 8, y))
            y += line_h

        pygame.draw.rect(screen, (40, 40, 40), choose_rect)
        pygame.draw.rect(screen, (180, 180, 180), choose_rect, 1)
        choose_text = small_font.render("Choose Env", True, (230, 230, 230))
        screen.blit(
            choose_text,
            (
                choose_rect.x + (choose_rect.width - choose_text.get_width()) // 2,
                choose_rect.y + 6,
            ),
        )

        if len(hubs) > 1:
            hint_text = "Left/Right hub  Up/Down env  Enter choose  Esc cancel"
        else:
            hint_text = "Up/Down env  Enter choose  Esc cancel"
        hint = small_font.render(hint_text, True, (180, 180, 180))
        screen.blit(hint, (20, screen_h - 45))
        pygame.display.flip()
        clock.tick(30)


def _select_master_run_ui(
    results_dir: Path,
    title_text: str = "Select Master Run",
    include_nested: bool = False,
):
    masters = []
    if include_nested:
        candidates = sorted(results_dir.rglob("master_*"))
    else:
        candidates = sorted(results_dir.glob("master_*"))
    for path in candidates:
        if not path.is_dir():
            continue
        try:
            run_num = int(path.name.split("_", 1)[1])
        except Exception:
            continue
        masters.append((run_num, path))
    if not masters:
        return None, None, None
    masters.sort(key=lambda item: (item[0], str(item[1])))

    screen_w = 800
    screen_h = 520
    screen = pygame.display.set_mode((screen_w, screen_h))
    pygame.display.set_caption("Select Master Run")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 18)
    clock = pygame.time.Clock()

    selected = 0
    scroll = 0
    editing_message = False
    message_text = ""
    message_value = ""
    confirm_delete = False
    delete_target = None
    list_top = 70
    list_bottom = screen_h - 80
    list_left = 20
    list_width = 320
    line_h = small_font.get_height() + 6
    visible_count = max(1, (list_bottom - list_top) // line_h)
    detail_x = list_left + list_width + 20
    detail_y = list_top
    detail_w = screen_w - detail_x - 20
    detail_h = list_bottom - list_top
    message_line_idx = 4
    message_rect = pygame.Rect(
        detail_x + 8,
        detail_y + 10 + line_h * message_line_idx,
        detail_w - 16,
        line_h,
    )
    choose_rect = pygame.Rect(0, 0, 0, 0)
    delete_rect = pygame.Rect(0, 0, 0, 0)
    delete_yes_rect = pygame.Rect(0, 0, 0, 0)
    delete_no_rect = pygame.Rect(0, 0, 0, 0)
    exit_rect = pygame.Rect(screen_w - 90, 16, 70, 26)

    def _ensure_visible():
        nonlocal scroll
        if selected < scroll:
            scroll = selected
        elif selected >= scroll + visible_count:
            scroll = selected - visible_count + 1
        scroll = max(0, min(scroll, max(0, len(masters) - visible_count)))

    _ensure_visible()

    while True:
        detail_line_count = 5
        preview_rect = pygame.Rect(
            detail_x + 10,
            detail_y + 10 + line_h * detail_line_count + 10,
            detail_w - 20,
            180,
        )
        btn_y = preview_rect.bottom + 12
        choose_rect = pygame.Rect(detail_x + 10, btn_y, 120, 28)
        delete_rect = pygame.Rect(choose_rect.right + 10, btn_y, 120, 28)
        delete_yes_rect = pygame.Rect(detail_x + 10, btn_y, 70, 28)
        delete_no_rect = pygame.Rect(delete_yes_rect.right + 10, btn_y, 70, 28)

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return None, None, None
            if event.type == pygame.KEYDOWN:
                if editing_message:
                    if event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        run_num, path = masters[selected]
                        message_value = message_text.strip() or "debug run"
                        _write_master_message(path, message_value)
                        editing_message = False
                    elif event.key == pygame.K_ESCAPE:
                        editing_message = False
                        message_text = message_value
                    elif event.key == pygame.K_BACKSPACE:
                        message_text = message_text[:-1]
                    else:
                        ch = event.unicode
                        if ch and ch.isprintable() and len(message_text) < 80:
                            message_text += ch
                else:
                    if event.key == pygame.K_ESCAPE:
                        return None, None, None
                    if event.key == pygame.K_UP:
                        selected = (selected - 1) % len(masters)
                        editing_message = False
                        _ensure_visible()
                    elif event.key == pygame.K_DOWN:
                        selected = (selected + 1) % len(masters)
                        editing_message = False
                        _ensure_visible()
                    elif event.key == pygame.K_m:
                        run_num, path = masters[selected]
                        message_value = _read_master_message(path)
                        message_text = message_value
                        editing_message = True
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
                        return run_num, settings_snapshot, path
                    elif event.key == pygame.K_DELETE:
                        confirm_delete = True
                        delete_target = masters[selected]
            if event.type == pygame.MOUSEWHEEL:
                if event.y > 0:
                    selected = (selected - 1) % len(masters)
                elif event.y < 0:
                    selected = (selected + 1) % len(masters)
                editing_message = False
                _ensure_visible()
            if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if exit_rect.collidepoint(mx, my):
                    return None, None, None
                if confirm_delete:
                    if delete_yes_rect.collidepoint(mx, my):
                        if delete_target is None:
                            delete_target = masters[selected]
                        run_num, path = delete_target
                        try:
                            shutil.rmtree(path)
                        except Exception:
                            pass
                        masters = [m for m in masters if m[1] != path]
                        if not masters:
                            return None, None, None
                        selected = max(0, min(selected, len(masters) - 1))
                        confirm_delete = False
                        delete_target = None
                        editing_message = False
                        _ensure_visible()
                        continue
                    if delete_no_rect.collidepoint(mx, my):
                        confirm_delete = False
                        delete_target = None
                        continue
                if message_rect.collidepoint(mx, my):
                    run_num, path = masters[selected]
                    message_value = _read_master_message(path)
                    message_text = message_value
                    editing_message = True
                    continue
                if list_top <= my <= list_bottom:
                    idx = (my - list_top) // line_h + scroll
                    if 0 <= idx < len(masters):
                        selected = int(idx)
                        editing_message = False
                        _ensure_visible()
                if choose_rect.collidepoint(mx, my):
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
                    return run_num, settings_snapshot, path
                if delete_rect.collidepoint(mx, my):
                    confirm_delete = True
                    delete_target = masters[selected]

        screen.fill((18, 18, 18))
        title = font.render(title_text, True, (230, 230, 230))
        screen.blit(title, (20, 20))
        pygame.draw.rect(screen, (60, 60, 60), exit_rect)
        pygame.draw.rect(screen, (160, 160, 160), exit_rect, 1)
        exit_text = small_font.render("Exit", True, (230, 230, 230))
        screen.blit(
            exit_text,
            (
                exit_rect.x + (exit_rect.width - exit_text.get_width()) // 2,
                exit_rect.y + 4,
            ),
        )
        hint_text = "Enter: select  Esc: cancel  M: edit message  Del: delete"
        if editing_message:
            hint_text = "Editing message: Enter=save  Esc=cancel"
        hint = small_font.render(hint_text, True, (180, 180, 180))
        screen.blit(hint, (20, screen_h - 50))

        for idx in range(scroll, min(len(masters), scroll + visible_count)):
            run_num, path = masters[idx]
            y = list_top + (idx - scroll) * line_h
            label = f"master_{run_num}"
            if include_nested:
                try:
                    rel_parent = path.parent.relative_to(results_dir)
                    if str(rel_parent) != ".":
                        label = f"{label} | {rel_parent}"
                except Exception:
                    label = f"{label} | {path.parent}"
            color = (0, 200, 255) if idx == selected else (220, 220, 220)
            if idx == selected:
                pygame.draw.rect(
                    screen, (35, 35, 35), (list_left - 4, y - 2, list_width, line_h)
                )
            text = small_font.render(label, True, color)
            screen.blit(text, (list_left, y))

        # Detail panel for selected master
        sel_run, sel_path = masters[selected]
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
            meta_path = sel_path.parent / str(run_num) / "run_meta.json"
            run_meta = _load_run_meta(meta_path)
            if isinstance(run_meta, dict):
                elapsed = run_meta.get("elapsed_seconds")
                species = run_meta.get("amnt_of_species")
                if isinstance(elapsed, (int, float)):
                    elapsed_vals.append(float(elapsed))
                if isinstance(species, (int, float)):
                    species_vals.append(float(species))

        max_elapsed = max(elapsed_vals) if elapsed_vals else 0.0
        _maybe_autoupdate_master_message(sel_path, max_elapsed)
        if not editing_message:
            message_value = _read_master_message(sel_path)
        message_display = message_text if editing_message else message_value

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
        _draw_text_with_caret(
            screen,
            small_font,
            f"Message: {message_display}",
            (detail_x + 10, text_y),
            (255, 220, 160) if editing_message else (220, 220, 220),
            show_caret=editing_message,
            max_caret_x=detail_x + detail_w - 12,
        )

        # Arithmetic mean preview
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

        if confirm_delete:
            pygame.draw.rect(screen, (80, 40, 40), delete_yes_rect)
            pygame.draw.rect(screen, (200, 200, 200), delete_yes_rect, 1)
            yes_text = small_font.render("Yes", True, (230, 230, 230))
            screen.blit(
                yes_text,
                (
                    delete_yes_rect.x + (delete_yes_rect.width - yes_text.get_width()) // 2,
                    delete_yes_rect.y + 6,
                ),
            )
            pygame.draw.rect(screen, (60, 60, 60), delete_no_rect)
            pygame.draw.rect(screen, (200, 200, 200), delete_no_rect, 1)
            no_text = small_font.render("No", True, (230, 230, 230))
            screen.blit(
                no_text,
                (
                    delete_no_rect.x + (delete_no_rect.width - no_text.get_width()) // 2,
                    delete_no_rect.y + 6,
                ),
            )
            prompt = small_font.render("Delete selected master?", True, (230, 200, 200))
            screen.blit(prompt, (detail_x + 10, delete_yes_rect.y - line_h))
        else:
            pygame.draw.rect(screen, (40, 40, 40), choose_rect)
            pygame.draw.rect(screen, (180, 180, 180), choose_rect, 1)
            choose_text = small_font.render("Choose", True, (230, 230, 230))
            screen.blit(
                choose_text,
                (
                    choose_rect.x + (choose_rect.width - choose_text.get_width()) // 2,
                    choose_rect.y + 6,
                ),
            )
            pygame.draw.rect(screen, (70, 40, 40), delete_rect)
            pygame.draw.rect(screen, (180, 180, 180), delete_rect, 1)
            del_text = small_font.render("Delete", True, (230, 230, 230))
            screen.blit(
                del_text,
                (
                    delete_rect.x + (delete_rect.width - del_text.get_width()) // 2,
                    delete_rect.y + 6,
                ),
            )

        pygame.display.flip()
        clock.tick(30)


def _edit_startup_ui(settings: dict, results_dir: Path):
    screen_w = 700
    screen_h = 520
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
    editing_message = False
    edit_text = ""
    message_text = ""
    error_text = ""
    continue_master_run = None
    continue_settings = None
    continue_master_dir = None
    continue_results_dir = results_dir
    message_for_new = "debug run"
    message_value = message_for_new

    confirm_rect = pygame.Rect(screen_w - 160, screen_h - 60, 140, 36)
    select_rect = pygame.Rect(screen_w - 160, 220, 140, 32)
    upload_rect = pygame.Rect(screen_w - 320, screen_h - 60, 140, 36)
    exit_rect = pygame.Rect(screen_w - 90, 16, 70, 26)
    upload_notice = ""
    upload_notice_time = 0.0

    def _infer_continue_from_num_master():
        try:
            target_run = max(0, int(num_master))
        except Exception:
            return (None, None, None, None)
        run_num, master_dir, master_results_dir = _resolve_continue_master(
            results_dir,
            target_run,
            None,
        )
        if run_num is None or master_dir is None or int(run_num) != int(target_run):
            return (None, None, None, None)
        settings_snapshot = None
        settings_path = _master_settings_path(master_dir)
        if settings_path.exists():
            try:
                settings_snapshot = json.loads(settings_path.read_text())
            except Exception:
                settings_snapshot = None
        if not isinstance(settings_snapshot, dict):
            meta = _load_master_meta(master_dir)
            settings_snapshot = (
                meta.get("settings") if isinstance(meta, dict) else None
            )
        return (
            int(run_num),
            settings_snapshot,
            master_dir,
            master_results_dir,
        )

    def _apply_edit():
        nonlocal num_tries, num_master, editing, edit_text, error_text
        nonlocal continue_master_run, continue_settings, message_value, message_for_new
        nonlocal continue_master_dir, continue_results_dir
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
            continue_master_run = None
            continue_settings = None
            continue_master_dir = None
            continue_results_dir = results_dir
            message_value = message_for_new
        editing = False
        edit_text = ""
        error_text = ""

    while True:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return None
            if event.type == pygame.KEYDOWN:
                if editing_message:
                    if event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        message_value = message_text.strip() or "debug run"
                        if continue_master_run is None:
                            (
                                inferred_run,
                                _inferred_settings,
                                inferred_dir,
                                _inferred_results_dir,
                            ) = _infer_continue_from_num_master()
                            if inferred_run is not None and inferred_dir is not None:
                                _write_master_message(
                                    inferred_dir,
                                    message_value,
                                )
                            else:
                                message_for_new = message_value
                        else:
                            target_master_dir = continue_master_dir
                            if target_master_dir is None:
                                target_master_dir = results_dir / f"master_{continue_master_run}"
                            _write_master_message(
                                target_master_dir,
                                message_value,
                            )
                        editing_message = False
                    elif event.key == pygame.K_ESCAPE:
                        editing_message = False
                        message_text = message_value
                    elif event.key == pygame.K_BACKSPACE:
                        message_text = message_text[:-1]
                    else:
                        ch = event.unicode
                        if ch and ch.isprintable() and len(message_text) < 80:
                            message_text += ch
                elif editing:
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
                    if event.key == pygame.K_m:
                        selected = 3
                        editing = False
                        edit_text = ""
                        error_text = ""
                        editing_message = True
                        message_text = message_value
                        continue
                    if event.key == pygame.K_UP:
                        selected = (selected - 1) % 4
                    elif event.key == pygame.K_DOWN:
                        selected = (selected + 1) % 4
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
                            continue_master_dir = None
                            continue_results_dir = results_dir
                            message_value = message_for_new
                    elif event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        if selected in (1, 2):
                            editing = True
                            edit_text = str(num_tries if selected == 1 else num_master)
                            error_text = ""
                        elif selected == 3:
                            editing = False
                            edit_text = ""
                            error_text = ""
                            editing_message = True
                            message_text = message_value
            if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if exit_rect.collidepoint(mx, my):
                    return None
                if confirm_rect.collidepoint(mx, my):
                    effective_continue_run = continue_master_run
                    effective_continue_settings = continue_settings
                    effective_continue_dir = continue_master_dir
                    effective_continue_results_dir = continue_results_dir
                    if effective_continue_run is None:
                        (
                            inferred_run,
                            inferred_settings,
                            inferred_dir,
                            inferred_results_dir,
                        ) = _infer_continue_from_num_master()
                        if inferred_run is not None and inferred_dir is not None:
                            effective_continue_run = inferred_run
                            effective_continue_settings = inferred_settings
                            effective_continue_dir = inferred_dir
                            effective_continue_results_dir = inferred_results_dir
                    settings["draw"] = draw_value
                    settings["num_tries"] = num_tries
                    settings["num_tries_master"] = num_master
                    save_settings(settings)
                    return (
                        settings,
                        effective_continue_run,
                        effective_continue_settings,
                        message_for_new,
                        effective_continue_dir,
                        effective_continue_results_dir,
                    )
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
                    pick_root = _master_search_root(results_dir)
                    picked_run, picked_settings, picked_path = _select_master_run_ui(
                        pick_root,
                        title_text="Select Master",
                        include_nested=True,
                    )
                    if picked_run is not None:
                        try:
                            num_master = int(picked_run)
                        except Exception:
                            pass
                        continue_master_run = picked_run
                        continue_settings = picked_settings
                        continue_master_dir = picked_path
                        continue_results_dir = (
                            picked_path.parent if picked_path is not None else results_dir
                        )
                        if continue_master_dir is None:
                            continue_master_dir = (
                                continue_results_dir / f"master_{continue_master_run}"
                            )
                        message_value = _read_master_message(continue_master_dir)
                        message_text = message_value
                        editing_message = False
                line_start = 120
                line_gap = 50
                for idx in range(4):
                    rect = pygame.Rect(16, line_start + idx * line_gap - 4, screen_w - 32, 32)
                    if rect.collidepoint(mx, my):
                        selected = idx
                        if idx == 3:
                            editing = False
                            edit_text = ""
                            error_text = ""
                            editing_message = True
                            message_text = message_value
                        break

        screen.fill((18, 18, 18))
        title = font.render("Startup Options", True, (230, 230, 230))
        screen.blit(title, (20, 20))
        pygame.draw.rect(screen, (60, 60, 60), exit_rect)
        pygame.draw.rect(screen, (160, 160, 160), exit_rect, 1)
        exit_text = small_font.render("Exit", True, (230, 230, 230))
        screen.blit(
            exit_text,
            (
                exit_rect.x + (exit_rect.width - exit_text.get_width()) // 2,
                exit_rect.y + 4,
            ),
        )

        draw_label = f"Draw: {'ON' if draw_value else 'OFF'}"
        try:
            sim_count = int(settings.get("simulations", {}).get("count", 3))
        except Exception:
            sim_count = 3
        sim_count = max(1, sim_count)
        range_start = num_tries
        range_end = num_tries + sim_count - 1
        num_label = f"SIM run ids (num_tries) next {range_start}-{range_end}: {num_tries}"
        master_label = f"MASTER run id (num_tries_master): {num_master}"
        display_continue_run = continue_master_run
        display_continue_dir = continue_master_dir
        display_continue_results_dir = continue_results_dir
        display_continue_auto = False
        display_message_value = message_value
        if display_continue_run is None:
            (
                inferred_run,
                _inferred_settings,
                inferred_dir,
                inferred_results_dir,
            ) = _infer_continue_from_num_master()
            if inferred_run is not None and inferred_dir is not None:
                display_continue_run = inferred_run
                display_continue_dir = inferred_dir
                display_continue_results_dir = inferred_results_dir
                display_continue_auto = True
                display_message_value = _read_master_message(inferred_dir)
        if display_continue_run is None:
            master_label = f"{master_label} (new)"
        if display_continue_run is None:
            message_label = f"message (new): {message_value}"
        else:
            message_label = (
                f"message (master_{display_continue_run}): {display_message_value}"
            )

        for idx, line in enumerate([draw_label, num_label, master_label, message_label]):
            y = 120 + idx * 50
            color = (0, 200, 255) if idx == selected else (220, 220, 220)
            if idx == selected:
                pygame.draw.rect(screen, (35, 35, 35), (16, y - 4, screen_w - 32, 32))
            text = small_font.render(line, True, color)
            screen.blit(text, (22, y))

        pygame.draw.rect(screen, (60, 60, 60), select_rect)
        pygame.draw.rect(screen, (160, 160, 160), select_rect, 1)
        select_text = small_font.render("Select Master", True, (230, 230, 230))
        screen.blit(
            select_text,
            (
                select_rect.x + (select_rect.width - select_text.get_width()) // 2,
                select_rect.y + 6,
            ),
        )

        if display_continue_run is not None:
            cont_loc = "results"
            if display_continue_results_dir != results_dir:
                try:
                    cont_loc = str(display_continue_results_dir.relative_to(results_dir))
                except Exception:
                    cont_loc = str(display_continue_results_dir)
            mode_label = "Run mode: CONTINUE (auto)" if display_continue_auto else "Run mode: CONTINUE"
            cont_text = small_font.render(
                f"{mode_label} | master_{display_continue_run}",
                True,
                (180, 220, 180),
            )
            screen.blit(cont_text, (22, 350))
            loc_text = small_font.render(
                f"Path: {cont_loc}",
                True,
                (180, 220, 180),
            )
            screen.blit(loc_text, (22, 375))
        else:
            cont_text = small_font.render(
                "Run mode: NEW master", True, (180, 180, 220)
            )
            screen.blit(cont_text, (22, 350))
            loc_text = small_font.render(
                "Use Select Master to continue an existing master.", True, (180, 180, 220)
            )
            screen.blit(loc_text, (22, 375))

        hint_text = "Up/Down select  Left/Right adjust  Enter edit  Esc cancel  M edit message"
        if editing_message:
            hint_text = "Editing message: Enter=save  Esc=cancel"
        hint = small_font.render(hint_text, True, (180, 180, 180))
        screen.blit(hint, (20, screen_h - 115))

        if editing:
            edit_line = f"Edit: {edit_text}"
            edit_color = (255, 220, 160) if not error_text else (255, 160, 160)
            _draw_text_with_caret(
                screen,
                small_font,
                edit_line,
                (20, screen_h - 88),
                edit_color,
                show_caret=True,
                max_caret_x=screen_w - 12,
            )
        if editing_message:
            msg_line = f"Message: {message_text}"
            msg_color = (255, 220, 160)
            _draw_text_with_caret(
                screen,
                small_font,
                msg_line,
                (20, screen_h - 88),
                msg_color,
                show_caret=True,
                max_caret_x=screen_w - 12,
            )
        if error_text:
            err_surf = small_font.render(error_text, True, (255, 160, 160))
            screen.blit(err_surf, (20, screen_h - 62))

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


def _format_wall_time(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(float(ts), tz=_LA_TZ)
        return dt.strftime("%Y-%m-%d %I:%M:%S %p %Z")
    except Exception:
        return "--"


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
        f"start {since_start} | "
        f"since now {since_now} | 1KFrame {interval_label}"
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


def _apply_update(selected_row: int, update_tokens: list[int], force_all: bool = False) -> None:
    if not update_tokens:
        return
    if force_all or selected_row == 0:
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
    parser.add_argument(
        "--results-dir",
        type=str,
        default="results",
        help="Root results directory for this run",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Skip startup/settings UIs and run directly with current settings/args",
    )
    parser.add_argument(
        "--env-change-rate",
        type=float,
        default=None,
        help="Override enviormentChangeRate for this run",
    )
    parser.add_argument(
        "--species-stop",
        type=int,
        default=None,
        help="Enable species stop limit with this threshold",
    )
    parser.add_argument(
        "--master-run-num",
        type=int,
        default=None,
        help="Force a specific master run id (used by hub orchestration).",
    )
    parser.add_argument(
        "--continue-master-run",
        type=int,
        default=None,
        help="Directly continue an existing master run id.",
    )
    parser.add_argument(
        "--continue-master-dir",
        type=str,
        default=None,
        help="Path to the master_<id> directory to continue directly.",
    )
    return parser.parse_args()


def _path_in_use(path: Path) -> bool:
    try:
        return path.exists() or path.is_symlink()
    except Exception:
        return False


def _allocate_run_numbers(count: int, results_dir: Path | None = None) -> list[int]:
    if results_dir is None:
        results_dir = Path("results")
    results_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings()
    try:
        current = int(settings.get("num_tries", 0))
    except Exception:
        current = 0
    root_results = Path("results")
    run_nums = []
    candidate = max(0, current)
    while len(run_nums) < count:
        if (not _path_in_use(root_results / str(candidate))) and (
            not _path_in_use(results_dir / str(candidate))
        ):
            run_nums.append(candidate)
        candidate += 1
    settings["num_tries"] = candidate
    save_settings(settings)
    return run_nums


def _allocate_master_run_number(
    results_dir: Path, forced_num: int | None = None
) -> int:
    settings = load_settings()
    try:
        current = int(settings.get("num_tries_master", 0))
    except Exception:
        current = 0
    root_results = Path("results")
    if forced_num is not None:
        new_val = int(forced_num)
        if new_val < 0:
            raise ValueError("master run id must be >= 0")
        if _path_in_use(root_results / f"master_{new_val}") or _path_in_use(
            results_dir / f"master_{new_val}"
        ):
            raise FileExistsError(
                f"master_{new_val} already exists in results paths; refusing overlap"
            )
        settings["num_tries_master"] = max(int(current), int(new_val) + 1)
        save_settings(settings)
        return new_val

    new_val = max(0, current)
    while _path_in_use(root_results / f"master_{new_val}") or _path_in_use(
        results_dir / f"master_{new_val}"
    ):
        new_val += 1
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


def _load_arithmetic_points_from_path(path: Path) -> list[tuple[float, float]]:
    if not path.exists():
        return []
    points = []
    try:
        with open(path, newline="") as csvfile:
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


def _load_arithmetic_snapshots(run_dir: Path) -> list[dict]:
    snap_dir = run_dir / "snapshots"
    if not snap_dir.exists():
        return []
    snapshots = []
    for path in snap_dir.glob("arith_mean_*.csv"):
        frame = None
        mtime = None
        try:
            mtime = path.stat().st_mtime
        except Exception:
            mtime = None
        try:
            frame = int(path.stem.split("_")[-1])
        except Exception:
            frame = None
        points = _load_arithmetic_points_from_path(path)
        snapshots.append(
            {"frame": frame, "points": points, "path": path, "mtime": mtime}
        )
    snapshots.sort(key=lambda item: item.get("frame") or 0)
    return snapshots


def _compute_arithmetic_bounds(snapshots: list[dict]):
    xs = []
    ys = []
    for snap in snapshots:
        for x, y in snap.get("points", []):
            xs.append(x)
            ys.append(y)
    if not xs or not ys:
        return None
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    if max_x == min_x:
        max_x = min_x + 1.0
    if max_y == min_y:
        max_y = min_y + 1.0
    x_pad = (max_x - min_x) * 0.05
    y_pad = (max_y - min_y) * 0.05
    return (min_x - x_pad, max_x + x_pad, min_y - y_pad, max_y + y_pad)


def _draw_snapshot_arithmetic_chart(
    surface,
    font,
    rect: pygame.Rect,
    points: list[tuple[float, float]],
    bounds=None,
) -> None:
    pygame.draw.rect(surface, (80, 80, 80), rect, 1)
    if not points:
        msg = font.render("No mean data", True, (160, 160, 160))
        surface.blit(msg, (rect.x + 6, rect.y + 6))
        return

    if bounds is None:
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
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
    else:
        min_x, max_x, min_y, max_y = bounds

    plot_left = rect.x + 6
    plot_top = rect.y + 6
    plot_width = rect.width - 12
    plot_height = rect.height - 12

    def _scale_point(x, y):
        px = int(((x - min_x) / (max_x - min_x)) * plot_width)
        py = plot_height - int(((y - min_y) / (max_y - min_y)) * plot_height)
        return px, py

    overlay = pygame.Surface((plot_width, plot_height), pygame.SRCALPHA)
    color = (0, 200, 255, _DOT_ALPHA)
    for x, y in sorted(points, key=lambda p: p[0]):
        px, py = _scale_point(x, y)
        pygame.draw.circle(overlay, color, (px, py), _DOT_RADIUS)
    surface.blit(overlay, (plot_left, plot_top))


def _draw_snapshot_multi_chart(
    surface,
    font,
    rect: pygame.Rect,
    series: list[tuple[list[tuple[float, float]], tuple[int, int, int]]],
    bounds=None,
    selected_point=None,
    selected_label=None,
    label_font=None,
) -> None:
    pygame.draw.rect(surface, (80, 80, 80), rect, 1)
    all_points = []
    for points, _ in series:
        all_points.extend(points)
    if not all_points:
        msg = font.render("No mean data", True, (160, 160, 160))
        surface.blit(msg, (rect.x + 6, rect.y + 6))
        return

    if bounds is None:
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
    else:
        min_x, max_x, min_y, max_y = bounds

    plot_left = rect.x + 6
    plot_top = rect.y + 6
    plot_width = rect.width - 12
    plot_height = rect.height - 12

    def _scale_point(x, y):
        px = int(((x - min_x) / (max_x - min_x)) * plot_width)
        py = plot_height - int(((y - min_y) / (max_y - min_y)) * plot_height)
        return px, py

    overlay = pygame.Surface((plot_width, plot_height), pygame.SRCALPHA)
    for points, base_color in series:
        if not points:
            continue
        if len(base_color) >= 4:
            color = base_color
        else:
            color = (base_color[0], base_color[1], base_color[2], _DOT_ALPHA)
        for x, y in sorted(points, key=lambda p: p[0]):
            px, py = _scale_point(x, y)
            pygame.draw.circle(overlay, color, (px, py), _DOT_RADIUS)
    surface.blit(overlay, (plot_left, plot_top))
    if selected_point:
        x = selected_point.get("x")
        y = selected_point.get("y")
        if isinstance(x, (int, float)) and isinstance(y, (int, float)):
            px, py = _scale_point(float(x), float(y))
            px += plot_left
            py += plot_top
            pygame.draw.circle(surface, (255, 255, 255), (px, py), 4, 1)
            if selected_label:
                use_font = label_font if label_font is not None else font
                _draw_value_label(surface, use_font, rect, selected_label, (px + 6, py - 6))



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


def _compute_species_counts(run_dir: Path) -> dict:
    raw_dir = run_dir / "raw_data"
    if not raw_dir.exists():
        return {}
    csv_files = sorted(raw_dir.glob("simulation_log_*.csv"))
    if not csv_files:
        return {}
    total = 0
    medium = 0
    big = 0
    for path in csv_files:
        try:
            with open(path, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if not row:
                        continue
                    try:
                        length_lived = float(row.get("length lived", ""))
                    except Exception:
                        continue
                    total += 1
                    if length_lived > 1999:
                        big += 1
                    elif length_lived > 500:
                        medium += 1
        except Exception:
            continue
    small = max(0, total - medium - big)
    return {
        "amnt_of_species": total,
        "amnt_of_small_species": small,
        "amnt_of_medium_species": medium,
        "amnt_of_big_species": big,
    }


def _get_snapshot_status(cache: dict, run_dir: Path, max_age: float = 1.0) -> dict:
    snap_dir = run_dir / "snapshots"
    now = time.time()
    if not snap_dir.exists():
        return {"count": 0, "last_frame": None, "last": now, "mtime": None}
    latest_mtime = None
    entry = cache.get(snap_dir)
    if entry and (now - entry.get("last", 0)) < max_age:
        return entry
    count = 0
    last_frame = None
    for path in snap_dir.glob("arith_mean_*.csv"):
        count += 1
        if latest_mtime is None:
            try:
                latest_mtime = path.stat().st_mtime
            except Exception:
                latest_mtime = None
        else:
            try:
                latest_mtime = max(latest_mtime, path.stat().st_mtime)
            except Exception:
                pass
        try:
            frame = int(path.stem.split("_")[-1])
        except Exception:
            frame = None
        if frame is not None:
            last_frame = frame if last_frame is None else max(last_frame, frame)
    result = {"count": count, "last_frame": last_frame, "last": now, "mtime": latest_mtime}
    cache[snap_dir] = result
    return result


def _load_run_meta(path: Path) -> dict:
    run_dir = path.parent
    meta = {}
    if path.exists():
        try:
            meta = json.loads(path.read_text())
            if not isinstance(meta, dict):
                meta = {}
        except Exception:
            meta = {}
    if not isinstance(meta, dict):
        meta = {}
    need_counts = False
    for key in ("amnt_of_species", "amnt_of_medium_species", "amnt_of_big_species"):
        if not isinstance(meta.get(key), (int, float)):
            need_counts = True
            break
    if need_counts:
        counts = _compute_species_counts(run_dir)
        if counts:
            meta.update(counts)
            try:
                path.write_text(json.dumps(meta))
            except Exception:
                pass
    if not path.exists():
        return meta
    try:
        payload = json.loads(path.read_text())
        if isinstance(payload, dict):
            try:
                payload["__mtime"] = path.stat().st_mtime
            except Exception:
                pass
        return payload if isinstance(payload, dict) else meta
    except Exception:
        return meta


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


def _view_arithmetic_snapshots(
    results_dir: Path,
    run_nums: list[int],
    initial_sim_index: int = 0,
    master_dir: Path | None = None,
) -> None:
    run_nums = [int(n) for n in run_nums] if run_nums else []
    if not run_nums:
        return
    run_dirs = {run_num: results_dir / str(run_num) for run_num in run_nums}
    run_index_map = {run_num: idx for idx, run_num in enumerate(run_nums)}
    screen_w = 900
    screen_h = 620
    screen = pygame.display.set_mode((screen_w, screen_h))
    pygame.display.set_caption("Arithmetic Timeline")
    font = pygame.font.SysFont("Consolas", 22)
    small_font = pygame.font.SysFont("Consolas", 16)
    clock = pygame.time.Clock()
    exit_top_rect = pygame.Rect(screen_w - 90, 16, 70, 26)
    view_mode = "sim"
    view_sim_index = max(0, min(int(initial_sim_index), len(run_nums) - 1))
    snapshots_by_run = {}
    run_frames_by_run = {}
    run_start_times = {}
    run_meta_by_run = {}
    frames = []
    bounds = None
    current_idx = 0
    dragging = False
    playing = False
    speed_min = 0.5
    speed_max = 32.0
    speed_multiplier = 1.0
    speed_auto = True
    play_accum = 0.0
    play_tick_fps = 30.0

    title_y = 20
    title_h = font.get_height()
    info_line_h = small_font.get_height()
    info_gap = 8
    info_line_gap = 4
    info_top = title_y + title_h + info_gap
    line1_y = info_top
    line2_y = info_top + info_line_h + info_line_gap
    line3_y = info_top + 2 * (info_line_h + info_line_gap)
    content_left = 40
    content_right = screen_w - 40
    mode_btn_w = 92
    mode_btn_h = 24
    mode_btn_gap = 8
    sim_btn_w = 80
    sim_btn_h = 22
    sim_btn_gap = 6
    mode_row_y = line3_y + info_line_h + 8
    mode_buttons = []
    mode_defs = [
        {"mode": "master", "label": "Master", "color": (200, 200, 200)},
        {"mode": "average", "label": "Average", "color": (255, 200, 80)},
        {"mode": "merge", "label": "Merge", "color": (180, 220, 255)},
    ]
    mode_x = content_left
    for entry in mode_defs:
        rect = pygame.Rect(mode_x, mode_row_y, mode_btn_w, mode_btn_h)
        entry = dict(entry)
        entry["rect"] = rect
        mode_buttons.append(entry)
        mode_x += mode_btn_w + mode_btn_gap

    sim_row_y = mode_row_y + mode_btn_h + 6

    def _layout_sim_buttons():
        buttons = []
        if not run_nums:
            return buttons, 0
        x = content_left
        y = sim_row_y
        row_h = sim_btn_h
        max_x = content_right
        for idx, run_num in enumerate(run_nums):
            if x + sim_btn_w > max_x:
                x = content_left
                y += row_h + sim_btn_gap
            rect = pygame.Rect(x, y, sim_btn_w, sim_btn_h)
            buttons.append(
                {
                    "rect": rect,
                    "index": idx,
                    "run_num": run_num,
                    "label": f"Sim {idx + 1}",
                    "color": _SIM_COLORS[idx % len(_SIM_COLORS)],
                }
            )
            x += sim_btn_w + sim_btn_gap
        total_h = (y - sim_row_y) + sim_btn_h
        return buttons, total_h

    sim_buttons, sim_area_h = _layout_sim_buttons()
    chart_top = sim_row_y + sim_area_h + 10
    bottom_reserved = 220
    chart_h = max(200, screen_h - bottom_reserved - chart_top)
    chart_rect = pygame.Rect(content_left, chart_top, screen_w - 80, chart_h)
    slider_rect = pygame.Rect(60, chart_rect.bottom + 40, screen_w - 120, 6)
    knob_radius = 8

    speed_slider_rect = pygame.Rect(60, slider_rect.bottom + 16, 240, 6)
    speed_knob_radius = 7
    btn_y = speed_slider_rect.bottom + 16
    btn_h = 26
    btn_gap = 10
    btn_w = 70
    base_x = 60
    back30_btn = pygame.Rect(base_x, btn_y, btn_w, btn_h)
    prev_btn = pygame.Rect(back30_btn.right + btn_gap, btn_y, btn_w, btn_h)
    next_btn = pygame.Rect(prev_btn.right + btn_gap, btn_y, btn_w, btn_h)
    fwd30_btn = pygame.Rect(next_btn.right + btn_gap, btn_y, btn_w, btn_h)
    play_btn = pygame.Rect(fwd30_btn.right + btn_gap, btn_y, 80, btn_h)
    refresh_btn = pygame.Rect(play_btn.right + btn_gap, btn_y, 80, btn_h)
    export_btn = pygame.Rect(refresh_btn.right + btn_gap, btn_y, 90, btn_h)
    export_all_btn = pygame.Rect(export_btn.right + btn_gap, btn_y, 110, btn_h)
    exit_btn = pygame.Rect(export_all_btn.right + btn_gap, btn_y, 80, btn_h)

    export_status = ""
    selected_timeline_dot = None

    def _draw_timeline_button(rect, label, accent_color, active: bool) -> None:
        fill = (70, 70, 70) if active else (40, 40, 40)
        border = accent_color if active else (120, 120, 120)
        pygame.draw.rect(screen, fill, rect)
        pygame.draw.rect(screen, border, rect, 1)
        pygame.draw.circle(screen, accent_color, (rect.x + 10, rect.centery), 4)
        text = small_font.render(label, True, (230, 230, 230))
        screen.blit(text, (rect.x + 18, rect.y + 4))


    def _reload(keep_end: bool = False) -> None:
        nonlocal snapshots_by_run, bounds, current_idx, speed_multiplier, speed_auto, frames
        nonlocal run_start_times, run_frames_by_run, run_meta_by_run
        current_frame = frames[current_idx] if frames else None
        snapshots_by_run = {}
        run_start_times = {}
        run_frames_by_run = {}
        run_meta_by_run = {}
        frames_set = set()
        all_snaps = []
        for run_num in run_nums:
            run_dir = run_dirs[run_num]
            snaps = _load_arithmetic_snapshots(run_dir)
            snap_map = {}
            run_frames = []
            for snap in snaps:
                frame_val = snap.get("frame")
                if isinstance(frame_val, int):
                    snap_map[frame_val] = snap
                    frames_set.add(frame_val)
                    run_frames.append(frame_val)
            snapshots_by_run[run_num] = snap_map
            run_frames.sort()
            run_frames_by_run[run_num] = run_frames
            all_snaps.extend(snaps)
            meta = _load_run_meta(run_dir / "run_meta.json")
            run_meta_by_run[run_num] = meta if isinstance(meta, dict) else {}
            start_time = None
            if isinstance(meta, dict):
                st = meta.get("start_time")
                if isinstance(st, (int, float)):
                    start_time = float(st)
                if start_time is None:
                    elapsed = meta.get("elapsed_seconds")
                    meta_mtime = meta.get("__mtime")
                    if isinstance(elapsed, (int, float)) and isinstance(meta_mtime, (int, float)):
                        start_time = float(meta_mtime) - float(elapsed)
            run_start_times[run_num] = start_time
        frames = sorted(frames_set)
        bounds = _compute_arithmetic_bounds(all_snaps)
        if not frames:
            current_idx = 0
        elif current_frame is not None and current_frame in frames:
            current_idx = frames.index(current_frame)
        elif keep_end:
            current_idx = len(frames) - 1
        else:
            current_idx = min(current_idx, max(0, len(frames) - 1))
        if speed_auto:
            speed_multiplier = 1.0

    def _snap_for_frame(run_num, frame_val):
        frame_list = run_frames_by_run.get(run_num, [])
        if not frame_list:
            return None
        if not isinstance(frame_val, int):
            return snapshots_by_run.get(run_num, {}).get(frame_list[-1])
        idx = bisect_right(frame_list, frame_val) - 1
        if idx < 0:
            return None
        key = frame_list[idx]
        return snapshots_by_run.get(run_num, {}).get(key)

    def _collect_view_snaps(frame_val, mode: str | None = None, sim_index: int | None = None):
        if mode is None:
            mode = view_mode
        if sim_index is None:
            sim_index = view_sim_index
        view_snaps = []
        if mode == "sim":
            run_num = run_nums[sim_index]
            snap = _snap_for_frame(run_num, frame_val)
            if snap:
                view_snaps.append((run_num, snap))
            return view_snaps
        for run_num in run_nums:
            snap = _snap_for_frame(run_num, frame_val)
            if snap:
                view_snaps.append((run_num, snap))
        return view_snaps

    def _labels_for_snaps(view_snaps):
        saved_label = "--"
        latest_mtime = None
        for _, snap in view_snaps:
            mtime = snap.get("mtime")
            if isinstance(mtime, (int, float)):
                latest_mtime = mtime if latest_mtime is None else max(latest_mtime, mtime)
        if latest_mtime is not None:
            saved_label = _format_wall_time(float(latest_mtime))
        since_start = "--:--:--"
        max_elapsed = None
        for run_num, snap in view_snaps:
            mtime = snap.get("mtime")
            start_time = run_start_times.get(run_num)
            if isinstance(mtime, (int, float)) and isinstance(start_time, (int, float)):
                elapsed = float(mtime) - float(start_time)
                max_elapsed = elapsed if max_elapsed is None else max(max_elapsed, elapsed)
        if max_elapsed is not None:
            since_start = _format_duration(max(0.0, max_elapsed))
        return saved_label, since_start

    def _species_label_for_view(
        view_snaps,
        mode: str | None = None,
        sim_index: int | None = None,
    ) -> str:
        if mode is None:
            mode = view_mode
        if sim_index is None:
            sim_index = view_sim_index

        if mode == "sim":
            if not view_snaps:
                return "Species: --"
            snap = view_snaps[0][1]
            points = snap.get("points", []) if isinstance(snap, dict) else []
            return f"Species: {len(points)}"

        counts = []
        for _, snap in view_snaps:
            points = snap.get("points", []) if isinstance(snap, dict) else []
            counts.append(len(points))
        if not counts:
            return "Species: --"
        total = sum(counts)
        mean_val = total / len(counts)
        return f"Species total: {total} | mean: {mean_val:.1f}"

    def _series_for_frame(frame_val, mode: str | None = None, sim_index: int | None = None):
        if mode is None:
            mode = view_mode
        if sim_index is None:
            sim_index = view_sim_index
        view_snaps = _collect_view_snaps(frame_val, mode, sim_index)
        series = []
        if mode == "sim":
            if view_snaps:
                run_num, snap = view_snaps[0]
                color = _SIM_COLORS[sim_index % len(_SIM_COLORS)]
                series.append((snap.get("points", []), color))
        elif mode == "master":
            for run_num, snap in view_snaps:
                idx = run_index_map.get(run_num, 0)
                color = _SIM_COLORS[idx % len(_SIM_COLORS)]
                series.append((snap.get("points", []), color))
        elif mode == "merge":
            merged = []
            for _, snap in view_snaps:
                merged.extend(snap.get("points", []))
            series.append((merged, (200, 200, 200)))
        elif mode == "average":
            buckets = {}
            for _, snap in view_snaps:
                for x, y in snap.get("points", []):
                    key = round(float(x), 3)
                    buckets.setdefault(key, []).append(float(y))
            avg_points = [(x, sum(vals) / len(vals)) for x, vals in buckets.items() if vals]
            avg_points.sort(key=lambda p: p[0])
            series.append((avg_points, (255, 200, 80)))
        return series, view_snaps

    def _series_meta(view_snaps, mode: str | None = None, sim_index: int | None = None):
        if mode is None:
            mode = view_mode
        if sim_index is None:
            sim_index = view_sim_index
        meta = []
        if mode == "sim":
            run_num = run_nums[sim_index] if 0 <= sim_index < len(run_nums) else None
            if view_snaps:
                run_num = view_snaps[0][0]
            meta.append({"run_num": run_num, "sim_index": sim_index})
        elif mode == "master":
            for run_num, _ in view_snaps:
                meta.append({"run_num": run_num, "sim_index": run_index_map.get(run_num)})
        else:
            meta.append({"run_num": None, "sim_index": None})
        return meta

    def _pick_timeline_dot(
        click_pos: tuple[int, int],
        rect: pygame.Rect,
        series: list[tuple[list[tuple[float, float]], tuple[int, int, int]]],
        view_snaps,
        mode: str | None = None,
        sim_index: int | None = None,
        max_distance: int = 8,
    ):
        all_points = []
        for points, _ in series:
            all_points.extend(points)
        if not all_points:
            return None
        if bounds is None:
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
        else:
            min_x, max_x, min_y, max_y = bounds
        plot_left = rect.x + 6
        plot_top = rect.y + 6
        plot_width = rect.width - 12
        plot_height = rect.height - 12

        def _scale_point(x, y):
            px = int(((x - min_x) / (max_x - min_x)) * plot_width)
            py = plot_height - int(((y - min_y) / (max_y - min_y)) * plot_height)
            return px + plot_left, py + plot_top

        meta = _series_meta(view_snaps, mode, sim_index)
        best = None
        best_d2 = max_distance * max_distance
        cx, cy = click_pos
        for s_idx, item in enumerate(series):
            points = item[0]
            if not points:
                continue
            point_meta = meta[s_idx] if s_idx < len(meta) else {"run_num": None, "sim_index": None}
            for x, y in points:
                px, py = _scale_point(x, y)
                dx = px - cx
                dy = py - cy
                d2 = dx * dx + dy * dy
                if d2 <= best_d2:
                    best_d2 = d2
                    best = {
                        "mode": mode if mode is not None else view_mode,
                        "run_num": point_meta.get("run_num"),
                        "sim_index": point_meta.get("sim_index"),
                        "target_x": float(x),
                        "x": float(x),
                        "y": float(y),
                    }
        return best

    def _resolve_tracked_dot(
        tracked_dot,
        series: list[tuple[list[tuple[float, float]], tuple[int, int, int]]],
        view_snaps,
        mode: str | None = None,
        sim_index: int | None = None,
    ):
        if not tracked_dot:
            return None
        target_x = tracked_dot.get("target_x")
        if not isinstance(target_x, (int, float)):
            return None
        target_x = float(target_x)
        tracked_run = tracked_dot.get("run_num")
        meta = _series_meta(view_snaps, mode, sim_index)
        best = None
        best_dx = None
        for s_idx, item in enumerate(series):
            points = item[0]
            if not points:
                continue
            point_meta = meta[s_idx] if s_idx < len(meta) else {"run_num": None, "sim_index": None}
            if tracked_run is not None and point_meta.get("run_num") != tracked_run:
                continue
            for x, y in points:
                dx = abs(float(x) - target_x)
                if best_dx is None or dx < best_dx:
                    best_dx = dx
                    best = {
                        "mode": mode if mode is not None else view_mode,
                        "run_num": point_meta.get("run_num"),
                        "sim_index": point_meta.get("sim_index"),
                        "target_x": target_x,
                        "x": float(x),
                        "y": float(y),
                        "x_error": float(dx),
                    }
        return best

    def _next_export_path(base_dir: Path, base_name: str) -> Path:
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

    def _export_mov_for(mode: str, sim_index: int | None = None):
        if not frames:
            return False, "no snapshots"
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            return False, "ffmpeg not found"
        export_base = master_dir if master_dir is not None else results_dir
        if mode == "sim":
            if sim_index is None:
                return False, "missing sim"
            run_num = run_nums[sim_index]
            export_root = export_base
            export_suffix = f"sim_{run_num}"
            base_name = f"timeline_sim_{run_num}.mov"
        else:
            export_root = export_base
            export_suffix = mode
            base_name = f"timeline_{mode}.mov"
        export_dir = export_root / f"timeline_export_{export_suffix}"
        export_dir.mkdir(parents=True, exist_ok=True)
        for old in export_dir.glob("frame_*.png"):
            try:
                old.unlink()
            except Exception:
                pass
        fps = 30
        label_h = info_line_h * 2 + 8
        export_w = chart_rect.width
        export_h = chart_rect.height + label_h
        chart_export_rect = pygame.Rect(0, label_h, export_w, chart_rect.height)
        for idx, frame_val in enumerate(frames):
            series, view_snaps = _series_for_frame(frame_val, mode, sim_index)
            surf = pygame.Surface((export_w, export_h))
            surf.fill((18, 18, 18))
            saved_label, _ = _labels_for_snaps(view_snaps)
            species_label = _species_label_for_view(view_snaps, mode, sim_index)
            label1 = small_font.render(
                f"Saved: {saved_label}   {species_label}",
                True,
                (220, 220, 220),
            )
            label2 = small_font.render(f"FPS: {fps}", True, (220, 220, 220))
            surf.blit(label1, (6, 2))
            surf.blit(label2, (6, 2 + info_line_h + 2))
            _draw_snapshot_multi_chart(
                surf,
                small_font,
                chart_export_rect,
                series,
                bounds=bounds,
            )
            pygame.image.save(surf, export_dir / f"frame_{idx:06d}.png")
        output_path = _next_export_path(export_root, base_name)
        cmd = [
            ffmpeg,
            "-y",
            "-framerate",
            str(fps),
            "-i",
            str(export_dir / "frame_%06d.png"),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(output_path),
        ]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                return False, "export failed"
            return True, output_path.name
        except Exception:
            return False, "export failed"

    def _export_mov() -> None:
        nonlocal export_status
        ok, msg = _export_mov_for(view_mode, view_sim_index if view_mode == "sim" else None)
        if ok:
            export_status = f"Export completed: {msg}"
        else:
            export_status = f"Export failed: {msg}"

    def _export_all() -> None:
        nonlocal export_status
        modes = ["master", "average", "merge"]
        failed = []
        for mode in modes:
            ok, msg = _export_mov_for(mode, None)
            if not ok:
                failed.append(f"{mode} ({msg})")
        if failed:
            export_status = "Export failed: " + ", ".join(failed)
        else:
            export_status = "Export completed: master/average/merge"

    def _set_index_from_mouse(mx: int):
        nonlocal current_idx
        if len(frames) <= 1:
            current_idx = 0
            return
        ratio = (mx - slider_rect.x) / max(1, slider_rect.width)
        ratio = max(0.0, min(1.0, ratio))
        current_idx = int(round(ratio * (len(frames) - 1)))

    def _set_speed_from_mouse(mx: int):
        nonlocal speed_multiplier, speed_auto
        ratio = (mx - speed_slider_rect.x) / max(1, speed_slider_rect.width)
        ratio = max(0.0, min(1.0, ratio))
        speed_multiplier = speed_min + ratio * (speed_max - speed_min)
        speed_auto = False

    def _auto_speed(snapshot_count: int) -> float:
        if snapshot_count <= 1:
            return speed_min
        duration = 5.0 + math.sqrt(snapshot_count / 100.0)
        steps = max(1, snapshot_count - 1)
        return steps / max(0.1, duration)

    if speed_auto:
        speed_multiplier = 1.0

    def _advance(step: int):
        nonlocal current_idx, playing
        if not frames:
            return
        current_idx = max(0, min(len(frames) - 1, current_idx + step))
        if current_idx >= len(frames) - 1:
            playing = False

    def _maybe_restart_for_short_tail() -> None:
        nonlocal current_idx
        if not frames or len(frames) <= 1:
            return
        remaining = (len(frames) - 1) - current_idx
        if remaining <= 0:
            current_idx = 0
            return
        speed = max(0.001, float(speed_multiplier) * _auto_speed(len(frames)))
        if (remaining / speed) <= 0.3:
            current_idx = 0

    _reload(keep_end=True)

    running = True
    speed_dragging = False
    while running:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                if event.key in (pygame.K_ESCAPE, pygame.K_q):
                    running = False
                elif event.key == pygame.K_SPACE:
                    if not playing:
                        _maybe_restart_for_short_tail()
                        playing = True
                    else:
                        playing = False
                elif event.key == pygame.K_LEFT:
                    _advance(-1)
                elif event.key == pygame.K_RIGHT:
                    _advance(1)
                elif event.key == pygame.K_PAGEUP:
                    _advance(-30)
                elif event.key == pygame.K_PAGEDOWN:
                    _advance(30)
                elif event.key == pygame.K_r:
                    _reload(keep_end=True)
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if exit_top_rect.collidepoint(mx, my):
                    running = False
                    continue
                handled = False
                for btn in mode_buttons:
                    if btn["rect"].collidepoint(mx, my):
                        view_mode = btn["mode"]
                        handled = True
                        export_status = ""
                        selected_timeline_dot = None
                        break
                if not handled:
                    for btn in sim_buttons:
                        if btn["rect"].collidepoint(mx, my):
                            view_mode = "sim"
                            view_sim_index = btn["index"]
                            handled = True
                            export_status = ""
                            selected_timeline_dot = None
                            break
                if handled:
                    continue
                if chart_rect.collidepoint(mx, my):
                    if frames:
                        frame_val = frames[current_idx]
                        series, view_snaps = _series_for_frame(frame_val)
                        selected_timeline_dot = _pick_timeline_dot(
                            (mx, my),
                            chart_rect,
                            series,
                            view_snaps,
                            view_mode,
                            view_sim_index,
                        )
                    else:
                        selected_timeline_dot = None
                    continue
                if slider_rect.collidepoint(mx, my):
                    dragging = True
                    _set_index_from_mouse(mx)
                elif speed_slider_rect.collidepoint(mx, my):
                    speed_dragging = True
                    _set_speed_from_mouse(mx)
                elif back30_btn.collidepoint(mx, my):
                    _advance(-30)
                elif prev_btn.collidepoint(mx, my):
                    _advance(-1)
                elif next_btn.collidepoint(mx, my):
                    _advance(1)
                elif fwd30_btn.collidepoint(mx, my):
                    _advance(30)
                elif play_btn.collidepoint(mx, my):
                    if not playing:
                        _maybe_restart_for_short_tail()
                        playing = True
                    else:
                        playing = False
                elif refresh_btn.collidepoint(mx, my):
                    _reload(keep_end=True)
                elif export_btn.collidepoint(mx, my):
                    _export_mov()
                elif export_all_btn.collidepoint(mx, my):
                    _export_all()
                elif exit_btn.collidepoint(mx, my):
                    running = False
            elif event.type == pygame.MOUSEBUTTONUP and event.button == 1:
                dragging = False
                speed_dragging = False
            elif event.type == pygame.MOUSEMOTION and dragging:
                mx, _ = event.pos
                _set_index_from_mouse(mx)
            elif event.type == pygame.MOUSEMOTION and speed_dragging:
                mx, _ = event.pos
                _set_speed_from_mouse(mx)

        if playing and frames:
            base_speed = max(0.001, _auto_speed(len(frames)))
            # Advance by render frame ticks (not elapsed wall time) for stable playback.
            play_accum += (base_speed * speed_multiplier) / max(1.0, play_tick_fps)
            while play_accum >= 1.0:
                play_accum -= 1.0
                _advance(1)
                if not playing:
                    break

        screen.fill((18, 18, 18))
        if view_mode == "sim":
            run_num = run_nums[view_sim_index]
            title_label = f"Arithmetic Timeline - Sim {view_sim_index + 1} (run {run_num})"
        elif view_mode == "master":
            title_label = "Arithmetic Timeline - Master"
        elif view_mode == "average":
            title_label = "Arithmetic Timeline - Average"
        else:
            title_label = "Arithmetic Timeline - Merge"
        title = font.render(title_label, True, (230, 230, 230))
        screen.blit(title, (40, title_y))
        pygame.draw.rect(screen, (60, 60, 60), exit_top_rect)
        pygame.draw.rect(screen, (160, 160, 160), exit_top_rect, 1)
        exit_text = small_font.render("Exit", True, (230, 230, 230))
        screen.blit(
            exit_text,
            (
                exit_top_rect.x + (exit_top_rect.width - exit_text.get_width()) // 2,
                exit_top_rect.y + 4,
            ),
        )

        if not frames:
            msg = small_font.render(
                "No snapshots yet (wait for 100000 frames).",
                True,
                (180, 180, 180),
            )
            screen.blit(msg, (40, line1_y))
        else:
            frame_val = frames[current_idx]
            frame_label = f"{frame_val}" if isinstance(frame_val, int) else "--"
            series, view_snaps = _series_for_frame(frame_val)
            tracked_dot = _resolve_tracked_dot(
                selected_timeline_dot,
                series,
                view_snaps,
                view_mode,
                view_sim_index,
            )
            saved_label, since_start = _labels_for_snaps(view_snaps)
            info_line1 = small_font.render(
                f"Frame: {frame_label}   Snapshot {current_idx + 1}/{len(frames)}",
                True,
                (200, 200, 200),
            )
            screen.blit(info_line1, (40, line1_y))
            info_saved = small_font.render(
                f"Saved: {saved_label}   {_species_label_for_view(view_snaps)}",
                True,
                (200, 200, 200),
            )
            screen.blit(info_saved, (40, line2_y))
            dot_text = "Dot: click a point to track"
            dot_label = None
            if tracked_dot is not None:
                dx = tracked_dot.get("x_error")
                x_val = tracked_dot.get("x")
                y_val = tracked_dot.get("y")
                run_num = tracked_dot.get("run_num")
                sim_idx = tracked_dot.get("sim_index")
                extra = []
                if isinstance(sim_idx, int):
                    extra.append(f"Sim {sim_idx + 1}")
                if isinstance(run_num, int):
                    extra.append(f"run {run_num}")
                if isinstance(x_val, (int, float)) and isinstance(y_val, (int, float)):
                    dot_text = f"Dot: evo {float(x_val):.3f} | mean {float(y_val):.2f}"
                    if extra:
                        dot_text += " | " + " ".join(extra)
                    if isinstance(dx, (int, float)) and dx > 1e-6:
                        dot_text += f" | dx {float(dx):.4f}"
                    dot_label = f"x:{float(x_val):.3f} y:{float(y_val):.2f}"
                    if extra:
                        dot_label += " | " + " ".join(extra)
            elif selected_timeline_dot is not None:
                target_x = selected_timeline_dot.get("target_x")
                if isinstance(target_x, (int, float)):
                    dot_text = f"Dot: evo {float(target_x):.3f} | no point this frame"
            info_dot = small_font.render(dot_text, True, (200, 200, 200))
            screen.blit(info_dot, (40, line3_y))
            since_x = chart_rect.right - 220
            if since_x < 40:
                since_x = 40
            info_since = small_font.render(
                f"Since start: {since_start}",
                True,
                (200, 200, 200),
            )
            screen.blit(info_since, (since_x, line3_y))
            _draw_snapshot_multi_chart(
                screen,
                small_font,
                chart_rect,
                series,
                bounds=bounds,
                selected_point=tracked_dot,
                selected_label=dot_label,
                label_font=small_font,
            )

        for btn in mode_buttons:
            active = view_mode == btn["mode"]
            _draw_timeline_button(btn["rect"], btn["label"], btn["color"], active)
        for btn in sim_buttons:
            active = view_mode == "sim" and view_sim_index == btn["index"]
            _draw_timeline_button(btn["rect"], btn["label"], btn["color"], active)

        pygame.draw.rect(screen, (90, 90, 90), slider_rect)
        if frames:
            if len(frames) == 1:
                knob_x = slider_rect.x
            else:
                ratio = current_idx / max(1, len(frames) - 1)
                knob_x = slider_rect.x + int(ratio * slider_rect.width)
            pygame.draw.circle(screen, (220, 220, 220), (knob_x, slider_rect.centery), knob_radius)

        pygame.draw.rect(screen, (90, 90, 90), speed_slider_rect)
        speed_ratio = (speed_multiplier - speed_min) / max(1e-6, (speed_max - speed_min))
        speed_knob_x = speed_slider_rect.x + int(speed_ratio * speed_slider_rect.width)
        pygame.draw.circle(
            screen,
            (220, 220, 220),
            (speed_knob_x, speed_slider_rect.centery),
            speed_knob_radius,
        )
        base_speed = max(0.001, _auto_speed(len(frames)))
        effective_speed = base_speed * speed_multiplier
        speed_label = small_font.render(
            f"Speed: {speed_multiplier:.1f}x (auto {base_speed:.1f}x -> {effective_speed:.1f}x)",
            True,
            (200, 200, 200),
        )
        screen.blit(speed_label, (speed_slider_rect.right + 10, speed_slider_rect.y - 8))

        for rect, label in [
            (back30_btn, "-30"),
            (prev_btn, "-1"),
            (next_btn, "+1"),
            (fwd30_btn, "+30"),
            (play_btn, "Play" if not playing else "Pause"),
            (refresh_btn, "Reload"),
            (export_btn, "Export"),
            (export_all_btn, "Export All"),
            (exit_btn, "Exit"),
        ]:
            pygame.draw.rect(screen, (40, 40, 40), rect)
            pygame.draw.rect(screen, (120, 120, 120), rect, 1)
            text = small_font.render(label, True, (220, 220, 220))
            screen.blit(text, (rect.x + (rect.width - text.get_width()) // 2, rect.y + 5))

        if export_status:
            status_surf = small_font.render(export_status, True, (180, 220, 180))
            screen.blit(status_surf, (40, screen_h - 54))

        hint = small_font.render(
            "Click chart dot to track. Space=play/pause, Left/Right = +/-1, PageUp/PageDown = +/-30, R = reload, Esc = exit",
            True,
            (160, 160, 160),
        )
        screen.blit(hint, (40, screen_h - 30))

        pygame.display.flip()
        clock.tick(int(play_tick_fps))


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
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    forced_continue_run = (
        int(args.continue_master_run)
        if args.continue_master_run is not None
        else None
    )
    forced_continue_dir = (
        Path(args.continue_master_dir)
        if args.continue_master_dir
        else None
    )
    settings = load_settings()
    if args.env_change_rate is not None:
        try:
            settings["enviormentChangeRate"] = float(args.env_change_rate)
        except Exception:
            pass
    if args.species_stop is not None:
        cond = settings.get("stop_conditions", {})
        if not isinstance(cond, dict):
            cond = {}
        species_limit = max(0, int(args.species_stop))
        cond["species_enabled"] = bool(species_limit > 0)
        cond["max_species"] = species_limit
        if species_limit > 0:
            cond["runtime_enabled"] = False
            cond["frames_enabled"] = False
            cond["datetime_enabled"] = False
        settings["stop_conditions"] = cond
    _apply_master_graph_settings(settings)
    pygame.init()
    if args.non_interactive:
        continue_master_run = forced_continue_run
        continue_settings = None
        startup_message = ""
        continue_master_dir = forced_continue_dir
        if continue_master_dir is not None:
            continue_results_dir = continue_master_dir.parent
        else:
            continue_results_dir = results_dir
    elif forced_continue_run is not None:
        continue_master_run = forced_continue_run
        continue_settings = None
        startup_message = ""
        continue_master_dir = forced_continue_dir
        if continue_master_dir is not None:
            continue_results_dir = continue_master_dir.parent
        else:
            continue_results_dir = results_dir
    else:
        startup = _edit_startup_ui(settings, results_dir)
        if startup is None:
            pygame.quit()
            return
        (
            settings,
            continue_master_run,
            continue_settings,
            startup_message,
            continue_master_dir,
            continue_results_dir,
        ) = startup
    if (continue_master_run is not None) or (continue_master_dir is not None):
        (
            continue_master_run,
            resolved_master_dir,
            resolved_results_dir,
        ) = _resolve_continue_master(
            results_dir,
            continue_master_run,
            continue_master_dir,
        )
        if continue_master_run is not None and resolved_master_dir is None:
            raise SystemExit(
                f"Could not find master_{int(continue_master_run)} to continue."
            )
        if resolved_master_dir is not None:
            continue_master_dir = resolved_master_dir
            continue_results_dir = resolved_results_dir
            if not isinstance(continue_settings, dict):
                settings_path = _master_settings_path(continue_master_dir)
                if settings_path.exists():
                    try:
                        continue_settings = json.loads(settings_path.read_text())
                    except Exception:
                        continue_settings = None
                if not isinstance(continue_settings, dict):
                    meta = _load_master_meta(continue_master_dir)
                    continue_settings = (
                        meta.get("settings") if isinstance(meta, dict) else None
                    )
    if continue_results_dir is not None:
        try:
            selected_results_dir = Path(continue_results_dir)
            selected_results_dir.mkdir(parents=True, exist_ok=True)
            results_dir = selected_results_dir
        except Exception:
            pass
    _apply_master_graph_settings(settings)
    if (not args.non_interactive) and continue_master_run is not None:
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
            _apply_master_graph_settings(settings)
    elif not args.non_interactive:
        settings = _edit_settings_ui(settings)
        if settings is None:
            pygame.quit()
            return
        _apply_master_graph_settings(settings)
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

    if continue_master_run is not None:
        master_run_num = int(continue_master_run)
        master_label = f"master_{master_run_num}"
        if continue_master_dir is None:
            raise SystemExit(f"Missing directory for continuation: {master_label}")
        master_dir = Path(continue_master_dir)
        if not master_dir.is_dir():
            raise SystemExit(f"Master directory not found: {master_dir}")
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
        run_nums = _allocate_run_numbers(count, results_dir)
        master_run_num = _allocate_master_run_number(
            results_dir, forced_num=args.master_run_num
        )
        master_label = f"master_{master_run_num}"
        master_dir = results_dir / master_label
        master_dir.mkdir(parents=True, exist_ok=True)
        master_run_nums = run_nums

    _save_master_meta(
        master_dir,
        master_run_nums,
        settings,
        update_global=((continue_master_run is None) and (not args.non_interactive)),
    )
    if continue_master_run is None and startup_message:
        _write_master_message(master_dir, startup_message)

    control_path = Path(tempfile.gettempdir()) / f"sim_master_active_{os.getpid()}.txt"
    selected_row = 0
    enabled = [True for _ in range(count)]
    # Startup default: draw every N frames (DRAW/500), not always draw.
    draw_modes = [1 for _ in range(count)]
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
    global_chart_gap    = 60
    master_line_offset  = 300
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
    env_base["SIM_RESULTS_DIR"] = str(results_dir)
    if args.env_change_rate is not None:
        env_base["SIM_ENV_CHANGE_RATE"] = str(float(args.env_change_rate))
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
    snapshot_cache = {}
    fps_series = [[] for _ in range(count)]
    mean_series = [[] for _ in range(count)]
    meta_series = [{} for _ in range(count)]
    last_chart_refresh = 0.0
    last_message_check = 0.0
    base_chart_refresh_s = 2.0
    base_master_fps = 1
    uncapped_fps = 240
    chart_refresh_s = base_chart_refresh_s
    master_fps = base_master_fps
    master_fps_mode = _FPS_MODE_CAPPED
    stop_max_runtime = 0.0
    stop_max_frames = 0
    stop_max_species = 0
    stop_at_ts = None
    mean_kind = "Arithmetic"
    selected_mean_point = None
    selected_fps_point = None
    full_throttle_active = False
    saved_draw_modes = None
    saved_mode_values = None
    pressed_button = None
    confirm_quit = False
    close_request_file = os.environ.get("MASTER_CLOSE_REQUEST_FILE", "").strip()
    close_request_path = Path(close_request_file) if close_request_file else None


    max_scroll = max(0, content_h - window_h)
    scroll_offset = 0.0
    scroll_target = 0.0
    scroll_step = max(30, panel_h // 2)
    scroll_overscroll = 180.0
    scroll_smoothness = 14.0
    scroll_snap_epsilon = 0.5
    last_scroll_anim_time = time.perf_counter()

    def _scroll_limits() -> tuple[float, float]:
        if max_scroll <= 0:
            return 0.0, 0.0
        return -scroll_overscroll, float(max_scroll) + scroll_overscroll

    def _clamp_scroll(value: float) -> float:
        lower, upper = _scroll_limits()
        return max(lower, min(upper, float(value)))

    running = True
    temp_uncap_until = 0.0
    temp_uncap_prev = None

    def _trigger_graph_update_all() -> None:
        nonlocal last_chart_refresh
        _apply_update(selected_row, update_tokens, force_all=True)
        _write_control(
            control_path,
            active_sim_index,
            enabled,
            draw_modes,
            draw_every,
            mode_values,
            update_tokens,
        )
        # Force immediate reload from disk so partial results show up without waiting.
        fps_cache.clear()
        arithmetic_cache.clear()
        snapshot_cache.clear()
        last_chart_refresh = 0.0

    def _refresh_stop_conditions() -> None:
        nonlocal stop_max_runtime, stop_max_frames, stop_max_species, stop_at_ts
        cond = settings.get("stop_conditions", {}) if isinstance(settings, dict) else {}
        try:
            runtime_enabled = bool(cond.get("runtime_enabled", False))
            hours = float(cond.get("max_runtime_hours", 0) or 0)
            stop_max_runtime = (hours * 3600.0) if runtime_enabled else 0.0
        except Exception:
            stop_max_runtime = 0.0
        try:
            frames_enabled = bool(cond.get("frames_enabled", False))
            stop_max_frames = int(cond.get("max_frames", 0) or 0) if frames_enabled else 0
        except Exception:
            stop_max_frames = 0
        try:
            species_enabled = bool(cond.get("species_enabled", False))
            stop_max_species = int(cond.get("max_species", 0) or 0) if species_enabled else 0
        except Exception:
            stop_max_species = 0
        if bool(cond.get("datetime_enabled", False)):
            stop_at_ts = _parse_stop_datetime(cond.get("stop_at_datetime", ""))
        else:
            stop_at_ts = None

    _refresh_stop_conditions()

    def _open_settings_dialog() -> None:
        nonlocal settings, screen, font, small_font, label_font
        nonlocal max_window_h, window_h, max_scroll, scroll_offset, scroll_target
        updated = _edit_settings_ui(settings, master_dir=master_dir, write_global_on_confirm=False)
        if updated is None:
            return
        settings = updated
        _refresh_stop_conditions()
        _apply_master_graph_settings(settings)
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
        scroll_offset = _clamp_scroll(scroll_offset)
        scroll_target = _clamp_scroll(scroll_target)

    def _open_timeline_viewer() -> None:
        nonlocal screen, font, small_font, label_font
        if not run_nums:
            return
        if selected_row == 0:
            idx = 0
        else:
            idx = selected_row - 1
            if idx < 0 or idx >= len(run_nums):
                return
        _view_arithmetic_snapshots(
            results_dir,
            run_nums,
            initial_sim_index=idx,
            master_dir=master_dir,
        )
        screen = pygame.display.set_mode((window_w, window_h))
        pygame.display.set_caption("Simulation Master")
        font = pygame.font.SysFont("Consolas", 22)
        small_font = pygame.font.SysFont("Consolas", 16)
        label_font = pygame.font.SysFont("Consolas", 14)

    def _open_stop_conditions_dialog() -> None:
        nonlocal settings, screen, font, small_font, label_font
        nonlocal max_window_h, window_h, max_scroll, scroll_offset, scroll_target
        updated = _edit_stop_conditions_ui(settings)
        if updated is not None:
            settings = updated
            _refresh_stop_conditions()
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
        scroll_offset = _clamp_scroll(scroll_offset)
        scroll_target = _clamp_scroll(scroll_target)

    
    def _apply_master_fps_mode(new_mode: int, transient: bool = False) -> None:
        nonlocal master_fps_mode, chart_refresh_s, master_fps
        nonlocal full_throttle_active, saved_draw_modes, saved_mode_values
        nonlocal temp_uncap_prev, temp_uncap_until
        if not transient:
            temp_uncap_prev = None
            temp_uncap_until = 0.0
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

    def _bump_uncap() -> None:
        nonlocal temp_uncap_until, temp_uncap_prev
        if master_fps_mode == _FPS_MODE_FULL_THROTTLE:
            return
        now = time.time()
        if master_fps_mode == _FPS_MODE_CAPPED:
            temp_uncap_prev = _FPS_MODE_CAPPED
            _apply_master_fps_mode(_FPS_MODE_UNCAPPED, transient=True)
        if temp_uncap_prev == _FPS_MODE_CAPPED:
            temp_uncap_until = now + 3.0

    def _set_scroll_target(value: float, immediate: bool = False) -> None:
        nonlocal scroll_target, scroll_offset
        clamped = _clamp_scroll(value)
        scroll_target = clamped
        if immediate:
            scroll_offset = clamped

    def _scroll_by(delta: float) -> None:
        _set_scroll_target(scroll_target + float(delta))

    def _ensure_selected_visible() -> None:
        nonlocal scroll_target, scroll_offset
        if max_scroll <= 0:
            scroll_target = 0.0
            scroll_offset = 0.0
            return
        if selected_row == 0:
            scroll_target = 0.0
            _bump_uncap()
            return
        if selected_row == count:
            scroll_target = float(max_scroll)
            _bump_uncap()
            return
        row_top = header_h + (selected_row - 1) * panel_h
        row_bottom = row_top + panel_h
        if row_top < scroll_target:
            scroll_target = float(row_top)
        if row_bottom > scroll_target + window_h:
            scroll_target = float(row_bottom - window_h)
        scroll_target = max(0.0, min(float(max_scroll), scroll_target))
        _bump_uncap()

    def _check_stop_conditions(now_time: float) -> bool:
        if stop_at_ts is not None and now_time >= stop_at_ts:
            return True
        now_perf = time.perf_counter()
        need_meta_refresh = (stop_max_frames > 0) or (stop_max_species > 0) or (stop_max_runtime > 0)
        combined_species = 0.0
        saw_species = False
        for idx in range(count):
            meta = meta_series[idx] if idx < len(meta_series) else {}
            if need_meta_refresh:
                try:
                    meta_path = results_dir / str(run_nums[idx]) / "run_meta.json"
                    meta = _get_cached_points(
                        arithmetic_cache,
                        meta_path,
                        _load_run_meta,
                        max_age=0.5,
                    )
                    if idx < len(meta_series):
                        meta_series[idx] = meta
                except Exception:
                    meta = meta if isinstance(meta, dict) else {}
            frame_val = None
            species_val = None
            elapsed_val = None
            if isinstance(meta, dict):
                if isinstance(meta.get("frame_count"), (int, float)):
                    frame_val = float(meta.get("frame_count"))
                if isinstance(meta.get("amnt_of_species"), (int, float)):
                    species_val = float(meta.get("amnt_of_species"))
                if isinstance(meta.get("elapsed_seconds"), (int, float)):
                    elapsed_val = float(meta.get("elapsed_seconds"))
            if elapsed_val is None and idx < len(sim_start_times):
                elapsed_val = max(0.0, now_perf - sim_start_times[idx])
            if stop_max_frames > 0 and frame_val is not None and frame_val >= stop_max_frames:
                return True
            if stop_max_species > 0 and species_val is not None:
                combined_species += float(species_val)
                saw_species = True
            if stop_max_runtime > 0 and elapsed_val is not None and elapsed_val >= stop_max_runtime:
                return True
        if stop_max_species > 0 and saw_species and combined_species >= stop_max_species:
            return True
        return False

    while running:
        if close_request_path is not None and close_request_path.exists():
            running = False
            confirm_quit = False
            try:
                close_request_path.unlink(missing_ok=True)
            except Exception:
                pass
            break
        now = time.time()
        render_scroll = int(round(scroll_offset))
        margin = 20
        gap = 20
        chart_w = (window_w - margin * 2 - gap) // 2
        master_line_y = header_top + master_line_offset
        global_chart_y = master_line_y + global_chart_gap
        exit_top_rect = pygame.Rect(window_w - 90, 16, 70, 26)
        fps_all_rect_content = pygame.Rect(
            margin, global_chart_y, chart_w, global_chart_h
        )
        mean_all_rect_content = pygame.Rect(
            margin + chart_w + gap, global_chart_y, chart_w, global_chart_h
        )
        if temp_uncap_prev == _FPS_MODE_CAPPED and master_fps_mode == _FPS_MODE_UNCAPPED:
            if time.time() > temp_uncap_until:
                _apply_master_fps_mode(_FPS_MODE_CAPPED, transient=True)
                temp_uncap_prev = None
                temp_uncap_until = 0.0
        y_offset = -render_scroll

        button_w = 110
        button_h = 26
        button_gap = 10
        buttons_per_row = 4
        button_start_x = margin
        button_start_y = header_top + 160
        button_order = [
            "draw",
            "mode",
            "update",
            "mean",
            "fps",
            "onoff",
            "timeline",
            "limits",
            "settings",
            "exit",
        ]
        button_rects = {}
        for idx, key in enumerate(button_order):
            row = idx // buttons_per_row
            col = idx % buttons_per_row
            bx = button_start_x + col * (button_w + button_gap)
            by = button_start_y + row * (button_h + button_gap)
            button_rects[key] = pygame.Rect(bx, by, button_w, button_h)
        draw_btn = button_rects["draw"]
        mode_btn = button_rects["mode"]
        update_btn = button_rects["update"]
        mean_btn = button_rects["mean"]
        fps_btn = button_rects["fps"]
        onoff_btn = button_rects["onoff"]
        timeline_btn = button_rects["timeline"]
        limits_btn = button_rects["limits"]
        settings_btn = button_rects["settings"]
        exit_btn = button_rects["exit"]
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
                        _scroll_by(-window_h)
                        _bump_uncap()
                elif event.key == pygame.K_PAGEDOWN:
                    if max_scroll > 0:
                        _scroll_by(window_h)
                        _bump_uncap()
                elif event.key == pygame.K_HOME:
                    _set_scroll_target(0.0)
                    _bump_uncap()
                elif event.key == pygame.K_END:
                    _set_scroll_target(float(max_scroll))
                    _bump_uncap()
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
                    _trigger_graph_update_all()
                elif event.key == pygame.K_u:
                    _trigger_graph_update_all()
                elif event.key == pygame.K_f:
                    _apply_master_fps_mode((master_fps_mode + 1) % 3)
                elif event.key == pygame.K_t:
                    _open_timeline_viewer()
                elif event.key == pygame.K_l:
                    _open_stop_conditions_dialog()
            elif event.type == pygame.MOUSEBUTTONUP and event.button == 1:
                _bump_uncap()
                if pressed_button is not None:
                    mx, my = event.pos
                    content_y = my + render_scroll
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
                    elif pressed_button == "update" and update_btn.collidepoint(mx, content_y):
                        _trigger_graph_update_all()
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
                    elif pressed_button == "timeline" and timeline_btn.collidepoint(mx, content_y):
                        _open_timeline_viewer()
                    elif pressed_button == "limits" and limits_btn.collidepoint(mx, content_y):
                        _open_stop_conditions_dialog()
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
                _bump_uncap()
                if max_scroll > 0:
                    _scroll_by(-event.y * scroll_step)
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                _bump_uncap()
                mx, my = event.pos
                if exit_top_rect.collidepoint(mx, my):
                    if confirm_quit:
                        running = False
                    else:
                        confirm_quit = True
                    continue
                content_y = my + render_scroll
                handled_click = False
                pressed_button = None
                if confirm_quit:
                    if confirm_layout["yes_rect"].collidepoint(mx, my):
                        running = False
                        confirm_quit = False
                        continue
                    if confirm_layout["no_rect"].collidepoint(mx, my):
                        confirm_quit = False
                        running = True
                        continue
                if draw_btn.collidepoint(mx, content_y):
                    pressed_button = "draw"
                    handled_click = True
                elif mode_btn.collidepoint(mx, content_y):
                    pressed_button = "mode"
                    handled_click = True
                elif update_btn.collidepoint(mx, content_y):
                    pressed_button = "update"
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
                elif timeline_btn.collidepoint(mx, content_y):
                    pressed_button = "timeline"
                    handled_click = True
                elif limits_btn.collidepoint(mx, content_y):
                    pressed_button = "limits"
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
                        _scroll_by(-scroll_step)
                    else:
                        _scroll_by(scroll_step)
                    _bump_uncap()

        scroll_target = _clamp_scroll(scroll_target)
        scroll_offset = _clamp_scroll(scroll_offset)
        anim_now = time.perf_counter()
        anim_dt = max(0.0, min(0.1, anim_now - last_scroll_anim_time))
        last_scroll_anim_time = anim_now
        if abs(scroll_target - scroll_offset) <= scroll_snap_epsilon:
            scroll_offset = scroll_target
        else:
            alpha = 1.0 - math.exp(-scroll_smoothness * anim_dt)
            if alpha <= 0.0:
                alpha = 0.01
            scroll_offset += (scroll_target - scroll_offset) * alpha
            _bump_uncap()
        render_scroll = int(round(scroll_offset))
        y_offset = -render_scroll
        confirm_layout = _confirm_quit_layout(window_w, header_top, y_offset, small_font)
        
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
                "D: draw  M: mode  S/U: update all  F: fps mode  T: timeline  L: limits  Esc/Q: quit",
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
            pygame.draw.rect(screen, (60, 60, 60), exit_top_rect)
            pygame.draw.rect(screen, (160, 160, 160), exit_top_rect, 1)
            exit_text = small_font.render("Exit", True, (230, 230, 230))
            screen.blit(
                exit_text,
                (
                    exit_top_rect.x + (exit_top_rect.width - exit_text.get_width()) // 2,
                    exit_top_rect.y + 4,
                ),
            )

            for rect, label in [
                (draw_btn, "Draw"),
                (mode_btn, "Mode"),
                (update_btn, "Update"),
                (mean_btn, f"Mean {mean_kind[:4]}"),
                (fps_btn, "FPS Mode"),
                (onoff_btn, "On/Off"),
                (timeline_btn, "Timeline"),
                (limits_btn, "Limits"),
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
            max_elapsed = max(elapsed_vals) if elapsed_vals else 0.0
            if master_dir is not None and (now_perf - last_message_check) >= 2.0:
                _maybe_autoupdate_master_message(master_dir, max_elapsed)
                last_message_check = now_perf
            
            mean_frames = (sum(frame_vals) / len(frame_vals)) if frame_vals else 0.0
            mean_species = (sum(species_vals) / len(species_vals)) if species_vals else 0.0
            mean_elapsed = (sum(elapsed_vals) / len(elapsed_vals)) if elapsed_vals else 0.0
            last_saved_ts = None
            for meta in meta_series:
                if isinstance(meta, dict):
                    ts = meta.get("__mtime")
                    if isinstance(ts, (int, float)):
                        last_saved_ts = ts if last_saved_ts is None else max(last_saved_ts, ts)
            last_saved_label = _format_wall_time(last_saved_ts) if last_saved_ts else "--"
            timeline_status = "Timeline: select a sim"
            if selected_row > 0 and 0 <= (selected_row - 1) < len(run_nums):
                sel_run = run_nums[selected_row - 1]
                status = _get_snapshot_status(snapshot_cache, results_dir / str(sel_run))
                snap_count = status.get("count", 0)
                last_frame = status.get("last_frame")
                if snap_count:
                    frame_label = f"{last_frame}" if last_frame is not None else "--"
                    timeline_status = f"Timeline: run {sel_run} | snaps {snap_count} | last {frame_label}"
                else:
                    timeline_status = f"Timeline: run {sel_run} | snaps 0"

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
            master_saved = small_font.render(
                f"Last saved: {last_saved_label}",
                True,
                master_color,
            )
            master_timeline = small_font.render(
                timeline_status,
                True,
                master_color,
            )
            screen.blit(master_line, (margin, master_line_y + y_offset))
            screen.blit(master_stats, (margin, master_line_y + 16 + y_offset))
            screen.blit(master_timeline, (margin, master_line_y + 32 + y_offset))
            screen.blit(
                master_saved,
                (margin + chart_w + gap, global_chart_y - 18 + y_offset),
            )
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
        if _check_stop_conditions(now):
            running = False
            confirm_quit = False
        clock.tick(master_fps)

        for proc in procs:
            if proc.poll() is not None:
                running = False
                break

    def _all_simulations_stopped() -> bool:
        for proc in procs:
            if proc.poll() is None:
                return False
        return True

    # Ask every simulation to exit itself first so it can write final run_meta/logs.
    try:
        _write_control(control_path, -1, enabled, draw_modes, draw_every, mode_values, update_tokens)
    except Exception:
        pass

    graceful_deadline = time.time() + 8.0
    while (not _all_simulations_stopped()) and time.time() < graceful_deadline:
        time.sleep(0.05)

    # If any process did not exit gracefully, terminate it.
    for proc in procs:
        if proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass

    terminate_deadline = time.time() + 4.0
    while (not _all_simulations_stopped()) and time.time() < terminate_deadline:
        time.sleep(0.05)

    # Hard-kill only the stubborn leftovers.
    for proc in procs:
        if proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass

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
