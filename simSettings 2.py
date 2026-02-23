#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from typing import Any

from settings_manager import DEFAULT_SETTINGS, load_settings, save_settings


def _path_label(tokens: tuple[Any, ...]) -> str:
    out = ""
    for token in tokens:
        if isinstance(token, int):
            out += f"[{token}]"
        else:
            if out:
                out += "."
            out += str(token)
    return out


def _flatten_leaves(node: Any, prefix: tuple[Any, ...] = ()) -> list[tuple[tuple[Any, ...], Any]]:
    if isinstance(node, dict):
        out: list[tuple[tuple[Any, ...], Any]] = []
        for key, value in node.items():
            out.extend(_flatten_leaves(value, prefix + (key,)))
        return out
    if isinstance(node, list):
        out = []
        for idx, value in enumerate(node):
            out.extend(_flatten_leaves(value, prefix + (idx,)))
        return out
    return [(prefix, node)]


def _get_by_path(root: Any, tokens: tuple[Any, ...]) -> Any:
    node = root
    for token in tokens:
        node = node[token]
    return node


def _set_by_path(root: Any, tokens: tuple[Any, ...], value: Any) -> None:
    node = root
    for token in tokens[:-1]:
        node = node[token]
    node[tokens[-1]] = value


def _parse_bool(raw: str) -> bool | None:
    value = raw.strip().lower()
    if value in ("1", "true", "t", "yes", "y", "on"):
        return True
    if value in ("0", "false", "f", "no", "n", "off"):
        return False
    return None


def _parse_like_current(current: Any, raw: str) -> tuple[bool, Any, str]:
    text = raw.strip()
    if text == "":
        return False, None, "empty input"
    if text.startswith("json:"):
        payload = text[5:].strip()
        try:
            return True, json.loads(payload), ""
        except Exception as exc:
            return False, None, f"invalid json payload: {exc}"
    if isinstance(current, bool):
        parsed = _parse_bool(text)
        if parsed is None:
            return False, None, "expected boolean (true/false)"
        return True, parsed, ""
    if isinstance(current, int) and not isinstance(current, bool):
        try:
            return True, int(text), ""
        except Exception:
            return False, None, "expected integer"
    if isinstance(current, float):
        try:
            return True, float(text), ""
        except Exception:
            return False, None, "expected number"
    if isinstance(current, str):
        return True, text, ""

    try:
        return True, json.loads(text), ""
    except Exception:
        return True, text, ""


def _print_header(dirty: bool) -> None:
    print()
    print("=" * 72)
    print("simSettings (terminal editor)")
    print(f"Unsaved changes: {'yes' if dirty else 'no'}")
    print("Commands: list | edit <index> | save | reload | reset | help | quit")
    print("Tip: for non-string values, type plain numbers/true/false.")
    print("Tip: use `json:<value>` to force JSON parsing (example: json:[1,2,3]).")
    print("=" * 72)


def _print_entries(entries: list[tuple[tuple[Any, ...], Any]], filter_text: str = "") -> None:
    flt = filter_text.strip().lower()
    shown = 0
    for idx, (tokens, value) in enumerate(entries, start=1):
        path = _path_label(tokens)
        if flt and flt not in path.lower():
            continue
        value_type = type(value).__name__
        print(f"{idx:>3}. {path:<40} = {value!r} ({value_type})")
        shown += 1
    if shown == 0:
        if flt:
            print(f"No settings matched filter: {filter_text!r}")
        else:
            print("No editable settings found.")


def _confirm(prompt: str) -> bool:
    answer = input(prompt).strip().lower()
    return answer in ("y", "yes")


def _handle_edit(settings: dict, entries: list[tuple[tuple[Any, ...], Any]], cmd: str) -> bool:
    parts = cmd.split()
    if len(parts) != 2:
        print("Usage: edit <index>")
        return False
    try:
        idx = int(parts[1])
    except Exception:
        print("Index must be an integer.")
        return False
    if idx < 1 or idx > len(entries):
        print(f"Index out of range (1..{len(entries)}).")
        return False

    tokens, current = entries[idx - 1]
    path = _path_label(tokens)
    print(f"Editing: {path}")
    print(f"Current value: {current!r} ({type(current).__name__})")
    new_raw = input("New value (blank cancels): ")
    if new_raw.strip() == "":
        print("Edit cancelled.")
        return False

    ok, parsed, err = _parse_like_current(current, new_raw)
    if not ok:
        print(f"Invalid value: {err}")
        return False
    _set_by_path(settings, tokens, parsed)
    print(f"Updated {path} -> {parsed!r}")
    return True


def main() -> None:
    settings = load_settings()
    dirty = False
    filter_text = ""

    while True:
        entries = _flatten_leaves(settings)
        _print_header(dirty)
        _print_entries(entries, filter_text=filter_text)
        raw = input("\ncommand> ").strip()
        if not raw:
            continue

        lower = raw.lower()
        if lower in ("q", "quit", "exit"):
            if dirty and _confirm("Save changes before quitting? [y/N]: "):
                save_settings(settings)
                print("Saved settings.json")
            print("Bye.")
            return

        if lower in ("h", "help", "?"):
            print("list")
            print("  Show all editable settings.")
            print("list <text>")
            print("  Show only paths containing <text>.")
            print("edit <index>")
            print("  Edit one setting by index.")
            print("save")
            print("  Write current in-memory settings to settings.json.")
            print("reload")
            print("  Reload from settings.json (drops unsaved edits).")
            print("reset")
            print("  Reset to DEFAULT_SETTINGS from settings_manager.py.")
            print("quit")
            print("  Exit. Prompts to save if needed.")
            continue

        if lower.startswith("list"):
            parts = raw.split(maxsplit=1)
            filter_text = parts[1].strip() if len(parts) > 1 else ""
            continue

        if lower.startswith("edit "):
            if _handle_edit(settings, entries, raw):
                dirty = True
            continue

        if lower in ("s", "save"):
            save_settings(settings)
            dirty = False
            print("Saved settings.json")
            continue

        if lower in ("r", "reload"):
            if dirty and (not _confirm("Discard unsaved changes and reload? [y/N]: ")):
                print("Reload cancelled.")
                continue
            settings = load_settings()
            dirty = False
            print("Reloaded settings.json")
            continue

        if lower == "reset":
            if _confirm("Reset all settings to defaults? [y/N]: "):
                settings = json.loads(json.dumps(DEFAULT_SETTINGS))
                dirty = True
                print("Loaded defaults into memory (not saved yet).")
            else:
                print("Reset cancelled.")
            continue

        print("Unknown command. Type `help` for usage.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
