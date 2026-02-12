import json
from pathlib import Path

SETTINGS_PATH = Path("settings.json")
DEFAULT_SETTINGS = {
    "screen": {"width": 1280, "height": 832},
    "display_mode": 2,
    "draw": False,
    "drawSometimes": True,
    "drawAmnt": 500,
    "population_cap": 0.5,
    "enviormentChangeRate": 1.0,
    "ph_effect": {"scale": 1.0, "divisor": 2.0},
    "temp_effect": {"scale": 1.0, "divisor": 4.0},
    "reproduction_debuf_min": 0.001,
    "quan": 2.0,
    "immune_system_quan_factor": 0.15,
    "num_tries": 0,
    "num_tries_master": 0,
    "simulations": {"count": 3},
}


def _merge_settings(defaults, loaded):
    if not isinstance(loaded, dict):
        return defaults
    merged = {}
    for key, default_value in defaults.items():
        if isinstance(default_value, dict):
            merged[key] = _merge_settings(default_value, loaded.get(key, {}))
        else:
            merged[key] = loaded.get(key, default_value)
    for key, value in loaded.items():
        if key not in merged:
            merged[key] = value
    return merged


def _has_missing_keys(defaults, loaded):
    if not isinstance(loaded, dict):
        return True
    for key, default_value in defaults.items():
        if key not in loaded:
            return True
        if isinstance(default_value, dict) and _has_missing_keys(default_value, loaded.get(key)):
            return True
    return False


def save_settings(settings):
    try:
        with open(SETTINGS_PATH, "w") as handle:
            json.dump(settings, handle, indent=2)
    except Exception:
        pass


def load_settings():
    loaded = {}
    if SETTINGS_PATH.exists():
        try:
            with open(SETTINGS_PATH, "r") as handle:
                loaded = json.load(handle)
        except Exception:
            loaded = {}
    merged = _merge_settings(DEFAULT_SETTINGS, loaded)
    # Normalize counters to represent the next available run/master id.
    try:
        num_tries = int(merged.get("num_tries", 0))
    except Exception:
        num_tries = 0
    try:
        num_tries_master = int(merged.get("num_tries_master", 0))
    except Exception:
        num_tries_master = 0
    if (Path("results") / str(num_tries)).exists():
        merged["num_tries"] = max(0, num_tries + 1)
    if (Path("results") / f"master_{num_tries_master}").exists():
        merged["num_tries_master"] = max(0, num_tries_master + 1)
    if not SETTINGS_PATH.exists() or _has_missing_keys(DEFAULT_SETTINGS, loaded):
        save_settings(merged)
    return merged
