import runpy
from pathlib import Path

from settings_manager import load_settings


DRAW_SCRIPT = "import pygrame draw mode.py"
HEADLESS_SCRIPT = "import pygrame no draw fast.py"


def main() -> None:
    settings = load_settings()
    use_draw = bool(settings.get("draw", True))
    script = DRAW_SCRIPT if use_draw else HEADLESS_SCRIPT
    script_path = Path(script)
    if not script_path.exists():
        raise FileNotFoundError(f"Simulation script not found: {script_path}")
    runpy.run_path(str(script_path), run_name="__main__")


if __name__ == "__main__":
    main()
