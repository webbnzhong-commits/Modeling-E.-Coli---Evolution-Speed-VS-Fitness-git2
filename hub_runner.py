import argparse
import csv
import json
import math
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from settings_manager import load_settings, save_settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run hub folders of master simulations across enviormentChangeRate values."
    )
    parser.add_argument("--results-root", type=str, default="results")
    parser.add_argument("--script", type=str, default="simulation_entry.py")
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--start-rate", type=float, default=None)
    parser.add_argument("--end-rate", type=float, default=None)
    parser.add_argument("--step", type=float, default=None)
    parser.add_argument("--species-threshold", type=int, default=None)
    parser.add_argument("--max-masters", type=int, default=None)
    parser.add_argument(
        "--hub-select",
        action="store_true",
        help="Force hub selector UI (new hub or continue existing).",
    )
    parser.add_argument(
        "--continue-hub",
        type=int,
        default=None,
        help="Continue a specific existing hub index (e.g. --continue-hub 3).",
    )
    parser.add_argument(
        "--skip-plots",
        action="store_true",
        help="Skip matplotlib plot generation and only write CSV/JSON outputs.",
    )
    parser.add_argument(
        "--no-screen",
        action="store_true",
        help="Disable the live hub dashboard window.",
    )
    parser.add_argument(
        "--screen-hold-seconds",
        type=float,
        default=0.0,
        help="Keep the dashboard open for N seconds after completion (0 = do not hold).",
    )
    return parser.parse_args()


def _hub_defaults_from_settings() -> dict:
    settings = load_settings()
    hub_cfg = settings.get("hub", {}) if isinstance(settings, dict) else {}
    if not isinstance(hub_cfg, dict):
        hub_cfg = {}
    return {
        "start_rate": float(hub_cfg.get("start_rate", 0.5)),
        "end_rate": float(hub_cfg.get("end_rate", 1.5)),
        "step": float(hub_cfg.get("step", 0.01)),
        "species_threshold": int(hub_cfg.get("species_threshold", 100000)),
        "max_masters": int(hub_cfg.get("max_masters", 101)),
    }


def _rate_values(start: float, end: float, step: float) -> list[float]:
    if step <= 0:
        return []
    if end < start:
        start, end = end, start
    out = []
    cur = start
    guard = 0
    while cur <= end + (step * 0.5):
        out.append(round(cur, 6))
        cur += step
        guard += 1
        if guard > 100000:
            break
    return out


def _allocate_hub_index(results_root: Path) -> int:
    settings = load_settings()
    try:
        current = int(settings.get("num_tries_hub", 0))
    except Exception:
        current = 0
    current = max(0, current)
    default_root = Path("results")
    try:
        same_root = default_root.resolve() == results_root.resolve()
    except Exception:
        same_root = str(default_root) == str(results_root)

    candidate = current
    while True:
        in_target = results_root / f"hub_{candidate}"
        in_default = default_root / f"hub_{candidate}"
        if in_target.exists():
            candidate += 1
            continue
        if (not same_root) and in_default.exists():
            candidate += 1
            continue
        break

    settings["num_tries_hub"] = candidate + 1
    save_settings(settings)
    return candidate


def _rate_label(rate: float) -> str:
    return f"env_{rate:.2f}".replace(".", "p")


def _parse_hub_id(path: Path):
    if not path.name.startswith("hub_"):
        return None
    try:
        return int(path.name.split("_", 1)[1])
    except Exception:
        return None


def _parse_master_id(path: Path):
    if not path.name.startswith("master_"):
        return None
    try:
        return int(path.name.split("_", 1)[1])
    except Exception:
        return None


def _collect_existing_master_ids(results_root: Path) -> set[int]:
    ids: set[int] = set()
    if not results_root.exists():
        return ids
    for path in results_root.rglob("master_*"):
        if not path.is_dir():
            continue
        run_id = _parse_master_id(path)
        if run_id is None:
            continue
        ids.add(int(run_id))
    return ids


def _collect_hub_runs(results_root: Path) -> list[dict]:
    hubs = []
    if not results_root.exists():
        return hubs
    for path in sorted(results_root.glob("hub_*")):
        if not path.is_dir():
            continue
        hub_idx = _parse_hub_id(path)
        if hub_idx is None:
            continue
        meta_path = path / "hub_meta.json"
        meta = {}
        if meta_path.exists():
            try:
                loaded = json.loads(meta_path.read_text())
                if isinstance(loaded, dict):
                    meta = loaded
            except Exception:
                meta = {}
        rates = meta.get("rates")
        if not isinstance(rates, list):
            rates = []
        total_steps = len(rates)
        steps = meta.get("steps")
        if not isinstance(steps, list):
            steps = []
        ok_indices = set()
        for step in steps:
            if not isinstance(step, dict):
                continue
            if str(step.get("status", "")) != "ok":
                continue
            try:
                idx = int(step.get("step_index"))
            except Exception:
                continue
            if idx < 0:
                continue
            ok_indices.add(idx)
        completed_steps = len(ok_indices)
        if total_steps > 0:
            completed_steps = min(completed_steps, total_steps)
        hubs.append(
            {
                "hub_idx": int(hub_idx),
                "hub_dir": path,
                "status": str(meta.get("status", "unknown")),
                "total_steps": int(total_steps),
                "completed_steps": int(completed_steps),
                "created_at": meta.get("created_at"),
                "meta_path": meta_path,
            }
        )
    hubs.sort(key=lambda item: int(item["hub_idx"]))
    return hubs


def _ensure_csv_with_header(path: Path, header_line: str) -> None:
    if path.exists():
        try:
            if path.stat().st_size > 0:
                return
        except Exception:
            pass
    try:
        path.write_text(header_line)
    except Exception:
        pass


def _select_hub_run_ui(results_root: Path):
    hub_rows = _collect_hub_runs(results_root)
    try:
        import pygame  # pylint: disable=import-outside-toplevel
    except Exception:
        return {"mode": "new"}

    pygame.init()
    try:
        screen_w = 840
        screen_h = 560
        screen = pygame.display.set_mode((screen_w, screen_h))
        pygame.display.set_caption("Hub Selector")
        font = pygame.font.SysFont("Consolas", 22)
        small = pygame.font.SysFont("Consolas", 18)
        tiny = pygame.font.SysFont("Consolas", 15)
        clock = pygame.time.Clock()

        rows = [{"mode": "new", "label": "Create New Hub"}]
        for row in hub_rows:
            done = int(row.get("completed_steps", 0))
            total = int(row.get("total_steps", 0))
            status = str(row.get("status", "unknown"))
            rows.append(
                {
                    "mode": "continue",
                    "hub_idx": int(row["hub_idx"]),
                    "hub_dir": row["hub_dir"],
                    "created_at": row.get("created_at"),
                    "status": status,
                    "completed_steps": done,
                    "total_steps": total,
                    "label": f"Continue hub_{int(row['hub_idx'])} ({done}/{total}) [{status}]",
                }
            )

        selected = 0
        scroll = 0
        list_top = 82
        list_bottom = screen_h - 86
        line_h = small.get_height() + 8
        visible = max(1, (list_bottom - list_top) // line_h)
        left_x = 20
        left_w = 430
        detail_x = left_x + left_w + 20
        detail_w = screen_w - detail_x - 20
        choose_rect = pygame.Rect(detail_x, screen_h - 72, detail_w, 32)
        cancel_rect = pygame.Rect(screen_w - 94, 14, 74, 28)

        def _ensure_visible() -> None:
            nonlocal scroll
            if selected < scroll:
                scroll = selected
            elif selected >= scroll + visible:
                scroll = selected - visible + 1
            scroll = max(0, min(scroll, max(0, len(rows) - visible)))

        _ensure_visible()
        while True:
            selected = max(0, min(selected, len(rows) - 1))
            current = rows[selected]
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    return None
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_ESCAPE:
                        return None
                    if event.key == pygame.K_UP:
                        selected = (selected - 1) % len(rows)
                        _ensure_visible()
                    elif event.key == pygame.K_DOWN:
                        selected = (selected + 1) % len(rows)
                        _ensure_visible()
                    elif event.key in (pygame.K_RETURN, pygame.K_KP_ENTER):
                        return current
                if event.type == pygame.MOUSEWHEEL:
                    if event.y > 0:
                        selected = (selected - 1) % len(rows)
                    elif event.y < 0:
                        selected = (selected + 1) % len(rows)
                    _ensure_visible()
                if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                    mx, my = event.pos
                    if cancel_rect.collidepoint(mx, my):
                        return None
                    if choose_rect.collidepoint(mx, my):
                        return current
                    if left_x <= mx <= left_x + left_w and list_top <= my <= list_bottom:
                        idx = (my - list_top) // line_h + scroll
                        if 0 <= idx < len(rows):
                            selected = int(idx)
                            _ensure_visible()

            screen.fill((18, 18, 18))
            title = font.render("Hub Selector", True, (230, 230, 230))
            screen.blit(title, (20, 20))

            pygame.draw.rect(screen, (60, 60, 60), cancel_rect)
            pygame.draw.rect(screen, (160, 160, 160), cancel_rect, 1)
            cancel_text = tiny.render("Cancel", True, (230, 230, 230))
            screen.blit(
                cancel_text,
                (
                    cancel_rect.x + (cancel_rect.width - cancel_text.get_width()) // 2,
                    cancel_rect.y + 6,
                ),
            )

            pygame.draw.rect(screen, (26, 28, 32), (left_x, list_top, left_w, list_bottom - list_top))
            pygame.draw.rect(screen, (70, 74, 86), (left_x, list_top, left_w, list_bottom - list_top), 1)
            for idx in range(scroll, min(len(rows), scroll + visible)):
                row = rows[idx]
                y = list_top + (idx - scroll) * line_h
                if idx == selected:
                    pygame.draw.rect(screen, (40, 44, 54), (left_x + 2, y - 2, left_w - 4, line_h))
                color = (0, 215, 255) if idx == selected else (220, 220, 220)
                txt = small.render(str(row.get("label", "")), True, color)
                screen.blit(txt, (left_x + 8, y))

            pygame.draw.rect(screen, (26, 28, 32), (detail_x, list_top, detail_w, list_bottom - list_top))
            pygame.draw.rect(screen, (70, 74, 86), (detail_x, list_top, detail_w, list_bottom - list_top), 1)

            detail_lines = []
            if current.get("mode") == "new":
                detail_lines = [
                    "Mode: New hub run",
                    "A new hub index will be allocated.",
                    f"Root: {results_root}",
                ]
            else:
                created_at = current.get("created_at")
                created_txt = "-"
                try:
                    created_txt = datetime.fromtimestamp(float(created_at)).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
                detail_lines = [
                    f"Mode: Continue hub_{int(current.get('hub_idx'))}",
                    f"Status: {current.get('status', 'unknown')}",
                    f"Progress: {current.get('completed_steps', 0)}/{current.get('total_steps', 0)}",
                    f"Created: {created_txt}",
                    f"Path: {current.get('hub_dir')}",
                ]
            yy = list_top + 10
            for line in detail_lines:
                line_txt = tiny.render(str(line), True, (205, 205, 205))
                screen.blit(line_txt, (detail_x + 8, yy))
                yy += line_h

            btn_label = "Choose New Hub" if current.get("mode") == "new" else "Continue Selected Hub"
            pygame.draw.rect(screen, (42, 42, 46), choose_rect)
            pygame.draw.rect(screen, (170, 170, 170), choose_rect, 1)
            choose_text = small.render(btn_label, True, (230, 230, 230))
            screen.blit(
                choose_text,
                (
                    choose_rect.x + (choose_rect.width - choose_text.get_width()) // 2,
                    choose_rect.y + 6,
                ),
            )

            hint = tiny.render("Up/Down select  Enter choose  Esc cancel", True, (170, 170, 170))
            screen.blit(hint, (20, screen_h - 46))
            pygame.display.flip()
            clock.tick(30)
    except Exception:
        return {"mode": "new"}
    finally:
        try:
            pygame.display.quit()
            pygame.quit()
        except Exception:
            pass


def _plan_master_ids(start_id: int, count: int, existing_ids: set[int]) -> list[int]:
    out = []
    candidate = max(0, int(start_id))
    used = set(int(x) for x in existing_ids)
    while len(out) < count:
        if candidate not in used:
            out.append(candidate)
            used.add(candidate)
        candidate += 1
        if candidate > 10_000_000:
            break
    return out


def _format_id_span(ids: list[int]) -> str:
    if not ids:
        return "none"
    ordered = sorted(set(int(x) for x in ids))
    if len(ordered) == 1:
        return str(ordered[0])
    contiguous = all((ordered[i] + 1) == ordered[i + 1] for i in range(len(ordered) - 1))
    if contiguous:
        return f"{ordered[0]}-{ordered[-1]}"
    return ",".join(str(x) for x in ordered)


def _latest_master_dir(results_dir: Path) -> Path | None:
    best = None
    best_num = -1
    for path in results_dir.glob("master_*"):
        if not path.is_dir():
            continue
        try:
            run_num = int(path.name.split("_", 1)[1])
        except Exception:
            continue
        if run_num > best_num:
            best_num = run_num
            best = path
    return best


def _master_run_nums(master_dir: Path) -> list[int]:
    meta_path = master_dir / "master_meta.json"
    if not meta_path.exists():
        return []
    try:
        payload = json.loads(meta_path.read_text())
    except Exception:
        return []
    out = []
    raw_runs = payload.get("run_nums", []) if isinstance(payload, dict) else []
    if not isinstance(raw_runs, list):
        return out
    for val in raw_runs:
        try:
            out.append(int(val))
        except Exception:
            continue
    return sorted(set(out))


def _read_species_from_run_meta(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    species = payload.get("amnt_of_species")
    if not isinstance(species, (int, float)):
        return None
    return float(species)


def _max_species(results_dir: Path, run_nums: list[int]) -> float | None:
    best = None
    for run_num in run_nums:
        species = _read_species_from_run_meta(results_dir / str(run_num) / "run_meta.json")
        if species is None:
            continue
        best = species if best is None else max(best, species)
    return best


def _extract_points_from_csv(path: Path) -> list[tuple[float, float]]:
    if not path.exists():
        return []
    points = []
    try:
        with open(path, newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                try:
                    x = float(row.get("evolution rate", ""))
                    y = float(row.get("arithmetic mean length lived", ""))
                except Exception:
                    continue
                if math.isfinite(x) and math.isfinite(y):
                    points.append((x, y))
    except Exception:
        return []
    return points


def _master_points(master_dir: Path, run_nums: list[int]) -> list[tuple[float, float]]:
    master_path = master_dir / f"parsedArithmeticMeanSimulatino{master_dir.name}_Log.csv"
    points = _extract_points_from_csv(master_path)
    if points:
        return points
    parent = master_dir.parent
    merged = []
    for run_num in run_nums:
        run_path = parent / str(run_num) / f"parsedArithmeticMeanSimulatino{run_num}_Log.csv"
        merged.extend(_extract_points_from_csv(run_path))
    return merged


def _predict_piecewise_gaussian(x: float, apex_x: float, apex_y: float, sigma_left: float, sigma_right: float) -> float:
    if apex_y <= 0:
        return 0.0
    sigma = sigma_left if x <= apex_x else sigma_right
    sigma = max(1e-9, sigma)
    expo = -((x - apex_x) ** 2) / (2.0 * sigma * sigma)
    return apex_y * math.exp(expo)


def _fit_side_sigma(apex_x: float, apex_y: float, side_points: list[tuple[float, float]]) -> float | None:
    if apex_y <= 0:
        return None
    num = 0.0
    den = 0.0
    for x, y in side_points:
        if y <= 0 or y >= apex_y:
            continue
        d2 = (x - apex_x) ** 2
        if d2 <= 0:
            continue
        try:
            lr = math.log(y / apex_y)
        except Exception:
            continue
        if not math.isfinite(lr):
            continue
        num += d2 * lr
        den += d2 * d2
    if den <= 0:
        return None
    slope = num / den
    if slope >= 0:
        return None
    sigma_sq = -1.0 / (2.0 * slope)
    if sigma_sq <= 0 or not math.isfinite(sigma_sq):
        return None
    return math.sqrt(sigma_sq)


def _fit_stitched_gaussian(points: list[tuple[float, float]]) -> dict | None:
    if len(points) < 3:
        return None
    apex_x, apex_y = max(points, key=lambda p: p[1])
    if apex_y <= 0:
        return None

    left = [(x, y) for (x, y) in points if x <= apex_x]
    right = [(x, y) for (x, y) in points if x >= apex_x]
    sigma_left = _fit_side_sigma(apex_x, apex_y, left)
    sigma_right = _fit_side_sigma(apex_x, apex_y, right)

    # Fallback keeps the fit usable even when one side is sparse.
    if sigma_left is None and sigma_right is None:
        sigma_left = 0.05
        sigma_right = 0.05
    elif sigma_left is None:
        sigma_left = sigma_right
    elif sigma_right is None:
        sigma_right = sigma_left

    assert sigma_left is not None
    assert sigma_right is not None

    ys = [y for _, y in points]
    y_mean = sum(ys) / len(ys)
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    ss_res = 0.0
    for x, y in points:
        pred = _predict_piecewise_gaussian(x, apex_x, apex_y, sigma_left, sigma_right)
        ss_res += (y - pred) ** 2
    r2 = None
    if ss_tot > 0:
        r2 = 1.0 - (ss_res / ss_tot)

    equation = (
        f"y={apex_y:.6g}*exp(-((x-{apex_x:.6g})^2)/(2*{sigma_left:.6g}^2)) for x<={apex_x:.6g}; "
        f"y={apex_y:.6g}*exp(-((x-{apex_x:.6g})^2)/(2*{sigma_right:.6g}^2)) for x>{apex_x:.6g}"
    )
    return {
        "apex_x": float(apex_x),
        "apex_y": float(apex_y),
        "sigma_left": float(sigma_left),
        "sigma_right": float(sigma_right),
        "r2": (None if r2 is None else float(r2)),
        "equation": equation,
    }


def _symlink_if_missing(alias: Path, target: Path) -> None:
    try:
        target = target.resolve()
    except Exception:
        return
    if alias.exists() or alias.is_symlink():
        return
    try:
        alias.symlink_to(target, target_is_directory=target.is_dir())
    except Exception:
        pass


def _plot_hub_scatter(rows: list[dict], out_path: Path) -> bool:
    try:
        import matplotlib.pyplot as plt
        from matplotlib import cm
    except Exception:
        return False
    usable = [r for r in rows if r.get("env_rate") is not None and r.get("apex_x") is not None and r.get("apex_y") is not None]
    if not usable:
        return False

    xs = [float(r["env_rate"]) for r in usable]
    ys = [float(r["apex_x"]) for r in usable]
    fits = [float(r["apex_y"]) for r in usable]
    f_min = min(fits)
    f_max = max(fits)
    denom = max(1e-9, f_max - f_min)
    norms = [(v - f_min) / denom for v in fits]
    sizes = [40.0 + (n * 460.0) for n in norms]
    colors = [cm.plasma(n) for n in norms]
    colors = [(r, g, b, 0.2 + (0.8 * n)) for (r, g, b, _), n in zip(colors, norms)]

    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_subplot(111)
    sc = ax.scatter(xs, ys, s=sizes, c=norms, cmap="plasma", alpha=0.95, edgecolors=colors, linewidths=1.0)
    ax.set_title("Hub Apex Map: Enviorment Change Rate vs Evolution Speed")
    ax.set_xlabel("enviorment change rate")
    ax.set_ylabel("apex evolution speed")
    ax.grid(alpha=0.25)
    cbar = fig.colorbar(sc, ax=ax)
    cbar.set_label("relative fitness (apex arithmetic mean length lived)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=170)
    plt.close(fig)
    return True


def _plot_stitched_fits(rows: list[dict], out_path: Path) -> bool:
    try:
        import matplotlib.pyplot as plt
        from matplotlib import cm
    except Exception:
        return False
    usable = [r for r in rows if r.get("fit") and r.get("points")]
    if not usable:
        return False

    usable = sorted(usable, key=lambda r: float(r["env_rate"]))
    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_subplot(111)
    total = max(1, len(usable) - 1)
    for idx, row in enumerate(usable):
        rate = float(row["env_rate"])
        fit = row["fit"]
        points = row["points"]
        color = cm.viridis(idx / total)
        xs = sorted(set(float(x) for x, _ in points))
        if len(xs) < 2:
            continue
        min_x = xs[0]
        max_x = xs[-1]
        sample = []
        for i in range(100):
            x = min_x + ((max_x - min_x) * i / 99.0)
            y = _predict_piecewise_gaussian(
                x,
                float(fit["apex_x"]),
                float(fit["apex_y"]),
                float(fit["sigma_left"]),
                float(fit["sigma_right"]),
            )
            sample.append((x, y))
        ax.plot([p[0] for p in sample], [p[1] for p in sample], color=color, alpha=0.8, linewidth=1.2)
        ax.scatter(
            [p[0] for p in points],
            [p[1] for p in points],
            color=[color],
            alpha=0.12,
            s=8,
        )
        ax.text(float(fit["apex_x"]), float(fit["apex_y"]), f"{rate:.2f}", color=color, fontsize=7)
    ax.set_title("Per-Master Stitched Normal Fits (Apex-Locked)")
    ax.set_xlabel("evolution speed")
    ax.set_ylabel("fitness (arithmetic mean length lived)")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=170)
    plt.close(fig)
    return True


def _plot_ratio_curve(rows: list[dict], out_path: Path) -> bool:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return False
    usable = []
    for row in rows:
        rate = row.get("env_rate")
        apex_x = row.get("apex_x")
        apex_y = row.get("apex_y")
        if rate is None or apex_x is None or apex_y is None:
            continue
        if float(apex_x) <= 0:
            continue
        usable.append((float(rate), float(apex_y) / float(apex_x)))
    if not usable:
        return False
    usable.sort(key=lambda v: v[0])
    fig = plt.figure(figsize=(14, 6))
    ax = fig.add_subplot(111)
    ax.plot([r for r, _ in usable], [ratio for _, ratio in usable], color="#cc3d1f", linewidth=2.0)
    ax.scatter([r for r, _ in usable], [ratio for _, ratio in usable], color="#f1a208", s=28)
    ax.set_title("Fitness / Evolution-Speed Ratio by Enviorment Change Rate")
    ax.set_xlabel("enviorment change rate")
    ax.set_ylabel("fitness / evolution speed")
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(out_path, dpi=170)
    plt.close(fig)
    return True


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _fmt_duration(seconds: float | None) -> str:
    if not _is_number(seconds):
        return "--:--"
    total = max(0, int(round(float(seconds))))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def _fmt_eta(unix_ts: float | None) -> str:
    if not _is_number(unix_ts):
        return "--:--"
    try:
        return datetime.fromtimestamp(float(unix_ts)).strftime("%H:%M:%S")
    except Exception:
        return "--:--"


def _linear_fit(xs: list[float], ys: list[float]):
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    n = float(len(xs))
    sx = sum(xs)
    sy = sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    den = (n * sxx) - (sx * sx)
    if abs(den) < 1e-12:
        return None
    slope = ((n * sxy) - (sx * sy)) / den
    intercept = (sy - (slope * sx)) / n
    return slope, intercept


def _project_metric(rows: list[dict], value_key: str) -> list[dict]:
    actual_points = []
    for row in rows:
        x = row.get("env_rate")
        y = row.get(value_key)
        if _is_number(x) and _is_number(y):
            actual_points.append((float(x), float(y)))

    model = None
    if len(actual_points) >= 2:
        model = _linear_fit([p[0] for p in actual_points], [p[1] for p in actual_points])
    elif len(actual_points) == 1:
        model = (0.0, float(actual_points[0][1]))

    projected = []
    for row in sorted(rows, key=lambda r: float(r.get("env_rate", 0.0))):
        x = row.get("env_rate")
        if not _is_number(x):
            continue
        x = float(x)
        y_actual = row.get(value_key)
        if _is_number(y_actual):
            projected.append({"x": x, "y": float(y_actual), "actual": True})
            continue
        y_pred = None
        if model is not None:
            y_pred = (model[0] * x) + model[1]
            if not _is_number(y_pred):
                y_pred = None
            elif y_pred < 0:
                y_pred = 0.0
        projected.append({"x": x, "y": y_pred, "actual": False})
    return projected


def _project_final_bubble(rows: list[dict]) -> list[dict]:
    evo_points = _project_metric(rows, "apex_evolution_rate")
    fit_points = _project_metric(rows, "apex_fitness")
    fit_by_x = {float(p["x"]): p for p in fit_points if _is_number(p.get("x"))}

    out = []
    for evo in evo_points:
        if not _is_number(evo.get("x")) or not _is_number(evo.get("y")):
            continue
        x = float(evo["x"])
        fit = fit_by_x.get(x)
        fitness = None
        fit_actual = False
        if isinstance(fit, dict) and _is_number(fit.get("y")):
            fitness = float(fit["y"])
            fit_actual = bool(fit.get("actual"))
        out.append(
            {
                "x": x,
                "evo": float(evo["y"]),
                "fitness": fitness,
                "actual": bool(evo.get("actual")) and fit_actual,
            }
        )
    return out


def _runtime_prediction(rows: list[dict], start_ts: float, now_ts: float):
    elapsed = max(0.0, float(now_ts - start_ts))
    done = 0
    durations = []
    running_elapsed = None
    for row in rows:
        status = str(row.get("status", "pending"))
        if status in ("ok", "failed", "no_master", "stopped", "aborted"):
            done += 1
            dur = row.get("duration_s")
            if _is_number(dur):
                durations.append(float(dur))
        elif status == "running":
            start = row.get("started_at")
            if _is_number(start):
                running_elapsed = max(0.0, now_ts - float(start))
    avg = None
    if durations:
        avg = sum(durations) / len(durations)
    elif _is_number(running_elapsed):
        avg = float(running_elapsed)
    total = len(rows)
    remaining_steps = max(0, total - done)
    predicted_total = None
    remaining_seconds = None
    eta = None
    if _is_number(avg):
        predicted_total = float(avg) * float(total)
        remaining_seconds = max(0.0, predicted_total - elapsed)
        eta = now_ts + remaining_seconds
    return {
        "elapsed_s": elapsed,
        "remaining_s": remaining_seconds,
        "predicted_total_s": predicted_total,
        "eta_ts": eta,
        "done_steps": done,
        "total_steps": total,
    }


_FPS_MODE_CAPPED = 0
_FPS_MODE_UNCAPPED = 1
_FPS_MODE_FULL_THROTTLE = 2


class _HubDashboard:
    def __init__(
        self,
        enabled: bool,
        hub_idx: int,
        hub_dir: Path,
        planned_master_span: str,
        species_threshold: int,
        reopen_callback=None,
        close_callback=None,
        shutdown_callback=None,
    ) -> None:
        self.enabled = False
        self.closed = False
        self.scroll = 0
        self.scroll_target = 0.0
        self._last_draw = 0.0
        self._refresh_s = 0.1
        self.hub_idx = int(hub_idx)
        self.hub_dir = str(hub_dir)
        self.planned_master_span = str(planned_master_span)
        self.species_threshold = int(species_threshold)
        self.fps_mode = _FPS_MODE_CAPPED
        self.capped_fps = 1
        self.uncapped_fps = 120
        self.draw_fps = self.capped_fps
        self.reopen_callback = reopen_callback
        self.close_callback = close_callback
        self.shutdown_callback = shutdown_callback
        self.selected_row_index = 0
        self._rows_cache = []
        self._table_rect = None
        self._row_h = 20
        self._rows_base_y = 0
        self._scroll_i = 0
        self._visible_rows = 0
        self._reopen_button_rect = None
        self._close_button_rect = None
        self._shutdown_button_rect = None
        if not enabled:
            return
        try:
            import pygame  # pylint: disable=import-outside-toplevel
        except Exception:
            return
        self.pg = pygame
        try:
            pygame.init()
            self.window_w = 1360
            self.window_h = 820
            self.screen = pygame.display.set_mode((self.window_w, self.window_h))
            pygame.display.set_caption("Hub Runner Dashboard")
            self.font = pygame.font.SysFont("Consolas", 18)
            self.small = pygame.font.SysFont("Consolas", 15)
            self.title = pygame.font.SysFont("Consolas", 24)
            self.clock = pygame.time.Clock()
            self.enabled = True
        except Exception:
            self.enabled = False

    def _apply_fps_mode(self, new_mode: int) -> None:
        self.fps_mode = int(new_mode) % 3
        if self.fps_mode == _FPS_MODE_CAPPED:
            self._refresh_s = 0.1
            self.draw_fps = self.capped_fps
        elif self.fps_mode == _FPS_MODE_UNCAPPED:
            self._refresh_s = 0.0
            self.draw_fps = self.uncapped_fps
        else:
            self._refresh_s = 0.0
            self.draw_fps = 0

    def poll_sleep_seconds(self) -> float:
        if self.fps_mode == _FPS_MODE_FULL_THROTTLE:
            return 0.0
        if self.fps_mode == _FPS_MODE_UNCAPPED:
            return 0.02
        return 0.12

    def _set_scroll_target(self, value: float, max_scroll: int) -> None:
        self.scroll_target = max(0.0, min(float(max_scroll), float(value)))

    def _scroll_by(self, delta: float, max_scroll: int) -> None:
        self._set_scroll_target(self.scroll_target + float(delta), max_scroll)

    def _selected_row(self):
        if not self._rows_cache:
            return None
        idx = max(0, min(int(self.selected_row_index), len(self._rows_cache) - 1))
        self.selected_row_index = idx
        return self._rows_cache[idx]

    def _trigger_reopen(self) -> None:
        if not callable(self.reopen_callback):
            return
        row = self._selected_row()
        if not isinstance(row, dict):
            return
        try:
            self.reopen_callback(row)
        except Exception:
            pass

    def _trigger_close(self) -> None:
        if not callable(self.close_callback):
            return
        row = self._selected_row()
        if not isinstance(row, dict):
            return
        try:
            self.close_callback(row)
        except Exception:
            pass

    def _trigger_shutdown(self) -> None:
        if callable(self.shutdown_callback):
            try:
                self.shutdown_callback()
            except Exception:
                pass
        self.closed = True

    def _pump_events(self) -> None:
        if not self.enabled:
            return
        for event in self.pg.event.get():
            if event.type == self.pg.QUIT:
                self.closed = True
            elif event.type == self.pg.KEYDOWN:
                if event.key in (self.pg.K_ESCAPE, self.pg.K_q):
                    self.closed = True
                elif event.key == self.pg.K_f:
                    self._apply_fps_mode((self.fps_mode + 1) % 3)
                elif event.key == self.pg.K_r:
                    self._trigger_reopen()
                elif event.key == self.pg.K_c:
                    self._trigger_close()
                elif event.key == self.pg.K_x:
                    self._trigger_shutdown()
                elif event.key == self.pg.K_UP:
                    self.scroll_target = max(0.0, self.scroll_target - 1.0)
                elif event.key == self.pg.K_DOWN:
                    self.scroll_target += 1.0
                elif event.key == self.pg.K_PAGEUP:
                    self.scroll_target = max(0.0, self.scroll_target - 12.0)
                elif event.key == self.pg.K_PAGEDOWN:
                    self.scroll_target += 12.0
                elif event.key == self.pg.K_HOME:
                    self.scroll_target = 0.0
                elif event.key == self.pg.K_END:
                    self.scroll_target = 1e9
            elif event.type == self.pg.MOUSEWHEEL:
                if event.y > 0:
                    self.scroll_target = max(0.0, self.scroll_target - 2.0)
                elif event.y < 0:
                    self.scroll_target += 2.0
            elif event.type == self.pg.MOUSEBUTTONDOWN and event.button == 1:
                mx, my = event.pos
                if (
                    self._reopen_button_rect is not None
                    and self._reopen_button_rect.collidepoint(mx, my)
                ):
                    self._trigger_reopen()
                if (
                    self._close_button_rect is not None
                    and self._close_button_rect.collidepoint(mx, my)
                ):
                    self._trigger_close()
                if (
                    self._shutdown_button_rect is not None
                    and self._shutdown_button_rect.collidepoint(mx, my)
                ):
                    self._trigger_shutdown()
                if self._table_rect is not None and self._table_rect.collidepoint(mx, my):
                    if my >= self._rows_base_y:
                        local = int((my - self._rows_base_y) // self._row_h)
                        if 0 <= local < self._visible_rows:
                            idx = int(self._scroll_i + local)
                            if 0 <= idx < len(self._rows_cache):
                                self.selected_row_index = idx
        if self.closed:
            try:
                self.pg.display.quit()
                self.pg.quit()
            except Exception:
                pass
            self.enabled = False

    def _draw_graph(self, rect, rows: list[dict]) -> None:
        pg = self.pg
        pg.draw.rect(self.screen, (22, 24, 29), rect)
        pg.draw.rect(self.screen, (70, 74, 86), rect, 1)
        title = self.font.render("Possible Final Results", True, (220, 220, 220))
        self.screen.blit(title, (rect.x + 10, rect.y + 8))
        subtitle = self.small.render(
            "x: env change rate   y: evolution speed   dot size: fitness",
            True,
            (170, 170, 170),
        )
        self.screen.blit(subtitle, (rect.x + 12, rect.y + 36))
        projected = _project_final_bubble(rows)
        ys = [p["evo"] for p in projected if _is_number(p.get("evo"))]
        xs = [p["x"] for p in projected if _is_number(p.get("x"))]
        if not ys or not xs:
            msg = self.small.render("Need more data to project final bubbles.", True, (170, 170, 170))
            self.screen.blit(msg, (rect.x + 12, rect.y + 38))
            return
        min_x = min(xs)
        max_x = max(xs)
        if max_x <= min_x:
            max_x = min_x + 1.0
        min_y = min(ys)
        max_y = max(ys)
        if max_y <= min_y:
            max_y = min_y + 1.0
        y_pad = (max_y - min_y) * 0.1
        min_y = min_y - y_pad
        max_y = max_y + y_pad

        plot = pg.Rect(rect.x + 46, rect.y + 62, rect.width - 62, rect.height - 86)
        pg.draw.rect(self.screen, (16, 18, 23), plot)
        pg.draw.rect(self.screen, (64, 68, 79), plot, 1)

        def _to_px(xv, yv):
            px = plot.x + int(((xv - min_x) / (max_x - min_x)) * plot.width)
            py = plot.y + plot.height - int(((yv - min_y) / (max_y - min_y)) * plot.height)
            return px, py

        fits = [
            float(p["fitness"])
            for p in projected
            if _is_number(p.get("fitness"))
        ]
        fit_min = min(fits) if fits else 0.0
        fit_max = max(fits) if fits else 1.0
        fit_denom = max(1e-9, fit_max - fit_min)

        def _fit_norm(value):
            if not _is_number(value):
                return 0.35
            return max(0.0, min(1.0, (float(value) - fit_min) / fit_denom))

        line_pts = [_to_px(float(point["x"]), float(point["evo"])) for point in projected]
        if len(line_pts) >= 2:
            pg.draw.lines(self.screen, (68, 82, 108), False, line_pts, 1)

        for point in projected:
            px, py = _to_px(float(point["x"]), float(point["evo"]))
            n = _fit_norm(point.get("fitness"))
            radius = 4 + int(round(8.0 * n))
            if point.get("actual"):
                color = (40 + int(70 * n), 170 + int(70 * n), 215 + int(40 * n))
                pg.draw.circle(self.screen, color, (px, py), radius)
            else:
                color = (255, 145 + int(70 * n), 80 + int(70 * n))
                pg.draw.circle(self.screen, color, (px, py), max(3, radius - 1), 1)

        min_label = self.small.render(f"{min_y:.1f}", True, (150, 150, 150))
        max_label = self.small.render(f"{max_y:.1f}", True, (150, 150, 150))
        self.screen.blit(max_label, (plot.x - 40, plot.y - 6))
        self.screen.blit(min_label, (plot.x - 40, plot.bottom - 8))
        x1 = self.small.render(f"{min_x:.2f}", True, (150, 150, 150))
        x2 = self.small.render(f"{max_x:.2f}", True, (150, 150, 150))
        self.screen.blit(x1, (plot.x, plot.bottom + 4))
        self.screen.blit(x2, (plot.right - x2.get_width(), plot.bottom + 4))
        x_label = self.small.render("env change rate", True, (155, 155, 155))
        y_label = self.small.render("evo speed", True, (155, 155, 155))
        self.screen.blit(x_label, (plot.x + 4, plot.bottom + 22))
        self.screen.blit(y_label, (plot.x - 42, plot.y + 8))

    def update(self, state: dict, force: bool = False) -> None:
        if not self.enabled:
            return
        self._rows_cache = list(state.get("rows", []))
        if self._rows_cache:
            self.selected_row_index = max(
                0, min(int(self.selected_row_index), len(self._rows_cache) - 1)
            )
        else:
            self.selected_row_index = 0
        self._pump_events()
        if not self.enabled:
            return
        now = time.time()
        if (not force) and ((now - self._last_draw) < self._refresh_s):
            return
        self._last_draw = now
        pg = self.pg
        self.screen.fill((12, 13, 17))

        rows = self._rows_cache
        pred = _runtime_prediction(rows, float(state.get("start_ts", now)), now)
        elapsed = _fmt_duration(pred.get("elapsed_s"))
        remain = _fmt_duration(pred.get("remaining_s"))
        total_pred = _fmt_duration(pred.get("predicted_total_s"))
        eta = _fmt_eta(pred.get("eta_ts"))
        completed = int(pred.get("done_steps", 0))
        total = int(pred.get("total_steps", 0))
        running_row = state.get("running_row")
        status = str(state.get("status", "running"))
        fps_mode_label = {
            _FPS_MODE_CAPPED: f"CAPPED({self.capped_fps})",
            _FPS_MODE_UNCAPPED: f"UNCAPPED({self.uncapped_fps})",
            _FPS_MODE_FULL_THROTTLE: "FULL",
        }.get(self.fps_mode, "CAPPED")
        header1 = self.title.render(
            f"HUB {self.hub_idx} Dashboard    status: {status.upper()}",
            True,
            (230, 230, 230),
        )
        self.screen.blit(header1, (20, 16))
        header2 = self.font.render(
            f"Hub dir: {self.hub_dir}    planned master ids: {self.planned_master_span}",
            True,
            (180, 180, 180),
        )
        self.screen.blit(header2, (20, 48))
        header3 = self.font.render(
            f"Elapsed: {elapsed}    Pred total: {total_pred}    Remaining: {remain}    ETA: {eta}",
            True,
            (170, 220, 180),
        )
        self.screen.blit(header3, (20, 74))
        header4 = self.font.render(
            f"Completed: {completed}/{total}    Species threshold: {self.species_threshold}    Running step: {running_row if running_row is not None else '-'}    FPS mode: {fps_mode_label} (F)",
            True,
            (170, 200, 230),
        )
        self.screen.blit(header4, (20, 100))
        self._shutdown_button_rect = pg.Rect(self.window_w - 188, 96, 168, 30)
        pg.draw.rect(self.screen, (110, 50, 50), self._shutdown_button_rect)
        pg.draw.rect(self.screen, (180, 160, 160), self._shutdown_button_rect, 1)
        close_hub_text = self.small.render("Close Hub (X)", True, (235, 235, 235))
        self.screen.blit(
            close_hub_text,
            (
                self._shutdown_button_rect.x
                + (self._shutdown_button_rect.width - close_hub_text.get_width()) // 2,
                self._shutdown_button_rect.y + 7,
            ),
        )

        left_margin = 20
        top_y = 132
        right_margin = 20
        mid_gap = 16
        min_right_w = 380
        right_w = max(min_right_w, int(self.window_w * 0.30))
        right_x = self.window_w - right_margin - right_w
        table_w = max(560, right_x - left_margin - mid_gap)
        table_h = max(320, self.window_h - top_y - 16)
        table_rect = pg.Rect(left_margin, top_y, table_w, table_h)
        self._table_rect = table_rect
        pg.draw.rect(self.screen, (18, 20, 25), table_rect)
        pg.draw.rect(self.screen, (70, 74, 86), table_rect, 1)
        cols = [
            ("step", 50),
            ("rate", 72),
            ("plan", 72),
            ("master", 72),
            ("status", 90),
            ("species", 96),
            ("fitness", 86),
            ("evo", 74),
            ("dur", 66),
        ]
        x = table_rect.x + 8
        y = table_rect.y + 8
        for name, width in cols:
            surf = self.small.render(name, True, (190, 190, 190))
            self.screen.blit(surf, (x, y))
            x += width
        pg.draw.line(self.screen, (58, 60, 70), (table_rect.x + 6, y + 20), (table_rect.right - 6, y + 20), 1)

        row_h = 20
        max_rows = max(1, (table_rect.height - 36) // row_h)
        total_rows = len(rows)
        max_scroll = max(0, total_rows - max_rows)
        self._set_scroll_target(self.scroll_target, max_scroll)
        self.scroll += (self.scroll_target - self.scroll) * 0.35
        if abs(self.scroll_target - self.scroll) < 0.05:
            self.scroll = self.scroll_target
        self.scroll = max(0.0, min(float(max_scroll), float(self.scroll)))
        # Keep sub-row precision so smooth scroll can still resolve to index 0.
        # Rounding here can trap at index 1 when target is 0.
        scroll_i = int(self.scroll)
        visible = rows[scroll_i : scroll_i + max_rows]
        base_y = y + 24
        self._row_h = row_h
        self._rows_base_y = base_y
        self._scroll_i = scroll_i
        self._visible_rows = len(visible)

        for idx, row in enumerate(visible):
            row_idx = scroll_i + idx
            ry = base_y + (idx * row_h)
            if idx % 2 == 0:
                pg.draw.rect(self.screen, (15, 17, 22), (table_rect.x + 4, ry - 1, table_rect.width - 8, row_h))
            if row_idx == int(self.selected_row_index):
                pg.draw.rect(
                    self.screen,
                    (34, 52, 72),
                    (table_rect.x + 3, ry - 1, table_rect.width - 6, row_h),
                )
            status_text = str(row.get("status", "pending"))
            color = {
                "ok": (150, 230, 160),
                "failed": (255, 140, 140),
                "running": (255, 220, 120),
                "pending": (170, 170, 170),
                "no_master": (255, 160, 120),
                "stopped": (255, 180, 120),
                "aborted": (255, 130, 130),
            }.get(status_text, (200, 200, 200))
            dur = row.get("duration_s")
            if status_text == "running" and _is_number(row.get("started_at")):
                dur = max(0.0, now - float(row["started_at"]))
            values = [
                str(int(row.get("step_index", 0)) + 1),
                f"{float(row.get('env_rate', 0.0)):.2f}" if _is_number(row.get("env_rate")) else "",
                "" if row.get("planned_master_run_num") is None else str(row.get("planned_master_run_num")),
                "" if row.get("master_run_num") is None else str(row.get("master_run_num")),
                status_text,
                f"{float(row.get('max_species')):.1f}" if _is_number(row.get("max_species")) else "",
                f"{float(row.get('apex_fitness')):.1f}" if _is_number(row.get("apex_fitness")) else "",
                f"{float(row.get('apex_evolution_rate')):.3f}" if _is_number(row.get("apex_evolution_rate")) else "",
                _fmt_duration(dur),
            ]
            x = table_rect.x + 8
            for c_idx, (_, width) in enumerate(cols):
                text = self.small.render(values[c_idx], True, color if c_idx == 4 else (205, 205, 205))
                self.screen.blit(text, (x, ry))
                x += width

        scroll_info = self.small.render(
            f"rows {scroll_i + 1}-{scroll_i + len(visible)} / {total_rows} (wheel/up/down/page/home/end)",
            True,
            (150, 150, 150),
        )
        self.screen.blit(scroll_info, (table_rect.x + 8, table_rect.bottom - 22))
        if total_rows > max_rows:
            bar_x = table_rect.right - 8
            bar_y = table_rect.y + 28
            bar_h = table_rect.height - 56
            self.pg.draw.rect(self.screen, (44, 47, 56), (bar_x, bar_y, 4, bar_h))
            thumb_h = max(18, int((max_rows / max(1, total_rows)) * bar_h))
            top_ratio = scroll_i / max(1, max_scroll)
            thumb_y = bar_y + int((bar_h - thumb_h) * top_ratio)
            self.pg.draw.rect(self.screen, (120, 130, 150), (bar_x - 1, thumb_y, 6, thumb_h))

        right_x = table_rect.right + mid_gap
        right_w = max(320, self.window_w - right_x - right_margin)
        right_h = table_rect.height
        graph_h = max(240, int(right_h * 0.62))
        info_h = max(140, right_h - graph_h - 12)
        graph_rect = pg.Rect(right_x, top_y, right_w, graph_h)
        self._draw_graph(graph_rect, rows)

        info_rect = pg.Rect(right_x, graph_rect.bottom + 12, right_w, info_h)
        pg.draw.rect(self.screen, (20, 22, 27), info_rect)
        pg.draw.rect(self.screen, (70, 74, 86), info_rect, 1)
        info_title = self.font.render("Other Information", True, (220, 220, 220))
        self.screen.blit(info_title, (info_rect.x + 10, info_rect.y + 10))
        selected_row = self._selected_row()
        selected_step = "-"
        selected_status = "-"
        selected_master = "-"
        selected_env = "-"
        can_reopen = False
        can_close = False
        if isinstance(selected_row, dict):
            selected_step = str(int(selected_row.get("step_index", 0)) + 1)
            selected_status = str(selected_row.get("status", "pending"))
            master_id = selected_row.get("master_run_num")
            if master_id is not None:
                selected_master = f"master_{master_id}"
            env_dir = selected_row.get("env_dir")
            if env_dir:
                selected_env = str(env_dir)
            can_reopen = (
                master_id is not None
                and bool(selected_row.get("env_dir"))
            )
            can_close = bool(selected_row.get("reopen_open"))
        info_lines = [
            f"Current rate: {state.get('current_rate', '--')}",
            f"Current planned master: {state.get('current_planned_master', '--')}",
            f"Last completed master: {state.get('last_master', '--')}",
            f"Selected row: {selected_step}  status: {selected_status}",
            f"Selected master: {selected_master}",
            f"Selected env: {selected_env}",
            f"Summary file: {state.get('summary_path', '')}",
        ]
        yy = info_rect.y + 42
        for line in info_lines:
            surf = self.small.render(str(line), True, (185, 185, 185))
            self.screen.blit(surf, (info_rect.x + 10, yy))
            yy += 24

        button_w = max(120, (info_rect.width - 30) // 2)
        self._reopen_button_rect = self.pg.Rect(
            info_rect.x + 10,
            info_rect.bottom - 42,
            button_w,
            30,
        )
        self._close_button_rect = self.pg.Rect(
            self._reopen_button_rect.right + 10,
            self._reopen_button_rect.y,
            button_w,
            30,
        )
        btn_bg = (40, 90, 60) if can_reopen else (42, 42, 46)
        btn_fg = (230, 230, 230) if can_reopen else (150, 150, 150)
        pg.draw.rect(self.screen, btn_bg, self._reopen_button_rect)
        pg.draw.rect(self.screen, (150, 150, 150), self._reopen_button_rect, 1)
        btn_text = self.small.render("Reopen (R)", True, btn_fg)
        self.screen.blit(
            btn_text,
            (
                self._reopen_button_rect.x
                + (self._reopen_button_rect.width - btn_text.get_width()) // 2,
                self._reopen_button_rect.y + 7,
            ),
        )
        close_bg = (110, 60, 60) if can_close else (42, 42, 46)
        close_fg = (230, 230, 230) if can_close else (150, 150, 150)
        pg.draw.rect(self.screen, close_bg, self._close_button_rect)
        pg.draw.rect(self.screen, (150, 150, 150), self._close_button_rect, 1)
        close_text = self.small.render("Close (C)", True, close_fg)
        self.screen.blit(
            close_text,
            (
                self._close_button_rect.x
                + (self._close_button_rect.width - close_text.get_width()) // 2,
                self._close_button_rect.y + 7,
            ),
        )

        pg.display.flip()
        self.clock.tick(self.draw_fps)


def main() -> None:
    args = _parse_args()
    defaults = _hub_defaults_from_settings()
    results_root = Path(args.results_root)
    results_root.mkdir(parents=True, exist_ok=True)
    selector_choice = None
    should_show_selector = (args.continue_hub is None) and ((not args.no_screen) or args.hub_select)
    if should_show_selector:
        selector_choice = _select_hub_run_ui(results_root)
        if selector_choice is None:
            print("Hub selection canceled.")
            return

    continue_hub_idx = None
    if args.continue_hub is not None:
        try:
            continue_hub_idx = max(0, int(args.continue_hub))
        except Exception:
            raise SystemExit("Invalid --continue-hub value.")
    elif isinstance(selector_choice, dict) and selector_choice.get("mode") == "continue":
        try:
            continue_hub_idx = max(0, int(selector_choice.get("hub_idx")))
        except Exception:
            continue_hub_idx = None

    continuing = continue_hub_idx is not None
    existing_hub_meta = {}
    if continuing:
        hub_idx = int(continue_hub_idx)
        hub_dir = results_root / f"hub_{hub_idx}"
        if not hub_dir.is_dir():
            raise SystemExit(f"Hub not found: {hub_dir}")
        hub_meta_path = hub_dir / "hub_meta.json"
        if not hub_meta_path.exists():
            raise SystemExit(f"Missing hub_meta.json for continuation: {hub_meta_path}")
        try:
            payload = json.loads(hub_meta_path.read_text())
        except Exception:
            raise SystemExit(f"Failed to parse {hub_meta_path}")
        if not isinstance(payload, dict):
            raise SystemExit(f"Invalid hub meta format: {hub_meta_path}")
        existing_hub_meta = payload
        start_rate = float(existing_hub_meta.get("start_rate", defaults["start_rate"]))
        end_rate = float(existing_hub_meta.get("end_rate", defaults["end_rate"]))
        step = float(existing_hub_meta.get("step", defaults["step"]))
        species_threshold = int(
            max(
                0,
                int(
                    args.species_threshold
                    if args.species_threshold is not None
                    else existing_hub_meta.get("species_threshold", defaults["species_threshold"])
                ),
            )
        )
        max_masters = int(existing_hub_meta.get("max_masters", defaults["max_masters"]))
        raw_rates = existing_hub_meta.get("rates")
        rates = []
        if isinstance(raw_rates, list):
            for val in raw_rates:
                try:
                    rates.append(float(val))
                except Exception:
                    continue
        if not rates:
            rates = _rate_values(start_rate, end_rate, step)
            if max_masters > 0:
                rates = rates[:max_masters]
    else:
        start_rate = float(args.start_rate if args.start_rate is not None else defaults["start_rate"])
        end_rate = float(args.end_rate if args.end_rate is not None else defaults["end_rate"])
        step = float(args.step if args.step is not None else defaults["step"])
        species_threshold = int(
            max(0, int(args.species_threshold if args.species_threshold is not None else defaults["species_threshold"]))
        )
        max_masters = int(args.max_masters if args.max_masters is not None else defaults["max_masters"])
        rates = _rate_values(start_rate, end_rate, step)
        if max_masters > 0:
            rates = rates[:max_masters]
        hub_idx = _allocate_hub_index(results_root)
        hub_dir = results_root / f"hub_{hub_idx}"
        hub_dir.mkdir(parents=True, exist_ok=True)

    if not rates:
        raise SystemExit("No rates to run.")

    hub_meta_path = hub_dir / "hub_meta.json"
    hub_summary_path = hub_dir / "hub_summary.csv"
    fit_csv_path = hub_dir / "hub_fit_equations.csv"
    settings_snapshot = load_settings()
    try:
        master_cursor = int(settings_snapshot.get("num_tries_master", 0))
    except Exception:
        master_cursor = 0
    existing_master_ids = _collect_existing_master_ids(results_root)
    default_root = Path("results")
    try:
        same_root = default_root.resolve() == results_root.resolve()
    except Exception:
        same_root = str(default_root) == str(results_root)
    if not same_root:
        existing_master_ids.update(_collect_existing_master_ids(default_root))
    planned_master_ids = []
    raw_planned = existing_hub_meta.get("planned_master_ids") if continuing else None
    if isinstance(raw_planned, list):
        for idx in range(len(rates)):
            val = raw_planned[idx] if idx < len(raw_planned) else None
            try:
                planned_id = int(val)
            except Exception:
                planned_id = None
            planned_master_ids.append(planned_id)
    else:
        planned_master_ids = [None] * len(rates)
    if continuing:
        steps = existing_hub_meta.get("steps")
        if isinstance(steps, list):
            for step_info in steps:
                if not isinstance(step_info, dict):
                    continue
                try:
                    step_idx = int(step_info.get("step_index"))
                except Exception:
                    continue
                if step_idx < 0 or step_idx >= len(rates):
                    continue
                try:
                    planned_id = int(step_info.get("planned_master_run_num"))
                except Exception:
                    continue
                if planned_master_ids[step_idx] is None:
                    planned_master_ids[step_idx] = planned_id
    used_master_ids = set(int(v) for v in existing_master_ids)
    used_master_ids.update(int(v) for v in planned_master_ids if isinstance(v, int))
    missing_count = sum(1 for v in planned_master_ids if v is None)
    if missing_count > 0:
        filled = _plan_master_ids(master_cursor, missing_count, used_master_ids)
        fill_i = 0
        for idx in range(len(planned_master_ids)):
            if planned_master_ids[idx] is None and fill_i < len(filled):
                planned_master_ids[idx] = int(filled[fill_i])
                fill_i += 1
    planned_master_ids = [
        (None if v is None else int(v))
        for v in planned_master_ids
    ]
    planned_master_span = _format_id_span([v for v in planned_master_ids if isinstance(v, int)])
    if continuing:
        hub_meta = dict(existing_hub_meta)
        hub_meta["hub_index"] = int(hub_idx)
        hub_meta["start_rate"] = float(start_rate)
        hub_meta["end_rate"] = float(end_rate)
        hub_meta["step"] = float(step)
        hub_meta["species_threshold"] = int(species_threshold)
        hub_meta["max_masters"] = int(max_masters)
        hub_meta["rates"] = rates
        hub_meta["planned_master_ids"] = planned_master_ids
        hub_meta["planned_master_range"] = planned_master_span
        if not isinstance(hub_meta.get("steps"), list):
            hub_meta["steps"] = []
        if not _is_number(hub_meta.get("created_at")):
            hub_meta["created_at"] = time.time()
        hub_meta["status"] = "running"
        hub_meta["resumed_at"] = time.time()
        try:
            hub_meta["resume_count"] = int(hub_meta.get("resume_count", 0)) + 1
        except Exception:
            hub_meta["resume_count"] = 1
    else:
        hub_meta = {
            "hub_index": int(hub_idx),
            "created_at": time.time(),
            "start_rate": float(start_rate),
            "end_rate": float(end_rate),
            "step": float(step),
            "species_threshold": int(species_threshold),
            "max_masters": int(max_masters),
            "rates": rates,
            "planned_master_ids": planned_master_ids,
            "planned_master_range": planned_master_span,
            "steps": [],
            "status": "running",
        }
    hub_meta_path.write_text(json.dumps(hub_meta, indent=2))
    _ensure_csv_with_header(
        hub_summary_path,
        "enviorment change rate,planned master run,master run,apex evolution rate,fitness,max species,fit sigma left,fit sigma right,fit r2\n",
    )
    _ensure_csv_with_header(
        fit_csv_path,
        "enviorment change rate,master run,apex x,apex y,sigma left,sigma right,r2,equation\n",
    )

    repo_root = Path(__file__).resolve().parent
    master_script = repo_root / "master_simulations.py"
    interpreter = sys.executable
    reopened_master_procs = {}
    current_step_proc = None
    abort_requested = False

    def _step_row_for_master(master_run_num: int):
        for item in step_rows:
            if item.get("master_run_num") == master_run_num:
                return item
        return None

    def _terminate_process(proc: subprocess.Popen, label: str) -> None:
        if proc is None:
            return
        if proc.poll() is not None:
            return
        try:
            print(f"[hub_{hub_idx}] terminating {label} (pid={proc.pid})")
        except Exception:
            pass
        try:
            proc.terminate()
            proc.wait(timeout=2.0)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def _close_all_reopened() -> None:
        for master_num, proc in list(reopened_master_procs.items()):
            _terminate_process(proc, f"reopened master_{int(master_num)}")
            row = _step_row_for_master(master_num)
            if isinstance(row, dict):
                row["reopen_open"] = False
                row["reopen_pid"] = None
        reopened_master_procs.clear()

    def _refresh_reopened_processes() -> None:
        stale = []
        for master_num, proc in reopened_master_procs.items():
            if proc.poll() is None:
                row = _step_row_for_master(master_num)
                if isinstance(row, dict):
                    row["reopen_open"] = True
                    row["reopen_pid"] = int(proc.pid)
            else:
                stale.append(master_num)
                row = _step_row_for_master(master_num)
                if isinstance(row, dict):
                    row["reopen_open"] = False
                    row["reopen_pid"] = None
        for key in stale:
            reopened_master_procs.pop(key, None)

    def _reopen_master_row(row: dict) -> None:
        if not isinstance(row, dict):
            return
        master_run = row.get("master_run_num")
        env_dir = row.get("env_dir")
        if master_run is None or not env_dir:
            return
        cmd = [
            interpreter,
            str(master_script),
            "--results-dir",
            str(env_dir),
            "--continue-master-run",
            str(int(master_run)),
        ]
        master_dir = row.get("master_dir")
        if master_dir:
            cmd.extend(["--continue-master-dir", str(master_dir)])
        existing = reopened_master_procs.get(int(master_run))
        if existing is not None and existing.poll() is None:
            print(
                f"[hub_{hub_idx}] master_{int(master_run)} already open (pid={existing.pid})"
            )
            row["reopen_open"] = True
            row["reopen_pid"] = int(existing.pid)
            return
        print(
            f"[hub_{hub_idx}] reopen requested for master_{int(master_run)} @ {env_dir}"
        )
        proc = subprocess.Popen(cmd, cwd=str(repo_root))
        reopened_master_procs[int(master_run)] = proc
        row["reopen_open"] = True
        row["reopen_pid"] = int(proc.pid)

    def _close_master_row(row: dict) -> None:
        if not isinstance(row, dict):
            return
        master_run = row.get("master_run_num")
        if master_run is None:
            return
        proc = reopened_master_procs.get(int(master_run))
        if proc is None:
            row["reopen_open"] = False
            row["reopen_pid"] = None
            return
        _terminate_process(proc, f"master_{int(master_run)}")
        reopened_master_procs.pop(int(master_run), None)
        row["reopen_open"] = False
        row["reopen_pid"] = None

    def _shutdown_hub_run() -> None:
        nonlocal abort_requested, current_step_proc
        if abort_requested:
            return
        abort_requested = True
        print(f"[hub_{hub_idx}] close hub requested")
        _close_all_reopened()
        if current_step_proc is not None:
            _terminate_process(current_step_proc, "active hub step")

    hub_rows = []
    step_rows = []
    for idx, rate in enumerate(rates):
        env_dir = hub_dir / _rate_label(rate)
        row = {
            "step_index": int(idx),
            "env_rate": float(rate),
            "planned_master_run_num": (
                planned_master_ids[idx] if idx < len(planned_master_ids) else None
            ),
            "master_run_num": None,
            "status": "pending",
            "max_species": None,
            "apex_fitness": None,
            "apex_evolution_rate": None,
            "started_at": None,
            "finished_at": None,
            "duration_s": None,
            "env_dir": str(env_dir),
            "master_dir": None,
            "reopen_open": False,
            "reopen_pid": None,
        }
        if continuing and env_dir.is_dir():
            master_dir = _latest_master_dir(env_dir)
            if master_dir is not None:
                run_nums = _master_run_nums(master_dir)
                max_species = _max_species(env_dir, run_nums)
                points = _master_points(master_dir, run_nums)
                fit = _fit_stitched_gaussian(points)
                if fit is None:
                    apex_x, apex_y = (None, None)
                else:
                    apex_x = fit["apex_x"]
                    apex_y = fit["apex_y"]
                try:
                    master_run_num = int(master_dir.name.split("_", 1)[1])
                except Exception:
                    master_run_num = None
                row["status"] = "ok"
                row["master_run_num"] = master_run_num
                row["master_dir"] = str(master_dir)
                row["max_species"] = max_species
                row["apex_evolution_rate"] = apex_x
                row["apex_fitness"] = apex_y
                if row["planned_master_run_num"] is None and master_run_num is not None:
                    row["planned_master_run_num"] = int(master_run_num)
                hub_rows.append(
                    {
                        "env_rate": float(rate),
                        "apex_x": apex_x,
                        "apex_y": apex_y,
                        "fit": fit,
                        "points": points,
                    }
                )
        step_rows.append(row)
    dashboard = _HubDashboard(
        enabled=(not args.no_screen),
        hub_idx=hub_idx,
        hub_dir=hub_dir,
        planned_master_span=planned_master_span,
        species_threshold=species_threshold,
        reopen_callback=_reopen_master_row,
        close_callback=_close_master_row,
        shutdown_callback=_shutdown_hub_run,
    )
    dash_state = {
        "start_ts": float(hub_meta["created_at"]),
        "rows": step_rows,
        "status": hub_meta["status"],
        "running_row": None,
        "current_rate": "--",
        "current_planned_master": "--",
        "last_master": "--",
        "summary_path": str(hub_summary_path),
        "fit_path": str(fit_csv_path),
    }
    if continuing:
        print(f"[hub_{hub_idx}] continuing hub run at {hub_dir}")
    else:
        print(f"[hub_{hub_idx}] creating hub run at {hub_dir}")
    print(
        f"[hub_{hub_idx}] planned master ids for this hub: {planned_master_span}"
    )
    completed_rows = [row for row in step_rows if str(row.get("status", "")) == "ok"]
    pending_rows = [row for row in step_rows if str(row.get("status", "")) != "ok"]
    if completed_rows:
        last_done = sorted(
            completed_rows,
            key=lambda row: int(row.get("step_index", -1)),
        )[-1]
        master_num = last_done.get("master_run_num")
        if master_num is not None:
            dash_state["last_master"] = f"master_{int(master_num)}"
    if continuing:
        print(
            f"[hub_{hub_idx}] continuation progress: {len(completed_rows)}/{len(step_rows)} "
            f"completed, {len(pending_rows)} pending"
        )
    _refresh_reopened_processes()
    dashboard.update(dash_state, force=True)

    for step_idx, rate in enumerate(rates):
        if abort_requested:
            break
        row_ref = step_rows[step_idx]
        if str(row_ref.get("status", "")) == "ok":
            continue
        row_ref["status"] = "running"
        row_ref["started_at"] = time.time()
        dash_state["status"] = "running"
        dash_state["running_row"] = int(step_idx) + 1
        dash_state["current_rate"] = f"{float(rate):.2f}"
        env_dir = hub_dir / _rate_label(rate)
        env_dir.mkdir(parents=True, exist_ok=True)
        row_ref["env_dir"] = str(env_dir)
        planned_master_run = (
            planned_master_ids[step_idx] if step_idx < len(planned_master_ids) else None
        )
        row_ref["planned_master_run_num"] = planned_master_run
        dash_state["current_planned_master"] = (
            "--" if planned_master_run is None else str(int(planned_master_run))
        )
        cmd = [
            interpreter,
            str(master_script),
            "--non-interactive",
            "--results-dir",
            str(env_dir),
            "--env-change-rate",
            str(rate),
            "--species-stop",
            str(species_threshold),
            "--script",
            str(args.script),
        ]
        if planned_master_run is not None:
            cmd.extend(["--master-run-num", str(int(planned_master_run))])
        if args.count is not None:
            cmd.extend(["--count", str(int(args.count))])

        print(
            f"[hub_{hub_idx}] step {step_idx + 1}/{len(rates)} "
            f"rate={rate:.2f} planned_master={'' if planned_master_run is None else planned_master_run}"
        )
        proc = subprocess.Popen(cmd, cwd=str(repo_root))
        current_step_proc = proc
        while proc.poll() is None:
            if abort_requested:
                _terminate_process(proc, f"hub step {step_idx + 1}")
            _refresh_reopened_processes()
            dashboard.update(dash_state)
            sleep_s = dashboard.poll_sleep_seconds() if dashboard.enabled else 0.12
            if sleep_s > 0:
                time.sleep(sleep_s)
        returncode = int(proc.returncode if proc.returncode is not None else 1)
        current_step_proc = None
        row_ref["finished_at"] = time.time()
        if _is_number(row_ref.get("started_at")):
            row_ref["duration_s"] = max(
                0.0, float(row_ref["finished_at"]) - float(row_ref["started_at"])
            )

        step_info = {
            "step_index": int(step_idx),
            "env_rate": float(rate),
            "env_dir": str(env_dir),
            "planned_master_run_num": planned_master_run,
            "returncode": returncode,
            "finished_at": time.time(),
        }
        if abort_requested:
            step_info["status"] = "aborted"
            row_ref["status"] = "aborted"
            hub_meta["steps"].append(step_info)
            hub_meta["status"] = "aborted_by_user"
            hub_meta["aborted_at"] = time.time()
            dash_state["status"] = hub_meta["status"]
            dash_state["running_row"] = None
            hub_meta_path.write_text(json.dumps(hub_meta, indent=2))
            _refresh_reopened_processes()
            dashboard.update(dash_state, force=True)
            break
        if returncode != 0:
            step_info["status"] = "failed"
            row_ref["status"] = "failed"
            hub_meta["steps"].append(step_info)
            hub_meta["status"] = "failed"
            dash_state["status"] = hub_meta["status"]
            dash_state["running_row"] = None
            hub_meta_path.write_text(json.dumps(hub_meta, indent=2))
            _refresh_reopened_processes()
            dashboard.update(dash_state, force=True)
            raise SystemExit(returncode)

        master_dir = _latest_master_dir(env_dir)
        if master_dir is None:
            step_info["status"] = "no_master"
            row_ref["status"] = "no_master"
            hub_meta["steps"].append(step_info)
            hub_meta["status"] = "stopped_no_master"
            dash_state["status"] = hub_meta["status"]
            dash_state["running_row"] = None
            hub_meta_path.write_text(json.dumps(hub_meta, indent=2))
            _refresh_reopened_processes()
            dashboard.update(dash_state, force=True)
            break

        run_nums = _master_run_nums(master_dir)
        max_species = _max_species(env_dir, run_nums)
        points = _master_points(master_dir, run_nums)
        fit = _fit_stitched_gaussian(points)
        if fit is None:
            apex_x, apex_y = (None, None)
        else:
            apex_x = fit["apex_x"]
            apex_y = fit["apex_y"]
        try:
            master_run_num = int(master_dir.name.split("_", 1)[1])
        except Exception:
            master_run_num = None

        with open(hub_summary_path, "a", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    rate,
                    planned_master_run if planned_master_run is not None else "",
                    master_run_num if master_run_num is not None else "",
                    apex_x if apex_x is not None else "",
                    apex_y if apex_y is not None else "",
                    max_species if max_species is not None else "",
                    (fit["sigma_left"] if fit else ""),
                    (fit["sigma_right"] if fit else ""),
                    (fit["r2"] if fit else ""),
                ]
            )

        if fit:
            with open(fit_csv_path, "a", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        rate,
                        master_run_num if master_run_num is not None else "",
                        fit["apex_x"],
                        fit["apex_y"],
                        fit["sigma_left"],
                        fit["sigma_right"],
                        fit["r2"] if fit["r2"] is not None else "",
                        fit["equation"],
                    ]
                )

        step_info.update(
            {
                "status": "ok",
                "master_dir": str(master_dir),
                "master_run_num": master_run_num,
                "planned_master_run_num": planned_master_run,
                "run_nums": run_nums,
                "max_species": max_species,
                "apex_evolution_rate": apex_x,
                "apex_fitness": apex_y,
                "fit": fit,
                "point_count": len(points),
            }
        )
        row_ref["status"] = "ok"
        row_ref["master_run_num"] = master_run_num
        row_ref["master_dir"] = str(master_dir)
        row_ref["max_species"] = max_species
        row_ref["apex_evolution_rate"] = apex_x
        row_ref["apex_fitness"] = apex_y
        dash_state["last_master"] = (
            "--" if master_run_num is None else f"master_{int(master_run_num)}"
        )
        dash_state["running_row"] = None
        if (
            planned_master_run is not None
            and master_run_num is not None
            and int(planned_master_run) != int(master_run_num)
        ):
            step_info["master_run_mismatch"] = True
            step_info["master_run_mismatch_note"] = (
                f"expected master_{planned_master_run}, got master_{master_run_num}"
            )
        hub_meta["steps"].append(step_info)
        hub_meta_path.write_text(json.dumps(hub_meta, indent=2))

        # Keep masters/runs discoverable by the existing master_simulations UI.
        _symlink_if_missing(results_root / master_dir.name, master_dir)
        for run_num in run_nums:
            run_dir = env_dir / str(run_num)
            if run_dir.exists():
                _symlink_if_missing(results_root / str(run_num), run_dir)

        hub_rows.append(
            {
                "env_rate": float(rate),
                "apex_x": apex_x,
                "apex_y": apex_y,
                "fit": fit,
                "points": points,
            }
        )

        if species_threshold > 0 and (max_species is None or max_species < species_threshold):
            hub_meta["status"] = "stopped_threshold_not_reached"
            row_ref["status"] = "stopped"
            dash_state["status"] = hub_meta["status"]
            hub_meta["stopped_step"] = int(step_idx)
            hub_meta_path.write_text(json.dumps(hub_meta, indent=2))
            _refresh_reopened_processes()
            dashboard.update(dash_state, force=True)
            break
        _refresh_reopened_processes()
        dashboard.update(dash_state, force=True)
    else:
        hub_meta["status"] = "completed"
        hub_meta["completed_at"] = time.time()
        dash_state["status"] = hub_meta["status"]
        hub_meta_path.write_text(json.dumps(hub_meta, indent=2))
        _refresh_reopened_processes()
        dashboard.update(dash_state, force=True)

    if abort_requested and str(hub_meta.get("status", "")) == "running":
        hub_meta["status"] = "aborted_by_user"
        hub_meta["aborted_at"] = time.time()
        dash_state["status"] = hub_meta["status"]
        dash_state["running_row"] = None
        hub_meta_path.write_text(json.dumps(hub_meta, indent=2))
        _refresh_reopened_processes()
        dashboard.update(dash_state, force=True)

    if (not args.skip_plots) and (not abort_requested):
        plotted = []
        scatter_path = hub_dir / "hub_apex_scatter.png"
        if _plot_hub_scatter(hub_rows, scatter_path):
            plotted.append(scatter_path.name)
        fits_path = hub_dir / "hub_stitched_fits.png"
        if _plot_stitched_fits(hub_rows, fits_path):
            plotted.append(fits_path.name)
        ratio_path = hub_dir / "hub_ratio_curve.png"
        if _plot_ratio_curve(hub_rows, ratio_path):
            plotted.append(ratio_path.name)
        if plotted:
            hub_meta["plots"] = plotted
            hub_meta_path.write_text(json.dumps(hub_meta, indent=2))
            dash_state["status"] = hub_meta["status"]
            _refresh_reopened_processes()
            dashboard.update(dash_state, force=True)

    if dashboard.enabled and float(args.screen_hold_seconds) > 0.0:
        hold_until = time.time() + max(0.0, float(args.screen_hold_seconds))
        while dashboard.enabled and time.time() < hold_until:
            _refresh_reopened_processes()
            dashboard.update(dash_state, force=True)
            time.sleep(0.1)

    if abort_requested:
        print(f"Hub aborted by user: {hub_dir}")
    else:
        print(f"Hub complete: {hub_dir}")
    print(f"Summary: {hub_summary_path}")
    print(f"Equations: {fit_csv_path}")


if __name__ == "__main__":
    main()
