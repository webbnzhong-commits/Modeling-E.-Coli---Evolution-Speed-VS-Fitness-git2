from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
from datetime import datetime
from pathlib import Path


ENV_COLUMNS = ("environment_change_rate", "enviorment change rate", "enviormentChangeRate")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export NHSJS-ready tables and figures from hub simulation results."
    )
    parser.add_argument(
        "--hub-dir",
        type=Path,
        default=None,
        help="Hub result folder to export, such as results/hub/hub_78. Defaults to latest hub.",
    )
    parser.add_argument(
        "--results-root",
        type=Path,
        default=Path("results/hub"),
        help="Folder containing hub_* result folders.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("paper_exports"),
        help="Folder where paper-ready outputs will be written.",
    )
    parser.add_argument(
        "--skip-plots",
        action="store_true",
        help="Only write CSV/JSON/README outputs; skip PNG figure generation.",
    )
    return parser.parse_args()


def _number(value) -> float | None:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    if math.isfinite(output):
        return output
    return None


def _pick(row: dict, names: tuple[str, ...]) -> str:
    for name in names:
        if name in row:
            return row.get(name, "")
    return ""


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def _latest_hub(results_root: Path) -> Path:
    hubs = []
    for path in results_root.glob("hub_*"):
        if not path.is_dir():
            continue
        try:
            hub_num = int(path.name.split("_", 1)[1])
        except (IndexError, ValueError):
            continue
        hubs.append((hub_num, path))
    if not hubs:
        raise FileNotFoundError(f"No hub_* folders found in {results_root}")
    return max(hubs, key=lambda item: item[0])[1]


def _mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _sample_stdev(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) >= 2 else 0.0


def _linear_fit(xs: list[float], ys: list[float]) -> dict:
    if len(xs) < 2 or len(xs) != len(ys):
        return {"slope": 0.0, "intercept": 0.0, "r": 0.0, "r2": 0.0}
    mean_x = _mean(xs)
    mean_y = _mean(ys)
    ss_x = sum((x - mean_x) ** 2 for x in xs)
    ss_y = sum((y - mean_y) ** 2 for y in ys)
    ss_xy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    slope = ss_xy / ss_x if ss_x else 0.0
    intercept = mean_y - slope * mean_x
    r = ss_xy / math.sqrt(ss_x * ss_y) if ss_x and ss_y else 0.0
    return {"slope": slope, "intercept": intercept, "r": r, "r2": r * r}


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _load_summary_rows(hub_dir: Path) -> list[dict]:
    source = hub_dir / "hub_summary.csv"
    rows = []
    for row in _read_csv(source):
        env = _number(_pick(row, ENV_COLUMNS))
        apex = _number(row.get("apex evolution rate"))
        fitness = _number(row.get("fitness"))
        max_species = _number(row.get("max species"))
        fit_r2 = _number(row.get("fit r2"))
        master_run = row.get("master run") or row.get("planned master run") or ""
        if env is None or apex is None or fitness is None:
            continue
        rows.append(
            {
                "environment_change_rate": env,
                "apex_evolution_rate": apex,
                "fitness": fitness,
                "max_species": max_species if max_species is not None else "",
                "master_run": master_run,
                "fit_r2": fit_r2 if fit_r2 is not None else "",
            }
        )
    rows.sort(key=lambda item: item["environment_change_rate"])
    return rows


def _summarize_by_environment(all_points_path: Path) -> list[dict]:
    grouped: dict[float, list[tuple[float, float]]] = {}
    for row in _read_csv(all_points_path):
        env = _number(_pick(row, ENV_COLUMNS))
        evo = _number(row.get("evo rate") or row.get("evolution rate"))
        fitness = _number(row.get("fitness"))
        if env is None or evo is None or fitness is None:
            continue
        grouped.setdefault(env, []).append((evo, fitness))

    rows = []
    for env, points in sorted(grouped.items()):
        fitness_values = [fitness for _, fitness in points]
        best_evo, best_fitness = max(points, key=lambda item: item[1])
        rows.append(
            {
                "environment_change_rate": env,
                "point_count": len(points),
                "mean_fitness": _mean(fitness_values),
                "fitness_stdev": _sample_stdev(fitness_values),
                "best_evolution_rate": best_evo,
                "best_fitness": best_fitness,
            }
        )
    return rows


def _write_methods_json(
    out_dir: Path,
    hub_dir: Path,
    summary_rows: list[dict],
    env_rows: list[dict],
) -> None:
    xs = [float(row["environment_change_rate"]) for row in summary_rows]
    apex = [float(row["apex_evolution_rate"]) for row in summary_rows]
    fitness = [float(row["fitness"]) for row in summary_rows]
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_hub_dir": str(hub_dir),
        "source_files": {
            "hub_summary": str(hub_dir / "hub_summary.csv"),
            "hub_all_points": str(hub_dir / "hub_all_points.csv"),
            "hub_fit_equations": str(hub_dir / "hub_fit_equations.csv"),
            "hub_stats": str(hub_dir / "hub_stats.csv"),
        },
        "row_counts": {
            "environment_summary_rows": len(summary_rows),
            "binned_environment_rows": len(env_rows),
        },
        "main_relationships": {
            "environment_change_rate_vs_apex_evolution_rate": _linear_fit(xs, apex),
            "environment_change_rate_vs_peak_fitness": _linear_fit(xs, fitness),
        },
        "settings": _load_json(Path("settings.json")),
    }
    (out_dir / "paper_methods.json").write_text(json.dumps(payload, indent=2))


def _load_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text())
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _write_readme(out_dir: Path, hub_dir: Path) -> None:
    text = f"""# Paper Export

Source hub: `{hub_dir}`

Files:
- `paper_summary.csv`: one row per environment change rate, using the fitted apex evolution rate and peak fitness.
- `environment_bins.csv`: descriptive statistics from all binned fitness points.
- `paper_methods.json`: reproducibility metadata, settings snapshot, and simple linear relationship checks.
- `figure_apex_vs_environment.png`: environment change rate vs. apex evolution rate.
- `figure_fitness_vs_environment.png`: environment change rate vs. peak fitness.

Suggested Methods wording:
Simulation outputs were aggregated by environment change rate. For each environment, the evolution rate with the highest fitted survival/fitness value was recorded as the apex evolution rate. Summary tables and figures were generated from the hub-level CSV outputs.
"""
    (out_dir / "README.md").write_text(text)


def _write_plots(out_dir: Path, summary_rows: list[dict]) -> None:
    mpl_cache_dir = Path(".matplotlib_cache")
    mpl_cache_dir.mkdir(exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_cache_dir.resolve()))
    try:
        import matplotlib.pyplot as plt  # pylint: disable=import-outside-toplevel
    except Exception:
        print("matplotlib is not installed; skipping PNG figure generation.")
        return

    env = [float(row["environment_change_rate"]) for row in summary_rows]
    apex = [float(row["apex_evolution_rate"]) for row in summary_rows]
    fitness = [float(row["fitness"]) for row in summary_rows]

    _scatter_plot(
        out_dir / "figure_apex_vs_environment.png",
        env,
        apex,
        "Environment change rate",
        "Apex evolution rate",
        "Apex evolution rate by environment change rate",
        plt,
    )
    _scatter_plot(
        out_dir / "figure_fitness_vs_environment.png",
        env,
        fitness,
        "Environment change rate",
        "Peak fitness",
        "Peak fitness by environment change rate",
        plt,
    )


def _scatter_plot(
    path: Path,
    xs: list[float],
    ys: list[float],
    xlabel: str,
    ylabel: str,
    title: str,
    plt,
) -> None:
    fit = _linear_fit(xs, ys)
    predicted = [fit["intercept"] + fit["slope"] * x for x in xs]
    fig, ax = plt.subplots(figsize=(7.0, 4.8), dpi=200)
    ax.scatter(xs, ys, s=28, color="#1f77b4", alpha=0.86, label="simulation result")
    if len(xs) >= 2:
        ax.plot(xs, predicted, color="#d62728", linewidth=1.8, label=f"linear fit, R^2={fit['r2']:.3f}")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def main() -> int:
    args = _parse_args()
    hub_dir = args.hub_dir if args.hub_dir is not None else _latest_hub(args.results_root)
    if not hub_dir.exists():
        raise FileNotFoundError(f"Hub directory does not exist: {hub_dir}")

    export_dir = args.out_dir / hub_dir.name
    export_dir.mkdir(parents=True, exist_ok=True)

    summary_rows = _load_summary_rows(hub_dir)
    if not summary_rows:
        raise ValueError(f"No usable rows found in {hub_dir / 'hub_summary.csv'}")

    env_rows = _summarize_by_environment(hub_dir / "hub_all_points.csv")

    _write_csv(
        export_dir / "paper_summary.csv",
        summary_rows,
        [
            "environment_change_rate",
            "apex_evolution_rate",
            "fitness",
            "max_species",
            "master_run",
            "fit_r2",
        ],
    )
    _write_csv(
        export_dir / "environment_bins.csv",
        env_rows,
        [
            "environment_change_rate",
            "point_count",
            "mean_fitness",
            "fitness_stdev",
            "best_evolution_rate",
            "best_fitness",
        ],
    )
    _write_methods_json(export_dir, hub_dir, summary_rows, env_rows)
    _write_readme(export_dir, hub_dir)
    if not args.skip_plots:
        _write_plots(export_dir, summary_rows)

    print(f"Wrote paper export to {export_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
