from __future__ import annotations

import csv
import json
import math
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable, Tuple
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

_LA_TZ_NAME = "America/Los_Angeles"
if ZoneInfo is not None:
    try:
        _LA_TZ = ZoneInfo(_LA_TZ_NAME)
    except Exception:
        _LA_TZ = timezone(timedelta(hours=-8), "PST")
else:
    _LA_TZ = timezone(timedelta(hours=-8), "PST")


def _format_wall_time_la(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=_LA_TZ).strftime("%Y-%m-%d %I:%M:%S %p %Z")

try:
    import numpy as np  # type: ignore
    _HAS_NUMPY = True
except Exception:
    np = None
    _HAS_NUMPY = False

try:
    from scipy.stats import pearsonr as _scipy_pearsonr  # type: ignore
    _HAS_SCIPY = True
except Exception:
    _scipy_pearsonr = None
    _HAS_SCIPY = False


def _part_index(filename: str) -> int:
    match = re.search(r"_part(\\d+)\\.csv$", filename)
    return int(match.group(1)) if match else 0


def _geometric_mean(values: Iterable[float], scale: float = 100.0) -> float:
    vals = [v for v in values if v > 0]
    if not vals:
        return 0.0
    logs = [math.log(v / scale) for v in vals]
    return math.exp(sum(logs) / len(logs)) * scale


def _arithmetic_mean(values: Iterable[float]) -> float:
    vals = list(values)
    return sum(vals) / len(vals) if vals else 0.0


def _pearson_r(xs: Iterable[float], ys: Iterable[float]) -> float:
    x = list(xs)
    y = list(ys)
    if len(x) < 2 or len(x) != len(y):
        return 0.0
    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    den_x = sum((xi - mean_x) ** 2 for xi in x)
    den_y = sum((yi - mean_y) ** 2 for yi in y)
    denom = math.sqrt(den_x * den_y)
    return num / denom if denom != 0 else 0.0


def _linear_fit(xs: Iterable[float], ys: Iterable[float]) -> Tuple[float, float]:
    x = list(xs)
    y = list(ys)
    if len(x) < 2 or len(x) != len(y):
        return 0.0, 0.0
    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    den = sum((xi - mean_x) ** 2 for xi in x)
    slope = num / den if den != 0 else 0.0
    intercept = mean_y - slope * mean_x
    return slope, intercept


def _r2(y: Iterable[float], y_pred: Iterable[float]) -> float:
    y_list = list(y)
    y_pred_list = list(y_pred)
    if len(y_list) == 0 or len(y_list) != len(y_pred_list):
        return 0.0
    mean_y = sum(y_list) / len(y_list)
    ss_res = sum((yi - ypi) ** 2 for yi, ypi in zip(y_list, y_pred_list))
    ss_tot = sum((yi - mean_y) ** 2 for yi in y_list)
    return 1 - ss_res / ss_tot if ss_tot != 0 else 0.0


def parse_run(
    results_dir: Path,
    run_num: int,
    step: float = 0.001,
    max_speed: float = 0.4,
    fps: float = 60.0,
    quiet: bool = False,
    output_tag: str | None = None,
) -> Tuple[Path, Path]:
    run_dir = results_dir / str(run_num)
    raw_dir = run_dir / "raw_data"
    base_input = raw_dir / f"simulation_log_{run_num}.csv"
    part_pattern = f"simulation_log_{run_num}_part*.csv"
    part_files = sorted(
        raw_dir.glob(part_pattern),
        key=lambda p: _part_index(p.name),
    )
    input_files = []
    if base_input.exists():
        input_files.append(base_input)
    input_files.extend(part_files)
    tag = ""
    if output_tag:
        tag = output_tag if output_tag.startswith("_") else f"_{output_tag}"
    output_file_geo = run_dir / f"parsedGeometricMeanSimulatino{run_num}{tag}_Log.csv"
    output_file_mean = run_dir / f"parsedArithmeticMeanSimulatino{run_num}{tag}_Log.csv"
    summary_file = run_dir / f"stats_summary_{run_num}{tag}.txt"

    if not input_files:
        raise FileNotFoundError(
            f"Missing input log: {base_input} or {part_pattern}"
        )

    data = []
    for input_file in input_files:
        with open(input_file, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    evo_speed = float(row["evolution rate"])
                    length_lived = float(row["length lived"])
                    species_pop_time = float(row["species population time"])
                    population = float(row["population"])
                    data.append(
                        {
                            "evolution rate": evo_speed,
                            "length lived": length_lived,
                            "species population time": species_pop_time,
                            "population": population,
                        }
                    )
                except (ValueError, KeyError, TypeError):
                    continue

    parsed_data_geo = []
    parsed_data_mean = []
    current_speed = 0.0
    while current_speed <= max_speed + 1e-9:
        rows_in_bin = [
            row
            for row in data
            if abs(row["evolution rate"] - current_speed) < step / 2
        ]

        if rows_in_bin:
            length_values = [row["length lived"] for row in rows_in_bin]
            species_pop_values = [row["species population time"] for row in rows_in_bin]

            parsed_data_geo.append(
                {
                    "evolution rate": round(current_speed, 3),
                    "geometric mean length lived": _geometric_mean(length_values),
                    "geometric mean species population time": _geometric_mean(
                        species_pop_values
                    ),
                }
            )

            parsed_data_mean.append(
                {
                    "evolution rate": round(current_speed, 3),
                    "arithmetic mean length lived": _arithmetic_mean(length_values),
                    "arithmetic mean species population time": _arithmetic_mean(
                        species_pop_values
                    ),
                }
            )

        current_speed += step

    with open(output_file_geo, "w", newline="") as csvfile:
        fieldnames = [
            "evolution rate",
            "geometric mean length lived",
            "geometric mean species population time",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in parsed_data_geo:
            writer.writerow(row)

    with open(output_file_mean, "w", newline="") as csvfile:
        fieldnames = [
            "evolution rate",
            "arithmetic mean length lived",
            "arithmetic mean species population time",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in parsed_data_mean:
            writer.writerow(row)

    summary_lines = []

    x_geo = [row["evolution rate"] for row in parsed_data_geo]
    y_geo = [row["geometric mean length lived"] for row in parsed_data_geo]
    x_mean = [row["evolution rate"] for row in parsed_data_mean]
    y_mean = [row["arithmetic mean length lived"] for row in parsed_data_mean]

    def _add_line(line: str) -> None:
        summary_lines.append(line)
        if not quiet:
            print(line)

    def _print_stats(label: str, x: Iterable[float], y: Iterable[float]) -> None:
        x_list = list(x)
        y_list = list(y)
        _add_line(f"--- {label} ---")
        if len(x_list) < 2 or len(x_list) != len(y_list):
            _add_line("Not enough data for stats.")
            return

        if _HAS_SCIPY and _HAS_NUMPY:
            r, p_value = _scipy_pearsonr(np.array(x_list), np.array(y_list))
        else:
            r = _pearson_r(x_list, y_list)
            p_value = None

        _add_line(f"Pearson correlation coefficient (r): {r}")
        if p_value is not None:
            _add_line(f"P-value: {p_value}")
        else:
            _add_line("P-value: N/A")
        _add_line(f"Coefficient of determination (r^2): {r**2}")

        slope, intercept = _linear_fit(x_list, y_list)
        _add_line(f"Linear equation: y = {slope:.4f} * x + {intercept:.4f}")
        y_pred_linear = [slope * xi + intercept for xi in x_list]
        _add_line(f"Linear model R^2: {_r2(y_list, y_pred_linear):.4f}")

        if _HAS_NUMPY:
            coeffs = np.polyfit(np.array(x_list), np.array(y_list), 2)
            y_pred_quad = np.polyval(coeffs, np.array(x_list))
            _add_line(
                "Quadratic equation: y = "
                f"{coeffs[0]:.4f} * x^2 + {coeffs[1]:.4f} * x + {coeffs[2]:.4f}"
            )
            _add_line(f"Quadratic model R^2: {_r2(y_list, y_pred_quad):.4f}")
        else:
            _add_line("Quadratic fit skipped (numpy not available).")

    if fps > 0:
        seconds_1000 = 1000.0 / fps
        _add_line(
            f"Estimated time for 1000 iterations at {fps:.2f} FPS: {seconds_1000:.2f} seconds"
        )

    if not quiet:
        print("")
    _print_stats("Geometric mean", x_geo, y_geo)
    if not quiet:
        print("")
    _print_stats("Arithmetic mean", x_mean, y_mean)

    _add_line(f"Parsed geometric-mean data saved to {output_file_geo}")
    _add_line(f"Parsed arithmetic-mean data saved to {output_file_mean}")

    species_count_log = len(data)
    medium_count_log = sum(
        1
        for row in data
        if row["length lived"] > 500 and row["length lived"] <= 1999
    )
    big_count_log = sum(1 for row in data if row["length lived"] > 1999)
    _add_line("")
    _add_line("--- Species Summary (from logs) ---")
    _add_line(f"Species count: {species_count_log}")
    _add_line(f"Medium species count: {medium_count_log}")
    _add_line(f"Big species count: {big_count_log}")

    meta_path = run_dir / "run_meta.json"
    _add_line("")
    _add_line("--- Run Summary ---")
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            frame_count = meta.get("frame_count")
            start_time = meta.get("start_time")
            elapsed = meta.get("elapsed_seconds")
            amnt_species = meta.get("amnt_of_species")
            amnt_medium = meta.get("amnt_of_medium_species")
            amnt_big = meta.get("amnt_of_big_species")

            if frame_count is not None:
                _add_line(f"Total frames: {frame_count}")
            if start_time is not None:
                try:
                    start_ts = float(start_time)
                    start_label = _format_wall_time_la(start_ts)
                    _add_line(f"Start time: {start_label}")
                    if isinstance(elapsed, (int, float)):
                        end_ts = start_ts + float(elapsed)
                        end_label = _format_wall_time_la(end_ts)
                        _add_line(f"End time: {end_label}")
                except Exception:
                    _add_line(f"Start time (epoch): {start_time}")
            if elapsed is not None:
                try:
                    elapsed = float(elapsed)
                    hours = int(elapsed // 3600)
                    minutes = int((elapsed % 3600) // 60)
                    seconds = elapsed % 60
                    _add_line(
                        f"Total runtime: {hours:02d}:{minutes:02d}:{seconds:05.2f} (h:m:s)"
                    )
                except Exception:
                    _add_line(f"Total runtime (seconds): {elapsed}")
            if amnt_species is not None:
                _add_line(f"Species count: {amnt_species}")
            if amnt_medium is not None:
                _add_line(f"Medium species count: {amnt_medium}")
            if amnt_big is not None:
                _add_line(f"Big species count: {amnt_big}")
        except Exception:
            _add_line("Run metadata unreadable.")
    else:
        _add_line("Run metadata not found (run_meta.json missing).")

    summary_file.write_text("\n".join(summary_lines) + "\n")

    return output_file_geo, output_file_mean


def _infer_latest_run(results_dir: Path) -> int:
    from settings_manager import load_settings

    settings = load_settings()
    try:
        current = int(settings.get("num_tries", 0))
    except Exception:
        current = 0
    return max(0, current - 1)


if __name__ == "__main__":
    results_dir = Path("results")
    
    run_num = _infer_latest_run(results_dir)
    print (f"run num is {run_num}")
    parse_run(results_dir, run_num)
