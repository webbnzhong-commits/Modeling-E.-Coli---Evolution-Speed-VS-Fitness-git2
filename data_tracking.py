import atexit
import csv
import os
import re
from pathlib import Path
from typing import Union

from simulatino_parser import parse_run
from settings_manager import load_settings, save_settings


_HEADER = [
    "evolution rate",
    "length lived",
    "species population time",
    "population",
]


class RunDataTracker:
    def __init__(
        self, results_dir: Union[str, Path] = "results", rows_per_file: int = 1000000
    ) -> None:
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.rows_per_file = max(1, rows_per_file)

        env_run_num = os.environ.get("SIM_RUN_NUM")
        run_num_override = None
        if env_run_num is not None:
            try:
                run_num_override = int(env_run_num)
            except ValueError:
                run_num_override = None

        settings = load_settings()
        try:
            current = int(settings.get("num_tries", -1))
        except Exception:
            current = -1

        if run_num_override is not None:
            self.run_num = run_num_override
            if (self.run_num + 1) > current:
                settings["num_tries"] = self.run_num + 1
                save_settings(settings)
        else:
            self.run_num = current
            settings["num_tries"] = self.run_num + 1
            save_settings(settings)

        print(self.run_num)

        self.run_dir = self.results_dir / str(self.run_num)
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.raw_dir = self.run_dir / "raw_data"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.base_log_path = self.raw_dir / f"simulation_log_{self.run_num}.csv"
        self.current_part = 0
        self.current_rows = 1
        self._open_initial_log()

        self.master_dir = os.environ.get("SIM_MASTER_DIR")
        self.master_csv_file = None
        self.master_csv_writer = None
        self.master_current_part = 0
        self.master_current_rows = 1
        if self.master_dir:
            self._open_master_log()

        self.amntOfSpecies = 0
        self.amntOfMediumSpecies = 0
        self.amntOfBigSpecies = 0
        self.amntOfSpeciesEach = ""
        self.should_parse = False
        self._closed = False
        self.last_write_frame = None
        atexit.register(self._atexit_close)
        self._initialize_species_counts()

    def _part_path(self, part_index: int) -> Path:
        return self.raw_dir / f"simulation_log_{self.run_num}_part{part_index}.csv"

    @staticmethod
    def _part_index(path: Path) -> int:
        match = re.search(r"_part(\d+)\.csv$", path.name)
        return int(match.group(1)) if match else 0

    def _count_data_rows(self, path: Path) -> int:
        if not path.exists() or path.stat().st_size == 0:
            return 0
        with open(path, newline="") as csvfile:
            reader = csv.reader(csvfile)
            try:
                next(reader)
            except StopIteration:
                return 0
            return sum(1 for _ in reader)

    def _write_log_file(self, path: Path, rows) -> None:
        with open(path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(_HEADER)
            writer.writerows(rows)

    def _rebalance_existing_logs(self) -> None:
        input_files = [self.base_log_path] + sorted(
            self.raw_dir.glob(f"simulation_log_{self.run_num}_part*.csv"),
            key=self._part_index,
        )

        all_rows = []
        max_rows_in_file = 0
        for path in input_files:
            if not path.exists() or path.stat().st_size == 0:
                continue
            with open(path, newline="") as csvfile:
                reader = csv.reader(csvfile)
                try:
                    next(reader)
                except StopIteration:
                    continue
                file_rows = list(reader)
                max_rows_in_file = max(max_rows_in_file, len(file_rows))
                all_rows.extend(file_rows)

        if max_rows_in_file <= self.rows_per_file:
            return
        if not all_rows:
            return

        chunks = [
            all_rows[i : i + self.rows_per_file]
            for i in range(0, len(all_rows), self.rows_per_file)
        ]
        if not chunks:
            return

        for path in input_files:
            if path.exists() and path != self.base_log_path:
                path.unlink()

        self._write_log_file(self.base_log_path, chunks[0])
        for idx, chunk in enumerate(chunks[1:], start=1):
            self._write_log_file(self._part_path(idx), chunk)

    def _open_log_file(self, path: Path, write_header: bool) -> None:
        self.log_path = path
        self.csv_file = open(self.log_path, "a", newline="")
        self.csv_writer = csv.writer(self.csv_file)
        if write_header:
            self.csv_writer.writerow(_HEADER)
        print(f"Run #{self.run_num} -> writing log: {self.log_path}")

    def _open_master_log(self) -> None:
        master_dir = Path(self.master_dir) / str(self.run_num)
        master_dir.mkdir(parents=True, exist_ok=True)
        master_raw_dir = master_dir / "raw_data"
        master_raw_dir.mkdir(parents=True, exist_ok=True)
        self.master_base_log_path = master_raw_dir / f"simulation_log_{self.run_num}.csv"
        self.master_log_path = self.master_base_log_path
        self.master_csv_file = open(self.master_log_path, "a", newline="")
        self.master_csv_writer = csv.writer(self.master_csv_file)
        if self.master_log_path.stat().st_size == 0:
            self.master_csv_writer.writerow(_HEADER)

    def _rotate_master_if_needed(self) -> None:
        if self.master_csv_writer is None:
            return
        if self.master_current_rows < self.rows_per_file - 1:
            return
        self.master_csv_file.close()
        self.master_current_part = max(1, self.master_current_part + 1)
        self.master_current_rows = 0
        master_dir = Path(self.master_dir) / str(self.run_num) / "raw_data"
        master_dir.mkdir(parents=True, exist_ok=True)
        self.master_log_path = master_dir / f"simulation_log_{self.run_num}_part{self.master_current_part}.csv"
        self.master_csv_file = open(self.master_log_path, "a", newline="")
        self.master_csv_writer = csv.writer(self.master_csv_file)
        if self.master_log_path.stat().st_size == 0:
            self.master_csv_writer.writerow(_HEADER)

    def _open_initial_log(self) -> None:
        self._rebalance_existing_logs()
        part_files = sorted(
            self.raw_dir.glob(f"simulation_log_{self.run_num}_part*.csv"),
            key=self._part_index,
        )
        if part_files:
            last_part = part_files[-1]
            self.current_part = self._part_index(last_part)
            self.current_rows = self._count_data_rows(last_part)
            if self.current_rows >= self.rows_per_file:
                self.current_part += 1
                self.current_rows = 0
                self._open_log_file(self._part_path(self.current_part), True)
            else:
                write_header = not (last_part.exists() and last_part.stat().st_size > 0)
                self._open_log_file(last_part, write_header)
            return

        self.current_rows = self._count_data_rows(self.base_log_path)
        if self.current_rows >= self.rows_per_file:
            self.current_part = 1
            self.current_rows = 0
            self._open_log_file(self._part_path(self.current_part), True)
        else:
            write_header = not (
                self.base_log_path.exists() and self.base_log_path.stat().st_size > 0
            )
            self._open_log_file(self.base_log_path, write_header)

    def _initialize_species_counts(self) -> None:
        raw_paths = [self.base_log_path] + sorted(
            self.raw_dir.glob(f"simulation_log_{self.run_num}_part*.csv"),
            key=self._part_index,
        )
        total = 0
        medium = 0
        big = 0
        for path in raw_paths:
            if not path.exists() or path.stat().st_size == 0:
                continue
            try:
                with open(path, newline="") as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if not row:
                            continue
                        try:
                            lifespan = float(row.get("length lived", ""))
                        except Exception:
                            continue
                        total += 1
                        if lifespan > 1999:
                            big += 1
                        elif lifespan > 500:
                            medium += 1
            except Exception:
                continue
        self.amntOfSpecies = total
        self.amntOfMediumSpecies = medium
        self.amntOfBigSpecies = big

    def _rotate_if_needed(self) -> None:
        if self.current_rows < self.rows_per_file - 1:
            return
        self.csv_file.close()
        self.current_part = max(1, self.current_part + 1)
        self.current_rows = 0
        self._open_log_file(self._part_path(self.current_part), True)

    def write_species_info(self, evo_val, data, frame_count=None, min_frame_gap=None) -> bool:
        if frame_count is not None and min_frame_gap is not None:
            try:
                gap = int(min_frame_gap)
            except Exception:
                gap = 0
            if gap > 0 and self.last_write_frame is not None:
                if (frame_count - self.last_write_frame) < gap:
                    return False
        if self.current_rows >= self.rows_per_file - 1:
            self._rotate_if_needed()
        if self.master_csv_writer is not None:
            self._rotate_master_if_needed()

        if data["lifespan"] > 1999:
            self.amntOfBigSpecies += 1
            self.amntOfSpeciesEach += f"{data['lifespan']}: {str(evo_val)[:5]}, "
        elif data["lifespan"] > 500:
            self.amntOfMediumSpecies += 1

        population_when_dead = data["pop_time"] // data["lifespan"]

        self.csv_writer.writerow(
            [evo_val, data["lifespan"], data["pop_time"], population_when_dead]
        )
        self.current_rows += 1
        if self.master_csv_writer is not None:
            self.master_csv_writer.writerow(
                [evo_val, data["lifespan"], data["pop_time"], population_when_dead]
            )
            self.master_current_rows += 1
        if frame_count is not None:
            self.last_write_frame = frame_count

        self.amntOfSpecies += 1
        return True

    def set_should_parse(self, should_parse: bool) -> None:
        self.should_parse = should_parse

    def _atexit_close(self) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.csv_file.close()
        if self.master_csv_file is not None:
            self.master_csv_file.close()
        if self.should_parse:
            try:
                parse_run(self.results_dir, self.run_num)
            except Exception as e:
                print(f"Failed to parse results: {e}")
            if self.master_dir:
                try:
                    parse_run(Path(self.master_dir), self.run_num)
                except Exception as e:
                    print(f"Failed to parse master results: {e}")
