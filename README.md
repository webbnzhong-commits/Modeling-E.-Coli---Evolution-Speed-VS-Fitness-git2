# Modeling E. coli: Evolution Speed vs Fitness

This project runs and analyzes E. coli evolution simulations across environment change-rate settings.

## Main Scripts
- `simulation_entry.py`: Selects draw/headless simulation mode.
- `master_simulations.py`: Runs grouped simulation batches and aggregates per-master outputs.
- `hub_runner.py`: Runs hub-level sweeps across environment change rates.
- `hub_viewer.py`: Interactive viewer for hub outputs.

## Configuration
- `settings.json`: Primary runtime configuration.
- `settings_manager.py`: Loads and saves settings safely.

## Quick Start
1. Create and activate a Python environment.
2. Install dependencies:
   - `python3 -m pip install -r requirements.txt`
3. Run a simulation:
   - `python3 simulation_entry.py`
4. Run a hub sweep:
   - `python3 hub_runner.py`

## Output
Generated outputs are written under `results/` and are intentionally ignored in version control.

## Paper Export
To create NHSJS-ready summary tables and figures from the latest hub run:

```bash
python3 paper_export.py
```

To export a specific hub:

```bash
python3 paper_export.py --hub-dir results/hub/hub_78
```

The export is written to `paper_exports/<hub_name>/` and includes `paper_summary.csv`,
`environment_bins.csv`, `paper_methods.json`, and PNG figures when `matplotlib` is installed.
