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
2. Install dependencies used by the scripts (for example `pygame`, `numpy`, `matplotlib`).
3. Run a simulation:
   - `python simulation_entry.py`
4. Run a hub sweep:
   - `python hub_runner.py`

## Output
Generated outputs are written under `results/` and are intentionally ignored in version control.
