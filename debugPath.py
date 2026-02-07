import pygame
import random
import math
#import pyautogui
import time
import csv
import statistics
from collections import Counter
from pathlib import Path
import shutil
#from pathlib import Path


#git debug



# initialize per-run logging files

results_dir = Path("results")
target_dir = results_dir / "6"
target_dir.mkdir(parents=True, exist_ok=True)

sources = [
    results_dir / "1" / "simulation_log_1.csv",
    results_dir / "2" / "simulation_log_2.csv",
]
for src in sources:
    if src.exists():
        shutil.move(str(src), target_dir / src.name)




