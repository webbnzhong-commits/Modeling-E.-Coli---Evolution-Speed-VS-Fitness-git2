import pygame
import random
import math
import os
import json
try:
    import numpy as np
    HAS_NUMPY = True
except Exception:
    np = None
    HAS_NUMPY = False
#import pyautogui
import time
import statistics
import csv
from collections import Counter
from pathlib import Path
from data_tracking import RunDataTracker
from simulatino_parser import parse_run
from settings_manager import load_settings, save_settings
from multiprocessing import Pool, cpu_count
from fps_tracker import FPSTracker
fast_update = None
if HAS_NUMPY:
    try:
        from fast_loop import fast_update
    except Exception:
        fast_update = None

def work(x):
    return x * x



#from pathlib import Path


#git debug



# initialize per-run logging
tracker = RunDataTracker()
run_num = tracker.run_num
RUN_META_PATH = tracker.results_dir / str(run_num) / "run_meta.json"
MASTER_RUN_META_PATH = None
if os.environ.get("SIM_MASTER_DIR"):
    MASTER_RUN_META_PATH = Path(os.environ["SIM_MASTER_DIR"]) / str(run_num) / "run_meta.json"


# Window settings
settings = load_settings()
WIDTH = int(settings["screen"]["width"])
HEIGHT = int(settings["screen"]["height"])
WIDTH = max(200, WIDTH)
HEIGHT = max(200, HEIGHT)

display_mode = int(settings["display_mode"])
draw = bool(settings["draw"])
drawSometimes = bool(settings["drawSometimes"])
drawAmnt = int(settings["drawAmnt"])
population_cap = float(settings["population_cap"])
enviormentChangeRate = float(settings["enviormentChangeRate"])
PH_EFFECT_SCALE = float(settings["ph_effect"]["scale"])
PH_EFFECT_DIVISOR = float(settings["ph_effect"]["divisor"])
TEMP_EFFECT_SCALE = float(settings["temp_effect"]["scale"])
TEMP_EFFECT_DIVISOR = float(settings["temp_effect"]["divisor"])
REPRO_DEBUF_MIN = float(settings["reproduction_debuf_min"])

FPS_GRAPH_X = 400
FPS_GRAPH_Y = 25
FPS_GRAPH_H = 80
CANCER_ALPHA = 128


def _recalc_graph_width():
    global FPS_GRAPH_W
    FPS_GRAPH_W = WIDTH - FPS_GRAPH_X - 150


_recalc_graph_width()
ARITH_OVERLAY_H = 140

# Initialize pygame∫
pygame.init()
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Dots Example")

evo_speed_range = [0.05, 0.4]

SIM_CONTROL_FILE = os.environ.get("SIM_CONTROL_FILE")
ALL_ACTIVE = os.environ.get("SIM_ALL_ACTIVE") == "1"
SIM_FPS_PATH = os.environ.get("SIM_FPS_PATH")
FAST_MODE = os.environ.get("FAST_MODE") == "1" and HAS_NUMPY and fast_update is not None
try:
    SIM_INDEX = int(os.environ.get("SIM_INDEX", "0"))
except ValueError:
    SIM_INDEX = 0
try:
    SIM_TOTAL = int(os.environ.get("SIM_TOTAL", "0"))
except ValueError:
    SIM_TOTAL = 0


def _read_control_state():
    if not SIM_CONTROL_FILE:
        return None, None, None, None, None, None
    try:
        data = Path(SIM_CONTROL_FILE).read_text().strip()
    except Exception:
        return None, None, None, None, None, None
    if not data:
        return None, None, None, None, None, None
    if data.lstrip().startswith("{"):
        try:
            payload = json.loads(data)
        except Exception:
            return None, None, None, None, None, None
        active = payload.get("active")
        enabled = payload.get("enabled")
        draw_modes = payload.get("draw_mode")
        draw_every = payload.get("draw_every")
        mode_values = payload.get("mode")
        update_tokens = payload.get("update_tokens")
        try:
            active = int(active) if active is not None else None
        except (TypeError, ValueError):
            active = None
        enabled_flags = None
        if isinstance(enabled, list):
            enabled_flags = [bool(item) for item in enabled]
        draw_mode_list = None
        if isinstance(draw_modes, list):
            draw_mode_list = []
            for item in draw_modes:
                try:
                    draw_mode_list.append(int(item))
                except (TypeError, ValueError):
                    draw_mode_list.append(0)
        draw_every_list = None
        if isinstance(draw_every, list):
            draw_every_list = []
            for item in draw_every:
                try:
                    draw_every_list.append(int(item))
                except (TypeError, ValueError):
                    draw_every_list.append(1)
        mode_list = None
        if isinstance(mode_values, list):
            mode_list = []
            for item in mode_values:
                try:
                    mode_list.append(int(item))
                except (TypeError, ValueError):
                    mode_list.append(0)
        update_list = None
        if isinstance(update_tokens, list):
            update_list = []
            for item in update_tokens:
                try:
                    update_list.append(int(item))
                except (TypeError, ValueError):
                    update_list.append(0)
        return active, enabled_flags, draw_mode_list, draw_every_list, mode_list, update_list
    try:
        return int(data), None, None, None, None, None
    except ValueError:
        return None, None, None, None, None, None


def _build_caption(active, enabled):
    if SIM_TOTAL > 0:
        label = f"Simulation {SIM_INDEX + 1}/{SIM_TOTAL}"
    else:
        label = f"Simulation {SIM_INDEX + 1}"
    if not enabled:
        status = "OFF"
    else:
        status = "ACTIVE" if active else "INACTIVE"
    return f"Dots Example - {label} - {status}"


def apply_settings(new_settings, update_screen=False):
    global settings, WIDTH, HEIGHT, screen
    global display_mode, draw, drawSometimes, drawAmnt
    global population_cap, enviormentChangeRate
    global PH_EFFECT_SCALE, PH_EFFECT_DIVISOR, TEMP_EFFECT_SCALE, TEMP_EFFECT_DIVISOR, REPRO_DEBUF_MIN

    settings = new_settings
    new_width = int(settings["screen"]["width"])
    new_height = int(settings["screen"]["height"])
    new_width = max(200, new_width)
    new_height = max(200, new_height)

    if new_width != WIDTH or new_height != HEIGHT:
        WIDTH, HEIGHT = new_width, new_height
        _recalc_graph_width()
        if update_screen and pygame.get_init():
            screen = pygame.display.set_mode((WIDTH, HEIGHT))

    display_mode = int(settings["display_mode"])
    draw = bool(settings["draw"])
    drawSometimes = bool(settings["drawSometimes"])
    drawAmnt = int(settings["drawAmnt"])
    population_cap = float(settings["population_cap"])
    enviormentChangeRate = float(settings["enviormentChangeRate"])
    PH_EFFECT_SCALE = float(settings["ph_effect"]["scale"])
    PH_EFFECT_DIVISOR = float(settings["ph_effect"]["divisor"])
    TEMP_EFFECT_SCALE = float(settings["temp_effect"]["scale"])
    TEMP_EFFECT_DIVISOR = float(settings["temp_effect"]["divisor"])
    REPRO_DEBUF_MIN = float(settings["reproduction_debuf_min"])


def reload_settings():
    apply_settings(load_settings(), update_screen=True)


'''
Three traits:
reproduction rate: what resource amount is needed to reproduce. How much o or c or n is needed to reproduce, however they need 10 of each resource to live

speed of evolution: how fast traits change over generations
Charizma: when to dots encounter, how like one is to steal resources from the other, or if diffrent which one will kill the other
After ten cycles they will die.
Size - chance of surviving attacks from other dots
Chance to fight - how aggressive they are when encountering other dots
amount per reproduction - how much they reproduce each time (the more the more food they need to eat)
Offspring size


Resource taken: three resources, o, c, and n
Each cycle the amount of resources added to the environment will be similar to last cycle but a bit diffrent, however o + c + n will be 100

Each cycle there will be a new amount of resources added to the environment.
The resources will be distributed to each dot. They collect resources until they have enough to reproduce. (reproduction rate)

Ability to breed: get the squared diffrence of the traits above. If the those numbers are low enough they can breed.
'''

'''
Thesis: How does evoloution speed allow species to adapt to a changing environment?
Does evoloution speed affect species fitness
'''

def _load_arithmetic_mean_points(results_dir: Path, run_num: int):
    run_dir = results_dir / str(run_num)
    parsed_path = run_dir / f"parsedArithmeticMeanSimulatino{run_num}_Log.csv"
    if not parsed_path.exists():
        return []
    points = []
    with open(parsed_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                x = float(row["evolution rate"])
                y = float(row["arithmetic mean length lived"])
            except (ValueError, KeyError, TypeError):
                continue
            points.append((x, y))
    return points


def _draw_arithmetic_mean_overlay(surface, font, points, rect, error_text=""):
    surface.fill((0, 0, 0, 0))
    pygame.draw.rect(surface, (180, 180, 180), rect, 1)
    title = font.render("Arithmetic Mean Length Lived", True, (220, 220, 0))
    
    surface.blit(title, (rect.x + 8, rect.y + 6))

    if error_text:
        err = font.render(error_text, True, (255, 160, 160))
        surface.blit(err, (rect.x + 8, rect.y + 28))
        return

    if not points:
        msg = font.render("No arithmetic-mean data yet.", True, (200, 200, 200))
        surface.blit(msg, (rect.x + 8, rect.y + 28))
        return

    points_sorted = sorted(points, key=lambda p: p[0])
    xs = [p[0] for p in points_sorted]
    ys = [p[1] for p in points_sorted]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    if max_x == min_x:
        max_x = min_x + 1.0
    if max_y == min_y:
        max_y = min_y + 1.0

    x_pad = (max_x - min_x) * 0.05
    y_pad = (max_y - min_y) * 0.05
    min_x -= x_pad
    max_x += x_pad
    min_y -= y_pad
    max_y += y_pad

    plot_left = rect.x + 8
    plot_top = rect.y + 28
    plot_width = rect.width - 16
    plot_height = rect.height - 36

    def _scale_point(x, y):
        px = plot_left + int(((x - min_x) / (max_x - min_x)) * plot_width)
        py = plot_top + plot_height - int(((y - min_y) / (max_y - min_y)) * plot_height)
        return px, py

    last_pt = None
    for x, y in points_sorted:
        px, py = _scale_point(x, y)
        if last_pt is not None:
            pygame.draw.line(surface, (0, 200, 255), last_pt, (px, py), 2)
        pygame.draw.circle(surface, (0, 220, 255), (px, py), 2)
        last_pt = (px, py)

    
    

# Create a Dot class
class Dot:
    def __init__(self, x, y, radius=5, color=(255, 255, 255)):
        self.x = x
        self.realx = x
        self.y = y
        self.realy = y
        self.radius = radius
        self.color = color
        self.speed = [random.uniform(-0.002, 0.002), random.uniform(-0.002, 0.002)]
        self.cancerous = False
        self._cancer_surface = None
        self._cancer_surface_size = None
        self._cancer_surface_color = None

        self.favored_resource = random.choice(["o", "c", "n"])
        self.immune_system = random.randint(0, 5)  # resistance to antiBiotics
        self.optimal_ph = random.uniform(7, 8)  # preferred pH level, works best in 7.2
        self.optimal_temp = random.uniform(36, 38)  # preferred temperature, works best at 37



        self.color = [random.randint(100, 255) for _ in range(3)]
        num = math.sqrt(self.speed[0]**2 + self.speed[1]**2)
        num /= 4.0
        if num != 0:
            self.speed = [self.speed[0]/num, self.speed[1]/num]

        # Evolution traits
        self.reproduction_resource = {
            "o": random.randint(5, 1000),
            "c": random.randint(5, 1000),
            "n": random.randint(5, 1000)
        }  # resources required to reproduce
        factor = 10/(total := sum(self.reproduction_resource.values()))
        for r in ["o", "c", "n"]:
            self.reproduction_resource[r] = max(1, int(self.reproduction_resource[r] * factor))   

        


        self.evolution_speed = random.uniform(evo_speed_range[0], evo_speed_range[1])  # how quickly traits evolve
        #self.evolution_speed = 0.05 + (random.randint(0, 100)/1000)    

        
        
        # Resources held
        self.resources = {"o": 0, "c": 0, "n": 0}
        
        # Life cycle count
        self.life_cycles = 0
        self.max_cycles = 40 + random.randint(0, 20)  # lifespan in cycles

        self.size = 1 + random.randint(0, 5)  # size affects survival chance



    def update(self):
        global dots
        self.collect_resources()
        # Move the dot
        self.realx += self.speed[0]
        self.realy += self.speed[1]
        self.x = int(self.realx)
        self.y = int(self.realy)

        # Bounce off edges
        if self.x <= 0 or self.x >= WIDTH:
            self.speed[0] *= -1
        if self.y <= 0 or self.y >= HEIGHT:
            self.speed[1] *= -1

        self.life_cycles += 1
        if self.life_cycles >= self.max_cycles:
            self.death()
            return
        
        reproduce = True
        ph_diff = abs(self.optimal_ph - phLevel)
        temp_diff = abs(self.optimal_temp - temp)
        ph_div = max(PH_EFFECT_DIVISOR, 1e-6)
        temp_div = max(TEMP_EFFECT_DIVISOR, 1e-6)
        ph_effect = min(1.0, (ph_diff / ph_div) * PH_EFFECT_SCALE)
        temp_effect = min(1.0, (temp_diff / temp_div) * TEMP_EFFECT_SCALE)
        debuf = max(REPRO_DEBUF_MIN, ph_effect * temp_effect)
        for r in ["o", "c", "n"]:
            if self.resources[r] / debuf < self.reproduction_resource[r]:
                reproduce = False

        if reproduce:
            self.resources["o"] -= self.reproduction_resource["o"]
            self.resources["c"] -= self.reproduction_resource["c"]
            self.resources["n"] -= self.reproduction_resource["n"]
            child = _spawn_child_from_parent(self)
            dots.append(child)

    def death (self):

        nutrient.dead_cell(self.resources)
        dots.remove(self)
        return
    
    def collect_resources(self):
        resource_pool = nutrient.get_resources()
        total = 0
        for r in ["o", "c", "n"]:
            if resource_pool[r] > 0:
                
                self.resources[r] += resource_pool[r]
                

    def draw(self, surface):
        
        #b = 125

        #pygame.draw.circle(surface, color, (int(self.x), int(self.y)), self.size)
        if self.cancerous:
            color_tuple = tuple(self.color)
            if (
                self._cancer_surface is None
                or self._cancer_surface_size != self.size
                or self._cancer_surface_color != color_tuple
            ):
                size = max(1, int(self.size))
                diameter = size * 2 + 2
                self._cancer_surface = pygame.Surface((diameter, diameter), pygame.SRCALPHA)
                pygame.draw.circle(
                    self._cancer_surface,
                    (color_tuple[0], color_tuple[1], color_tuple[2], CANCER_ALPHA),
                    (diameter // 2, diameter // 2),
                    size,
                )
                self._cancer_surface_size = self.size
                self._cancer_surface_color = color_tuple
            surface.blit(self._cancer_surface, (int(self.x) - self.size - 1, int(self.y) - self.size - 1))
        else:
            pygame.draw.circle(surface, self.color, (int(self.x), int(self.y)), self.size)
        



# Initial resource pool
class nutrients ():
    def __init__ (self):
        self.resource_pool = {"o": 34, "c": 33, "n": 33}
        self.deadNutreints = {"o": 0, "c": 0, "n": 0}

        self.goingToAmnt = random.randint(10, 30)
        self.foodAmnt = 100
        
        



    def dead_cell(self, resources):
        for r in ["o", "c", "n"]:
            self.deadNutreints[r] += resources[r]
    

    def regenerate_resources(self):
        # Slightly vary each resource value
        for r in ["o", "c", "n"]:
            self.resource_pool[r] += random.uniform(-0.1, 0.1)

        # Prevent negative values
        for r in ["o", "c", "n"]:
            if self.resource_pool[r] < 0:
                self.resource_pool[r] = 0
            if self.resource_pool[r] > 1:
                self.resource_pool[r] = 1
            
            
            

        

        # Normalize so that o + c + n ≈ 100
        total = self.resource_pool["o"] + self.resource_pool["c"] + self.resource_pool["n"]
        for r in ["o", "c", "n"]:
            self.resource_pool[r] *= 1/ total
        '''
        if len(dots) > 300:
            self.foodAmnt -= 1
        else:
            self.foodAmnt += 1
        '''
        
        if self.foodAmnt > self.goingToAmnt:
            self.foodAmnt -= 1
        elif self.foodAmnt < self.goingToAmnt:
            self.foodAmnt += 1
        else:
            # Choose a new target amount based on population.
            pop = max(1, len(dots))
            scale = 300 / pop

            low = int(7 * scale) * (1 - enviormentChangeRate/10)
            
            high = low * ((1 + enviormentChangeRate/10) / (1 - enviormentChangeRate/10))

            # Clamp to keep targets reasonable
            low = int(max(1, min(low, 200)))
            high = int(max(low + 1, min(high, 200)))

            self.goingToAmnt = random.randint(low, high)


        

    def get_resources(self):
        const = len(dots)
        return {"o": (self.resource_pool["o"] * self.foodAmnt + self.deadNutreints["o"])/const,
                "c": (self.resource_pool["c"] * self.foodAmnt + self.deadNutreints["c"])/const,
                "n": (self.resource_pool["n"] * self.foodAmnt + self.deadNutreints["n"])/const} 
    
    def update(self):
        self.deadNutreints = {"o": 0, "c": 0, "n": 0}

def reset_simulation():
    
    #dots = [Dot(random.randint(0, WIDTH), random.randint(0, HEIGHT)) for x in range(30)]
    dots = []
    for x in range(30):
        dots.append(Dot(random.randint(0, WIDTH), random.randint(0, HEIGHT)))
        dots[-1].evolution_speed = (evo_speed_range[1] - evo_speed_range[0]) / 30 * x + evo_speed_range[0]
    nutrient = nutrients()
    #totalSim += 1
    frame_count = 0
    return dots, nutrient

nutrient = nutrients()

# Create dots
dots = []
dots, nutrient = reset_simulation()



clock = pygame.time.Clock()
running = True
should_parse = False
frame_count = 0
fps_tracker = FPSTracker(sample_interval=1000, log_path=SIM_FPS_PATH)
avgSpecies = []
run_start_time = time.perf_counter()
META_WRITE_INTERVAL = 1000






#time.sleep(10)  # Give user time to switch to pygame window
antiBiotics = 0
totalSim = 0
pause = False
info = ""
info2 = ""
info3 = ""
info4 = ""
info5 = ""
info6 = ""
info7 = ""
info8 = ""
info9 = ""
last_avg_evo_speed          = 0
species_period              = 0
current_species_populatino  = 0
species_trackers            = {}
show_arithmetic_graph       = False
arithmetic_points           = []
arithmetic_graph_error       = ""
arithmetic_surface          = None

temp = 37.0



fightChance                 = 5


phLevel                     = 7.0


font = pygame.font.SysFont("Consolas", 20)

tempDirUp = True


def _update_stats_snapshot():
    global info, info2, info3, info4, info5, info6, info7, info8, info9
    global show_arithmetic_graph, arithmetic_points, arithmetic_graph_error, arithmetic_surface
    evo_speeds = [round(dot.evolution_speed, 2) for dot in dots] if len(dots) > 0 else []
    try:
        speed_counts = Counter(evo_speeds)
        common = speed_counts.most_common()
        infos = []
        total_dots = len(dots)

        qualified = []
        for speed, _ in common:
            mode_dots = [dot for dot in dots if round(dot.evolution_speed, 2) == speed]
            count = len(mode_dots)
            if total_dots > 0 and count / total_dots >= 0.10:
                qualified.append((speed, mode_dots, count))

        if len(qualified) == 0 and common:
            qualified = [
                (
                    common[0][0],
                    [dot for dot in dots if round(dot.evolution_speed, 2) == common[0][0]],
                    speed_counts[common[0][0]],
                )
            ]
        while len(qualified) < 3 and qualified:
            qualified.append(qualified[0])

        for idx in range(3):
            if idx >= len(qualified):
                infos.append("No dots")
                infos.append("")
                continue
            speed, mode_dots, count = qualified[idx]
            pct = round((count / total_dots) * 100, 2) if total_dots > 0 else 0
            chosen = random.choice(mode_dots) if mode_dots else None
            if chosen:
                infos.append(
                    f"{pct}% Mode {str(speed)[0:5]} Size:{str(chosen.size)[0:5]} "
                    f" Imm:{str(chosen.immune_system)[0:5]} "
                    f"Fav:{chosen.favored_resource}  Cyc:{str(chosen.max_cycles)[0:5]} Evo Speed :{str(chosen.evolution_speed)[0:5]} "
                )
                infos.append(("color_square", chosen.color))
                infos.append(
                    f"Needs o:{str(chosen.reproduction_resource['o'])[0:5]} "
                    f"c:{str(chosen.reproduction_resource['c'])[0:5]} "
                    f"n:{str(chosen.reproduction_resource['n'])[0:5]}"
                )
            else:
                infos.append("No dots")
                infos.append("")

        while len(infos) < 9:
            infos.append("")

        info, info2, info3, info4, info5, info6, info7, info8, info9 = infos[:9]
    except Exception:
        info = "No unique mode"
        info2 = ""

    show_arithmetic_graph = True
    try:
        tracker.csv_file.flush()
    except Exception:
        pass

    arithmetic_points = []
    arithmetic_graph_error = ""
    try:
        parse_run(tracker.results_dir, tracker.run_num, quiet=True)
        arithmetic_points = _load_arithmetic_mean_points(
            tracker.results_dir,
            tracker.run_num,
        )
    except Exception as e:
        arithmetic_graph_error = f"Graph error: {e}"

    overlay_rect = pygame.Rect(
        FPS_GRAPH_X,
        FPS_GRAPH_Y + FPS_GRAPH_H + 20,
        FPS_GRAPH_W,
        ARITH_OVERLAY_H,
    )
    arithmetic_surface = pygame.Surface(
        (overlay_rect.width, overlay_rect.height), pygame.SRCALPHA
    )
    _draw_arithmetic_mean_overlay(
        arithmetic_surface,
        font,
        arithmetic_points,
        pygame.Rect(0, 0, overlay_rect.width, overlay_rect.height),
        arithmetic_graph_error,
    )


def _spawn_child_from_parent(parent):
    child = Dot(parent.x, parent.y)
    child.evolution_speed = max(0.001, parent.evolution_speed)
    child.size = max(1, parent.size + (random.uniform(-child.evolution_speed, child.evolution_speed)))
    child.favored_resource = parent.favored_resource
    '''
    if (child.immune_system > 0.17):
        child.immune_system = 0 - 5
    
    '''
    child.immune_system = parent.immune_system + int(random.uniform(child.evolution_speed * -4.7, child.evolution_speed * 4.7 ) ** 3)#can start at 0.14 so
    
    child.immune_system = child.immune_system % 5


    child.optimal_ph = parent.optimal_ph + random.uniform(-child.evolution_speed * 2, child.evolution_speed * 2)
    child.optimal_temp = parent.optimal_temp + random.uniform(-child.evolution_speed * 2, child.evolution_speed * 2)
    child.color = parent.color.copy()
    if parent.evolution_speed > 0.17:
        child.immune_system = random.randint(0, 5)



    
    total = 0
    total2 = 0
    for r in ["o", "c", "n"]:
        mutation = int(random.uniform(-child.evolution_speed, child.evolution_speed) / 4)
        child.reproduction_resource[r] = max(1, parent.reproduction_resource[r] + mutation)
        if child.favored_resource != r:
            total2 += child.reproduction_resource[r]
        total += child.reproduction_resource[r]

    if total > 0:
        factor = child.size / total * 2
        for r in ["o", "c", "n"]:
            child.reproduction_resource[r] = max(1, int(child.reproduction_resource[r] * factor))

    child.reproduction_resource[child.favored_resource] = max(
        1 * child.size / 2, child.reproduction_resource[child.favored_resource]
    )

    if random.uniform(0, child.evolution_speed) < 0.09:
        return child
    for r in ["o", "c", "n"]:
        child.reproduction_resource[r] = float("inf")
    child.cancerous = True
    return child


def _log_species(evo_val, data):
    tracker.write_species_info(
        evo_val,
        data,
        frame_count=frame_count,
        min_frame_gap=SPECIES_LOG_GAP,
    )


def _write_run_meta(final=False):
    try:
        elapsed = time.perf_counter() - run_start_time
        payload = {
            "frame_count": int(frame_count),
            "elapsed_seconds": float(elapsed),
            "amnt_of_species": int(tracker.amntOfSpecies),
            "amnt_of_medium_species": int(tracker.amntOfMediumSpecies),
            "amnt_of_big_species": int(tracker.amntOfBigSpecies),
            "final": bool(final),
        }
        RUN_META_PATH.write_text(json.dumps(payload))
        if MASTER_RUN_META_PATH is not None:
            MASTER_RUN_META_PATH.parent.mkdir(parents=True, exist_ok=True)
            MASTER_RUN_META_PATH.write_text(json.dumps(payload))
    except Exception:
        pass

CONTROL_CHECK_INTERVAL = 50
SPECIES_LOG_GAP = 1000
PRINT_INTERVAL = 5000
control_active_index = None
enabled_flags = None
draw_modes = None
draw_every_list = None
mode_values = None
update_tokens = None
enabled = True
draw_mode = 0
draw_every = 500
last_update_token = None
external_active = True
caption_active = False
if SIM_CONTROL_FILE:
    (
        control_active_index,
        enabled_flags,
        draw_modes,
        draw_every_list,
        mode_values,
        update_tokens,
    ) = _read_control_state()
    if enabled_flags is not None and SIM_INDEX < len(enabled_flags):
        enabled = bool(enabled_flags[SIM_INDEX])
    if draw_modes is not None and SIM_INDEX < len(draw_modes):
        draw_mode = int(draw_modes[SIM_INDEX])
    if draw_every_list is not None and SIM_INDEX < len(draw_every_list):
        draw_every = max(1, int(draw_every_list[SIM_INDEX]))
    if mode_values is not None and SIM_INDEX < len(mode_values):
        try:
            display_mode = int(mode_values[SIM_INDEX])
        except (TypeError, ValueError):
            display_mode = display_mode
    if control_active_index is not None:
        caption_active = (control_active_index == SIM_INDEX)
    if ALL_ACTIVE:
        external_active = enabled
    else:
        external_active = enabled if control_active_index is None else enabled and caption_active
    if update_tokens is not None and SIM_INDEX < len(update_tokens):
        last_update_token = update_tokens[SIM_INDEX]
last_caption_state = None
pygame.display.set_caption(_build_caption(caption_active, enabled))


while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            for evo_val, data in list(species_trackers.items()):
                    if data["alive"]:
                        data["alive"] = False
                        if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                            _log_species(evo_val, data)
            should_parse = True
            running = False
        elif event.type == pygame.KEYDOWN:  # key pressed
            if event.key == pygame.K_SPACE:
                for evo_val, data in list(species_trackers.items()):
                    if data["alive"]:
                        data["alive"] = False
                        if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                            _log_species(evo_val, data)
                dots, nutrient = reset_simulation()
                species_trackers = {}
                totalSim += 1

                # Put your code here to handle space press
            if event.key == pygame.K_ESCAPE:
                should_parse = True
                running = False
            if event.key == pygame.K_p:
                pause = not pause  
            if event.key == pygame.K_q:
                for evo_val, data in list(species_trackers.items()):
                    if data["alive"]:
                        data["alive"] = False
                        if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                            _log_species(evo_val, data)
                should_parse = True
                running = False
            if event.key == pygame.K_d and not SIM_CONTROL_FILE:
                draw = not draw
                settings["draw"] = draw
                save_settings(settings)
            if event.key == pygame.K_h and not SIM_CONTROL_FILE:
                display_mode = (display_mode + 1) % 3
                settings["display_mode"] = display_mode
                save_settings(settings)
            if event.key == pygame.K_u:
                reload_settings()
            if event.key == pygame.K_s:
                _update_stats_snapshot()
    if SIM_CONTROL_FILE and frame_count % CONTROL_CHECK_INTERVAL == 0:
        (
            control_active_index,
            enabled_flags,
            draw_modes,
            draw_every_list,
            mode_values,
            update_tokens,
        ) = _read_control_state()
        if control_active_index == -1:
            should_parse = True
            running = False
            break
        if enabled_flags is not None and SIM_INDEX < len(enabled_flags):
            enabled = bool(enabled_flags[SIM_INDEX])
        if draw_modes is not None and SIM_INDEX < len(draw_modes):
            draw_mode = int(draw_modes[SIM_INDEX])
        if draw_every_list is not None and SIM_INDEX < len(draw_every_list):
            draw_every = max(1, int(draw_every_list[SIM_INDEX]))
        if mode_values is not None and SIM_INDEX < len(mode_values):
            try:
                display_mode = int(mode_values[SIM_INDEX])
            except (TypeError, ValueError):
                display_mode = display_mode
        if control_active_index is not None:
            caption_active = (control_active_index == SIM_INDEX)
        else:
            caption_active = False
        if ALL_ACTIVE:
            external_active = enabled
        else:
            external_active = enabled if control_active_index is None else enabled and caption_active
        if update_tokens is not None and SIM_INDEX < len(update_tokens):
            current_token = update_tokens[SIM_INDEX]
            if current_token != last_update_token:
                _update_stats_snapshot()
                last_update_token = current_token

    current_caption_state = (caption_active, enabled)
    if current_caption_state != last_caption_state:
        pygame.display.set_caption(_build_caption(caption_active, enabled))
        last_caption_state = current_caption_state
    if not external_active:
        clock.tick(30)
        continue
    if pause:
        continue
    
    if SIM_CONTROL_FILE:
        if draw_mode == 1:
            should_draw = external_active and (frame_count % draw_every == 0)
        elif draw_mode == 2:
            should_draw = False
        else:
            should_draw = external_active
    else:
        should_draw = external_active and (draw or (drawSometimes and frame_count % drawAmnt == 0))

    if should_draw:
        screen.fill((0, 0, 0))

    # Display frame count
    
    
    
    # (Text labels moved to after drawing dots)

    # Calculate mode evolution speed and pick a random dot with that speed
    

    
    if len(dots) <= 5:
        
        dots.append(Dot(random.randint(0, WIDTH), random.randint(0, HEIGHT)))
            
    
    

    # (Info text rendering moved to after drawing dots)
    
    

    

        

    
    nutrient.regenerate_resources()
    resource_pool = nutrient.get_resources()
    nutrient.update()
    

    # --- Multi-species tracking system ---
    evo_groups = {}
    for dot in dots:
        key = round(dot.evolution_speed, 6)
        evo_groups.setdefault(key, []).append(dot)

    total_population = len(dots)
    active_species = set(evo_groups.keys())

    # If there are 2 or fewer distinct species types, inject a new random dot
    # (species types are defined by evolution_speed rounded to 6 decimals, consistent with tracking)
    if len(active_species) <= 2:
        dots.append(Dot(random.randint(0, WIDTH), random.randint(0, HEIGHT)))

    # Update trackers for active species
    longestAlive = 0
    for evo_val, group in evo_groups.items():
        if evo_val not in species_trackers:
            species_trackers[evo_val] = {"lifespan": 0, "pop_time": 0, "alive": True}
        species_trackers[evo_val]["lifespan"] += 1
        species_trackers[evo_val]["pop_time"] += len(group)
        longestAlive = max(longestAlive, species_trackers[evo_val]["lifespan"])

    # Check for extinct species
    extinct_species = []
    for evo_val, data in list(species_trackers.items()):
        if data["lifespan"] > 4000:
            #reset simulation
            print("Resetting simulation due to long-lived species")
            for evo_val, data in list(species_trackers.items()):
                if data["alive"]:
                    data["alive"] = False
                    if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                        _log_species(evo_val, data)
            dots, nutrient = reset_simulation()
            species_trackers = {}
            totalSim += 1
            break
            
        if evo_val not in active_species and data["alive"]:
            
            data["alive"] = False
            if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                _log_species(evo_val, data)
            
            extinct_species.append(evo_val)

    # Remove extinct species after recording
    for evo_val in extinct_species:
        del species_trackers[evo_val]
    


    if frame_count % (500 / enviormentChangeRate) == 0:
        # Find the most common immune_system value among all dots
        immune_counts = Counter(dot.immune_system for dot in dots)
        if immune_counts:
            # Choose the most common immune_system type as the antiBiotic target
            antiBiotic = immune_counts.most_common(1)[0][0]
            # Pick a random new immune_system different from the most common one for the antiBiotic type
            
            # Infect all dots that do not match the new antiBiotic type
            for dot in list(dots):
                if abs(antiBiotic - dot.immune_system) <= 1:
                    dot.death()

    # --- Population Cap by Species ---
    if frame_count % 5 == 0:
        
        # Group dots by rounded evolution speed
        evo_groups = {}
        for dot in dots:
            key = round(dot.evolution_speed, 2)
            evo_groups.setdefault(key, []).append(dot)

        total_pop = len(dots)
        for evo, group in evo_groups.items():
            # If species population exceeds cap * total population, cull randomly
            max_allowed = int(total_pop * population_cap)
            if len(group) > max_allowed:
                excess = len(group) - max_allowed
                # Randomly kill the excess dots
                for dot in random.sample(group, excess):
                    if dot in dots:
                        dot.death()

    


    # Update and draw each dot
    if FAST_MODE:
        n = len(dots)
        if n > 0:
            realx = np.array([dot.realx for dot in dots], dtype=np.float32)
            realy = np.array([dot.realy for dot in dots], dtype=np.float32)
            speedx = np.array([dot.speed[0] for dot in dots], dtype=np.float32)
            speedy = np.array([dot.speed[1] for dot in dots], dtype=np.float32)
            life_cycles = np.array([dot.life_cycles for dot in dots], dtype=np.int32)
            max_cycles = np.array([dot.max_cycles for dot in dots], dtype=np.int32)
            res_o = np.array([dot.resources["o"] for dot in dots], dtype=np.float32)
            res_c = np.array([dot.resources["c"] for dot in dots], dtype=np.float32)
            res_n = np.array([dot.resources["n"] for dot in dots], dtype=np.float32)
            repro_o = np.array([dot.reproduction_resource["o"] for dot in dots], dtype=np.float32)
            repro_c = np.array([dot.reproduction_resource["c"] for dot in dots], dtype=np.float32)
            repro_n = np.array([dot.reproduction_resource["n"] for dot in dots], dtype=np.float32)
            opt_ph = np.array([dot.optimal_ph for dot in dots], dtype=np.float32)
            opt_temp = np.array([dot.optimal_temp for dot in dots], dtype=np.float32)

            dead_mask, reproduce_mask = fast_update(
                realx,
                realy,
                speedx,
                speedy,
                life_cycles,
                max_cycles,
                res_o,
                res_c,
                res_n,
                repro_o,
                repro_c,
                repro_n,
                opt_ph,
                opt_temp,
                phLevel,
                temp,
                PH_EFFECT_SCALE,
                PH_EFFECT_DIVISOR,
                TEMP_EFFECT_SCALE,
                TEMP_EFFECT_DIVISOR,
                REPRO_DEBUF_MIN,
                WIDTH,
                HEIGHT,
                resource_pool["o"],
                resource_pool["c"],
                resource_pool["n"],
            )

            if np.any(dead_mask):
                dead_o = float(res_o[dead_mask].sum())
                dead_c = float(res_c[dead_mask].sum())
                dead_n = float(res_n[dead_mask].sum())
                nutrient.deadNutreints["o"] += dead_o
                nutrient.deadNutreints["c"] += dead_c
                nutrient.deadNutreints["n"] += dead_n

            alive_mask = ~dead_mask
            if not np.all(alive_mask):
                dots = [dot for i, dot in enumerate(dots) if alive_mask[i]]
                realx = realx[alive_mask]
                realy = realy[alive_mask]
                speedx = speedx[alive_mask]
                speedy = speedy[alive_mask]
                life_cycles = life_cycles[alive_mask]
                res_o = res_o[alive_mask]
                res_c = res_c[alive_mask]
                res_n = res_n[alive_mask]
                reproduce_mask = reproduce_mask[alive_mask]

            for idx, dot in enumerate(dots):
                dot.realx = float(realx[idx])
                dot.realy = float(realy[idx])
                dot.x = int(dot.realx)
                dot.y = int(dot.realy)
                dot.speed[0] = float(speedx[idx])
                dot.speed[1] = float(speedy[idx])
                dot.life_cycles = int(life_cycles[idx])
                dot.resources["o"] = float(res_o[idx])
                dot.resources["c"] = float(res_c[idx])
                dot.resources["n"] = float(res_n[idx])

            if reproduce_mask is not None and len(dots) > 0:
                for idx, flag in enumerate(reproduce_mask):
                    if flag:
                        child = _spawn_child_from_parent(dots[idx])
                        dots.append(child)

            if should_draw:
                for dot in dots:
                    dot.draw(screen)
    else:
        for dot in list(dots):
            dot.update()
            if should_draw:
                dot.draw(screen)

    # Draw text labels (frame count, population, evo avg, evo median, etc.) after all dots are drawn
    if should_draw:
        if display_mode >= 1:
            fps_enabled = display_mode == 2
            if len(dots) > 0:
                avg_evo_speed = sum(dot.evolution_speed for dot in dots) / len(dots)
            else:
                avg_evo_speed = 0
            text = font.render(f"Run: {run_num} | Frames: {frame_count}", True, (255, 255, 255))
            screen.blit(text, (10, 10))
            # Count how many distinct species exist (species defined by evolution_speed rounded to 6 decimals)
            species_count = len({round(dot.evolution_speed, 6) for dot in dots}) if len(dots) > 0 else 0
            text = font.render(f"Population: {len(dots)} | Species#: {species_count}", True, (255, 255, 255))
            screen.blit(text, (10, 50))
            if fps_enabled:
                fps_est = fps_tracker.fps_estimate()
                if fps_tracker.last_interval_time is not None and fps_tracker.last_interval_time > 0 and fps_est is not None:
                    time_1000_text = f"1000 iters: {fps_tracker.last_interval_time:.2f}s @ {fps_est:.1f} FPS"
                else:
                    time_1000_text = "1000 iters: --"
            else:
                time_1000_text = "1000 iters: --"
            text = font.render(time_1000_text, True, (255, 255, 255))
            screen.blit(text, (10, 80))
            text = font.render(f"evo avg: {avg_evo_speed}", True, (255, 255, 255))
            screen.blit(text, (10, 110))

            # Graph of 1000-iteration times (0-2s) to the right of the HUD
            graph_x, graph_y = FPS_GRAPH_X, FPS_GRAPH_Y
            graph_w, graph_h = FPS_GRAPH_W, FPS_GRAPH_H
            if fps_enabled:
                fps_tracker.draw_graph(screen, font, graph_x, graph_y, graph_w, graph_h)

            if display_mode == 2 and show_arithmetic_graph and arithmetic_surface is not None:
                overlay_rect = pygame.Rect(
                    graph_x,
                    graph_y + graph_h + 20,
                    graph_w,
                    ARITH_OVERLAY_H,
                )
                screen.blit(arithmetic_surface, overlay_rect.topleft)
    if frame_count % 400 / enviormentChangeRate == 0:
        phLevel += random.uniform(-2, 2)
        if phLevel < 4:
            phLevel = 4
        if phLevel > 10:
            phLevel = 10
    if frame_count % 10 / enviormentChangeRate == 0:
        if tempDirUp:
            temp += random.uniform(0, 1)
        else:
            temp -= random.uniform(0, 1)
        if temp > 40:
            temp = 40
            tempDirUp = False
        if temp < 34:
            temp = 34
            tempDirUp = True
    '''
    if frame_count % 10 / enviormentChangeRate == 0:
        enviormentChangeRate += random.uniform(-0.5, 0.5)
        if enviormentChangeRate < 0.5:
            enviormentChangeRate = 0.5
        if enviormentChangeRate > 5:
            enviormentChangeRate = 5'''
    if should_draw:
        if display_mode >= 1:
            # Display median evolution speed below average
            if len(dots) > 0:
                median_evo_speed = statistics.median(dot.evolution_speed for dot in dots)
            else:
                median_evo_speed = 0

            text = font.render(
                f"evo median: {str(median_evo_speed)[:5]} | alive for: {longestAlive} | species #{tracker.amntOfSpecies} | medium Seepcies #{tracker.amntOfMediumSpecies} | big species #{tracker.amntOfBigSpecies}",
                True,
                (255, 255, 255),
            )
            screen.blit(text, (10, 130))

        if display_mode == 2:
            # Draw info text after all dots and labels are drawn so it overlays the dots
            # Draw info text and color squares for top performing dots
            y = 160
            for line in [info, info2, info3, info4, info5, info6, info7, info8, info9]:
                if isinstance(line, tuple) and line[0] == "color_square":
                    pygame.draw.rect(screen, line[1], (10, y, 20, 20))  # small 20x20 square
                else:
                    txt = font.render(line, True, (255, 255, 255))
                    screen.blit(txt, (40, y))  # shift text right so square doesn't overlap
                y += 30
    
        pygame.display.flip()
    clock.tick(0) # keep commented
    
    frame_count += 1
    if display_mode == 2:
        fps_tracker.update(frame_count, display_mode)
    if frame_count % META_WRITE_INTERVAL == 0:
        _write_run_meta()
    if frame_count % PRINT_INTERVAL == 0 and (not SIM_CONTROL_FILE or caption_active):
        print(frame_count)
        print(totalSim)
        if display_mode == 2:
            fps_est = fps_tracker.fps_estimate()
            if fps_tracker.last_interval_time is not None and fps_tracker.last_interval_time > 0 and fps_est is not None:
                print(f"FPS (last 1000): {fps_est:.1f}, and 1000 iters: {fps_tracker.last_interval_time}")
        print(f"amount of species: {tracker.amntOfSpecies}")
        print(tracker.amntOfSpeciesEach)

        print(f"resource pool: {nutrient.get_resources()}")
        print(f"food amnt {nutrient.foodAmnt}")
        
    
    

    
pygame.quit()
_write_run_meta(final=True)
tracker.set_should_parse(should_parse)
if __name__ == "__main__":
    if os.environ.get("SIM_RUN_POOL") == "1":
        with Pool(cpu_count()) as p:
            results = p.map(work, range(10_000))
