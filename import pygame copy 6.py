import pygame
import random
import math
#import pyautogui
import time
import statistics
import csv
from collections import Counter
from pathlib import Path
from data_tracking import RunDataTracker
from simulatino_parser import parse_run
from settings_manager import load_settings, save_settings

#from pathlib import Path


#git debug



# initialize per-run logging
tracker = RunDataTracker()
run_num = tracker.run_num


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
        self.speed = [random.uniform(-2, 2.0), random.uniform(-2, 2.0)]
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

        if reproduce: #it can reproduce
            # Deduct reproduction resources
            
            
            
            self.resources["o"] -= self.reproduction_resource["o"]
            self.resources["c"] -= self.reproduction_resource["c"]
            self.resources["n"] -= self.reproduction_resource["n"]

            
            # Create offspring with slight mutations 
            child = Dot(self.x, self.y)
            child.evolution_speed = max(0.001, self.evolution_speed) # + random.uniform(-0.01, 0.01))


            #set child traits based on parent trait and evolution speed
            child.size = max(1, self.size + (random.uniform(-child.evolution_speed, child.evolution_speed)))
            
            child.favored_resource = self.favored_resource
            child.immune_system = self.immune_system
            child.optimal_ph = self.optimal_ph + random.uniform(-child.evolution_speed * 2, child.evolution_speed * 2)
            child.optimal_temp = self.optimal_temp + random.uniform(-child.evolution_speed * 2, child.evolution_speed * 2)
            child.color = self.color.copy()
            #child.immune_system = min(5, max(0, self.immune_system + int(random.uniform(-child.evolution_speed*3.4, child.evolution_speed*3.4))))
            if self.evolution_speed > 0.17:
                child.immune_system = child.immune_system
                child.immune_system = random.randint(0, 5)
                #child.immune_system = int(min(5, max(0, self.immune_system + random.uniform(-self.evolution_speed, self.evolution_speed) * 20)))
                #child.immune_system = self.immune_system


            #child.max_cycles = max(10, self.max_cycles + int(random.uniform(-child.evolution_speed*5.5, child.evolution_speed*5.5)))
            total  = 0
            total2 = 0
            for r in ["o", "c", "n"]:
                mutation = int(random.uniform(-child.evolution_speed, child.evolution_speed) /4) 
                child.reproduction_resource[r] = max(1, self.reproduction_resource[r] + mutation)
                if child.favored_resource != r:
                    total2 += child.reproduction_resource[r]
                total += child.reproduction_resource[r]
            
            # Normalize reproduction rates to keep total similar
            factor = child.size / total * 2
            for r in ["o", "c", "n"]:
                child.reproduction_resource[r] = max(1, int(child.reproduction_resource[r] * factor))   
            
            child.reproduction_resource[child.favored_resource] = max(1 * child.size / 2, child.reproduction_resource[child.favored_resource])
            

            '''
            x = 2y
            (y / y+x) = 1/2
            '''
            #child.sameResourceNeeds()

            

            if random.uniform(0, child.evolution_speed) < 0.10: #random chance for cancer to happend, where the child can't reproduce or smth
                dots.append(child)
            else:
                for r in ["o", "c", "n"]:
                    child.reproduction_resource[r] = float('inf')  # can't reproduce if any resource is infinite
                child.cancerous = True
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
last_1000_start = time.perf_counter()
last_1000_time = None
iter_1000_times = []
avgSpecies = []






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


while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            for evo_val, data in list(species_trackers.items()):
                    if data["alive"]:
                        data["alive"] = False
                        if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                            tracker.write_species_info(evo_val, data)
            should_parse = True
            running = False
        elif event.type == pygame.KEYDOWN:  # key pressed
            if event.key == pygame.K_SPACE:
                for evo_val, data in list(species_trackers.items()):
                    if data["alive"]:
                        data["alive"] = False
                        if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                            tracker.write_species_info(evo_val, data)
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
                            tracker.write_species_info(evo_val, data)
                should_parse = True
                running = False
            if event.key == pygame.K_d:
                draw = not draw
                settings["draw"] = draw
                save_settings(settings)
            if event.key == pygame.K_h:
                display_mode = (display_mode + 1) % 3
                settings["display_mode"] = display_mode
                save_settings(settings)
            if event.key == pygame.K_u:
                reload_settings()
            if event.key == pygame.K_s and display_mode == 2:
                # Get common evolution speeds
                evo_speeds = [round(dot.evolution_speed, 2) for dot in dots] if len(dots) > 0 else []
                
                try:
                    speed_counts = Counter(evo_speeds)
                    common = speed_counts.most_common()
                    infos = []  # clear old
                    total_dots = len(dots)

                    # Collect all modes >=10%
                    qualified = []
                    for speed, _ in common:
                        mode_dots = [dot for dot in dots if round(dot.evolution_speed, 2) == speed]
                        count = len(mode_dots)
                        if total_dots > 0 and count / total_dots >= 0.10:
                            qualified.append((speed, mode_dots, count))

                    # If fewer than 3 qualifying modes, repeat top mode to fill
                    if len(qualified) == 0:
                        qualified = [(common[0][0], [dot for dot in dots if round(dot.evolution_speed,2)==common[0][0]], speed_counts[common[0][0]])]
                    while len(qualified) < 3:
                        qualified.append(qualified[0])

                    # Build info lines for up to 3 modes
                    for idx in range(3):
                        speed, mode_dots, count = qualified[idx]
                        pct = round((count / total_dots) * 100, 2) if total_dots > 0 else 0
                        chosen = random.choice(mode_dots) if mode_dots else None
                        if chosen:
                            infos.append(
                                f"{pct}% Mode {str(speed)[0:5]} Size:{str(chosen.size)[0:5]} "
                                f" Imm:{str(chosen.immune_system)[0:5]} "
                                f"Fav:{chosen.favored_resource}  Cyc:{str(chosen.max_cycles)[0:5]} Evo Speed :{str(chosen.evolution_speed)[0:5]} "
                                
                            )
                            # Add color square
                            infos.append(("color_square", chosen.color))
                            infos.append(
                                f"Needs o:{str(chosen.reproduction_resource['o'])[0:5]} "
                                f"c:{str(chosen.reproduction_resource['c'])[0:5]} "
                                f"n:{str(chosen.reproduction_resource['n'])[0:5]}"
                            )
                        else:
                            infos.append("No dots")
                            infos.append("")

                    # Assign display info variables
                    while len(infos) < 9:  # ensure enough lines for drawing
                        infos.append("")

                    info, info2, info3, info4, info5, info6, info7, info8, info9 = infos[:9]
                except Exception as e:
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
    if pause:
        continue
    
    if draw or (drawSometimes and frame_count % drawAmnt == 0):
        screen.fill((0, 0, 0))

    # Display frame count
    
    
    
    # (Text labels moved to after drawing dots)

    # Calculate mode evolution speed and pick a random dot with that speed
    

    
    if len(dots) <= 5:
        
        dots.append(Dot(random.randint(0, WIDTH), random.randint(0, HEIGHT)))
            
    
    

    # (Info text rendering moved to after drawing dots)
    
    

    

        

    
    nutrient.regenerate_resources()
    for dot in list(dots):
        dot.collect_resources()
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
                        tracker.write_species_info(evo_val, data)
            dots, nutrient = reset_simulation()
            species_trackers = {}
            totalSim += 1
            break
            
        if evo_val not in active_species and data["alive"]:
            
            data["alive"] = False
            if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                tracker.write_species_info(evo_val, data)
            
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
    
    for dot in list(dots):
        dot.update()
        if draw or (drawSometimes and frame_count % drawAmnt == 0):
            dot.draw(screen)

    # Draw text labels (frame count, population, evo avg, evo median, etc.) after all dots are drawn
    if draw or (drawSometimes and frame_count % drawAmnt == 0):
        if display_mode >= 1:
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
            if last_1000_time is not None and last_1000_time > 0:
                fps_est = 1000 / last_1000_time
                time_1000_text = f"1000 iters: {last_1000_time:.2f}s @ {fps_est:.1f} FPS"
            else:
                time_1000_text = "1000 iters: --"
            text = font.render(time_1000_text, True, (255, 255, 255))
            screen.blit(text, (10, 80))
            text = font.render(f"evo avg: {avg_evo_speed}", True, (255, 255, 255))
            screen.blit(text, (10, 110))

            # Graph of 1000-iteration times (0-2s) to the right of the HUD
            graph_x, graph_y = FPS_GRAPH_X, FPS_GRAPH_Y
            graph_w, graph_h = FPS_GRAPH_W, FPS_GRAPH_H
            pygame.draw.rect(screen, (80, 80, 80), (graph_x, graph_y, graph_w, graph_h), 1)
            # y-axis labels (0s at bottom, 2s at top)
            label_top = font.render("2.0s", True, (200, 200, 200))
            label_bot = font.render("0.0s", True, (200, 200, 200))
            screen.blit(label_top, (graph_x + graph_w + 5, graph_y - 5))
            screen.blit(label_bot, (graph_x + graph_w + 5, graph_y + graph_h - 15))
            if len(iter_1000_times) > 0:
                mean_1000 = sum(iter_1000_times) / len(iter_1000_times)
                mean_text = font.render(f"-Mean: {mean_1000:.2f}s", True, (200, 200, 200))
                screen.blit(mean_text, (graph_x + graph_w + 5, graph_y - mean_1000 + graph_h - mean_text.get_height()))
                max_points = graph_w  # one pixel per point
                recent = iter_1000_times[-max_points:]
                for i, tval in enumerate(recent):
                    t_clamped = max(0.0, min(2.0, tval))
                    px = graph_x + i
                    py = graph_y + graph_h - int((t_clamped / 2.0) * graph_h)
                    pygame.draw.circle(screen, (0, 200, 255), (px, py), 2)

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
    if draw or (drawSometimes and frame_count % drawAmnt == 0):
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
    
    if frame_count % 1000 == 0:
        now = time.perf_counter()
        last_1000_time = now - last_1000_start
        if display_mode >= 1:
            last_1000_time = now - last_1000_start
            iter_1000_times.append(last_1000_time)
        last_1000_start = now
    if frame_count % 1000 == 0:
        print (frame_count)
        print (totalSim)
        if last_1000_time is not None and last_1000_time > 0:
            fps_est = 1000 / last_1000_time
            print(f"FPS (last 1000): {fps_est:.1f}, and 1000 iters: {last_1000_time}")
        print (f"amount of species: {tracker.amntOfSpecies}")
        print (tracker.amntOfSpeciesEach)

        print (f"resource pool: {nutrient.get_resources()}")
        print(f"food amnt {nutrient.foodAmnt}")
        



    
pygame.quit()
tracker.set_should_parse(should_parse)
