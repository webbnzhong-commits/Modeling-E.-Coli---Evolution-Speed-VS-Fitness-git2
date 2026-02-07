import pygame
import random
import math
#import pyautogui
import time
import csv
import statistics
from collections import Counter
from pathlib import Path
from simulatino_parser import parse_run

#from pathlib import Path


#git debug



# initialize per-run logging files

results_dir = Path("results")
results_dir.mkdir(parents=True, exist_ok=True)

counter_path = results_dir / "numTries"
try:
    run_num = int(counter_path.read_text().strip()) + 1
except Exception:
    run_num = 0
counter_path.write_text(str(run_num))

print(run_num)

run_dir = results_dir / str(run_num)
run_dir.mkdir(parents=True, exist_ok=True)

log_path_1 = run_dir / f"simulation_log_{run_num}.csv"
log_path_2 = run_dir / f"simulation_log2_{run_num}.csv"

# Create NEW files each run (overwrite if they already exist)
csv_file = open(log_path_1, "w", newline="")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(["evolution rate", "length lived", "species population time", "population"])

csv_file2 = open(log_path_2, "w", newline="")
csv_writer2 = csv.writer(csv_file2)
csv_writer2.writerow(["frame", "avg evo", "population"])

print(f"Run #{run_num} -> writing logs: {log_path_1}, {log_path_2}")


# Initialize pygame∫
pygame.init()

# Window settings
WIDTH, HEIGHT = 1280, 832
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Dots Example")

evo_speed_range = [0.05, 0.4]


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
        for r in ["o", "c", "n"]:
            debuf =  min(1, (abs(self.optimal_ph - phLevel)/2)) * min(1, (abs(self.optimal_temp - temp)))

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
                child.color = (child.color[0], child.color[1], child.color[2], 0.5)  # visually distinct color for "cancerous" dots
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
            self.resource_pool[r] += random.randint(-1, 1)

        # Prevent negative values
        for r in ["o", "c", "n"]:
            if self.resource_pool[r] < 0:
                self.resource_pool[r] = 0

        # Normalize so that o + c + n ≈ 100
        total = self.resource_pool["o"] + self.resource_pool["c"] + self.resource_pool["n"]
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
            low = int(max(1, min(low, 600)))
            high = int(max(low + 1, min(high, 600)))

            self.goingToAmnt = random.randint(low, high)

        

    def get_resources(self):
        const = len(dots) * 100
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
amntOfSpecies               = 0
amntOfMediumSpecies         = 0
amntOfBigSpecies            = 0
amntOfSpeciesEach           = ""
species_trackers            = {}

temp = 37.0



population_cap              = 0.5
fightChance                 = 5


phLevel                     = 7.0


draw = False
drawSometimes = True
drawAmnt = 100

enviormentChangeRate = 1

font = pygame.font.SysFont("Consolas", 20)

tempDirUp = True


def wrte_info(evo_val, data):
    global amntOfBigSpecies, amntOfMediumSpecies, amntOfSpecies, amntOfSpeciesEach

    # Track counts by lifespan bucket
    if data["lifespan"] > 1999:
        amntOfBigSpecies += 1
        amntOfSpeciesEach += f"{data['lifespan']}: {str(evo_val)[:5]}, "
    elif data["lifespan"] > 500:
        amntOfMediumSpecies += 1

    population_when_dead = data["pop_time"] // data["lifespan"]

    csv_writer.writerow([
        evo_val,
        data["lifespan"],
        data["pop_time"],
        population_when_dead
    ])

    amntOfSpecies += 1
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            for evo_val, data in list(species_trackers.items()):
                    if data["alive"]:
                        data["alive"] = False
                        if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                            wrte_info(evo_val, data)
            should_parse = True
            running = False
        elif event.type == pygame.KEYDOWN:  # key pressed
            if event.key == pygame.K_SPACE:
                for evo_val, data in list(species_trackers.items()):
                    if data["alive"]:
                        data["alive"] = False
                        if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                            wrte_info(evo_val, data)
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
                            wrte_info(evo_val, data)
                should_parse = True
                running = False
            if event.key == pygame.K_d:
                draw = not draw
            if event.key == pygame.K_s:
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
    if frame_count % 100 == 0:
        population = len(dots)
        if population > 0:
            evo_speeds = [dot.evolution_speed for dot in dots]
            median_evo_speed = statistics.median(evo_speeds)
            csv_writer2.writerow([frame_count, median_evo_speed, population])

            # Write individual species data if they exceed 20% of total population
            evo_groups = {}
            for dot in dots:
                key = round(dot.evolution_speed, 2)
                evo_groups.setdefault(key, []).append(dot)
            for evo_val, group in evo_groups.items():
                pct = len(group) / population
                if pct > 0.2:
                    csv_writer2.writerow([frame_count, evo_val, len(group), f"{pct*100:.2f}%"])
        else:
            median_evo_speed = 0

    evo_speeds = [dot.evolution_speed for dot in dots]
    median_evo_speed = statistics.median(evo_speeds)
    

    
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
                        wrte_info(evo_val, data)
            dots, nutrient = reset_simulation()
            species_trackers = {}
            totalSim += 1
            break
            
        if evo_val not in active_species and data["alive"]:
            
            data["alive"] = False
            if data["lifespan"] != data["pop_time"]: #they reproduced at least once
                wrte_info(evo_val, data)
            
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
    # Display median evolution speed below average
    if len(dots) > 0:
        median_evo_speed = statistics.median(dot.evolution_speed for dot in dots)
    else:
        median_evo_speed = 0

    text = font.render(
        f"evo median: {str(median_evo_speed)[:5]} | alive for: {longestAlive} | species #{amntOfSpecies} | medium Seepcies #{amntOfMediumSpecies} | big species #{amntOfBigSpecies}",
        True,
        (255, 255, 255)
        )
    
    if draw or (drawSometimes and frame_count % drawAmnt == 0):
        screen.blit(text, (10, 130))

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
    #clock.tick(60) # keep commented
    
    frame_count += 1
    if frame_count % 1000 == 0:
        now = time.perf_counter()
        last_1000_time = now - last_1000_start
        last_1000_start = now
    if frame_count % 10 == 0:
        print (frame_count)
        print (totalSim)
        print (f"amount of species: {amntOfSpecies}")
        print (amntOfSpeciesEach)

        print (f"resource pool: {nutrient.resource_pool}")
        print(f"food amnt {nutrient.foodAmnt}")



    
pygame.quit()
csv_file.close()
csv_file2.close()
if should_parse:
    try:
        parse_run(results_dir, run_num)
    except Exception as e:
        print(f"Failed to parse results: {e}")
