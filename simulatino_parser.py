import csv
import math
import numpy as np
from scipy.stats import pearsonr
from numpy.polynomial.polynomial import Polynomial

# Input and output file names
num = 36
input_file = f"simulation_log_{num}.csv"
# Output files (one geometric mean, one arithmetic mean)
output_file_geo = f"parsedGeometricMeanSimulatino{num}_Log.csv"
output_file_mean = f"parsedArithmeticMeanSimulatino{num}_Log.csv"

# Read data from simulation_log.csv
data = []
with open(input_file, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    
    for row in reader:
        
        # Convert numeric fields to float
        try:
            evo_speed = float(row["evolution rate"])
            length_lived = float(row["length lived"])
            species_pop_time = float(row["species population time"])
            population = float(row["population"])
            data.append({
                "evolution rate": evo_speed,
                "length lived": length_lived,
                "species population time": species_pop_time,
                "population": population
            })
        except ValueError:
            # Skip rows with invalid data
            break

# Prepare bins for evolution speed: 0.000, 0.001, ..., 0.300
step = 0.001
parsed_data_geo = []
parsed_data_mean = []
current_speed = 0.0
while current_speed <= 0.3 + 1e-9:  # include 0.3
    # Filter rows within this evolution speed bin (allowing for floating point rounding)
    rows_in_bin = [row for row in data if abs(row["evolution rate"] - current_speed) < step/2]
    
    if rows_in_bin:
        # Compute both geometric mean and arithmetic mean for length lived and species population time
        def geometric_mean(values, scale=100.0):
            # geometric mean only works for positive values
            vals = [v for v in values if v > 0]
            if not vals:
                return 0.0
            logs = [math.log(v / scale) for v in vals]
            return math.exp(sum(logs) / len(logs)) * scale

        def arithmetic_mean(values):
            return sum(values) / len(values) if values else 0.0

        length_values = [row["length lived"] for row in rows_in_bin]
        species_pop_values = [row["species population time"] for row in rows_in_bin]

        geo_length_lived = geometric_mean(length_values)
        geo_species_pop_time = geometric_mean(species_pop_values)

        mean_length_lived = arithmetic_mean(length_values)
        mean_species_pop_time = arithmetic_mean(species_pop_values)

        parsed_data_geo.append({
            "evolution rate": round(current_speed, 3),
            "geometric mean length lived": geo_length_lived,
            "geometric mean species population time": geo_species_pop_time,
        })

        parsed_data_mean.append({
            "evolution rate": round(current_speed, 3),
            "arithmetic mean length lived": mean_length_lived,
            "arithmetic mean species population time": mean_species_pop_time,
        })
    else:
        # No data for this evolution speed, record as empty or 0
        """
        parsed_data.append({
            "evolution rate": round(current_speed, 3),
            "max length lived": 0,
            "population at max length lived": 0,
            "max species population time": 0,
            "population at max species population time": 0
        })
        """
    
    current_speed += step

# Write geometric-mean parsed results
with open(output_file_geo, "w", newline="") as csvfile:
    fieldnames = ["evolution rate", "geometric mean length lived",
                  "geometric mean species population time"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in parsed_data_geo:
        writer.writerow(row)

# Write arithmetic-mean parsed results
with open(output_file_mean, "w", newline="") as csvfile:
    fieldnames = ["evolution rate", "arithmetic mean length lived",
                  "arithmetic mean species population time"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in parsed_data_mean:
        writer.writerow(row)


# Define x and y from parsed data (both geometric and arithmetic)
x_geo = np.array([row["evolution rate"] for row in parsed_data_geo])
y_geo = np.array([row["geometric mean length lived"] for row in parsed_data_geo])

x_mean = np.array([row["evolution rate"] for row in parsed_data_mean])
y_mean = np.array([row["arithmetic mean length lived"] for row in parsed_data_mean])

def print_stats(label, x, y):
    r, p_value = pearsonr(x, y)
    r_squared = r**2
    print(f"\n--- {label} ---")
    print(f"Pearson correlation coefficient (r): {r}")
    print(f"P-value: {p_value}")
    print(f"Coefficient of determination (r^2): {r_squared}")

    # Linear fit
    linear_coeffs = np.polyfit(x, y, 1)
    slope, intercept = linear_coeffs
    print(f"Linear equation: y = {slope:.4f} * x + {intercept:.4f}")
    y_pred_linear = np.polyval(linear_coeffs, x)

    # Quadratic fit
    quadratic_coeffs = np.polyfit(x, y, 2)
    print(
        f"Quadratic equation: y = {quadratic_coeffs[0]:.4f} * x^2 + "
        f"{quadratic_coeffs[1]:.4f} * x + {quadratic_coeffs[2]:.4f}"
    )
    y_pred_quad = np.polyval(quadratic_coeffs, x)

    # R^2 comparison
    ss_res_linear = np.sum((y - y_pred_linear) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2_linear = 1 - ss_res_linear / ss_tot if ss_tot != 0 else 0.0

    ss_res_quad = np.sum((y - y_pred_quad) ** 2)
    r2_quad = 1 - ss_res_quad / ss_tot if ss_tot != 0 else 0.0

    print(f"Linear model R^2: {r2_linear:.4f}")
    print(f"Quadratic model R^2: {r2_quad:.4f}")

print_stats("Geometric mean", x_geo, y_geo)
print_stats("Arithmetic mean", x_mean, y_mean)

print(f"Parsed geometric-mean data saved to {output_file_geo}")
print(f"Parsed arithmetic-mean data saved to {output_file_mean}")