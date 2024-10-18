# Create hospitalization data

# Libraries ---------------------------------------------------------------

pacman::p_load(here, tidyverse, data.table, gtools)

# Constants ---------------------------------------------------------------

baseline_hosp_rate <- 0.001 # putting this at .1% of people per day
# may update once we have real or simulated medicare data to pull from

# these are the rate changes with outages, for different effect sizes
effect_of_outage_0.5_p <- 0.000005
effect_of_outage_1_p <- 0.00001 
effect_of_outage_5_p <- 0.00005


# Read --------------------------------------------------------------------

exp_data <- readRDS(here(
  'data',
  'power_outage_simulation_cleaned_data',
  'missingness_grid.RDS'
))


