# This script will take datasets with different percentages of missingness
# and splice them together to create datasets with different percentages
# of counties missing different percentages of obs. for a 9x9 grid.

# Author: Heather
# Last updated: Oct 17th, 2024

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, data.table, here, purrr)

# Helpers -----------------------------------------------------------------

# helper function to create columns with specified missingness 
create_col_with_m <- function(df,
                              num_counties_affected,
                              percent_missing_in_affected,
                              col_to_pull_w_missing,
                              col_to_pull_reference) {
  new_col_name <- paste0('m_col_',
                         num_counties_affected,
                         'p_missing_',
                         percent_missing_in_affected)
  df[[new_col_name]] <- 
    ifelse(df$within_group_id <= num_counties_affected, 
           df[[col_to_pull_w_missing]], 
           df[[col_to_pull_reference]])
  
  return(df)
}

# helper function to apply create_col_with_m and combine results
apply_and_combine <- function(percent_missing, col_to_pull_w_missing) {
  results <- lapply(
    num_counties_affected,
    create_col_with_m,
    df = exp_dat,
    percent_missing_in_affected = percent_missing,
    col_to_pull_w_missing = col_to_pull_w_missing,
    col_to_pull_reference = col_to_pull_reference
  )
  combined <- Reduce(function(x, y)
    merge(x, y, by = intersect(names(x), names(y)), all.x = TRUE), results)
  return(combined)
}

# Read --------------------------------------------------------------------

exp_dat <- readRDS(
  here(
    'data',
    'power_outage_simulation_cleaned_data',
    'days_exposed_unexposed_all_missingness.RDS'
  )
)

# Create group_id and within_group_id columns -----------------------------

# create a group_id column for each group of counties that will be one 
# simulation
# 100 groups, each with 100 counties, each with 366 days
exp_dat$group_id <- rep(1:100, each = 100 * 366)  

# create a within_group_id column that goes from 1 to 100 within each group
exp_dat$within_group_id <- rep(rep(1:100, each = 366), times = 100)

# Parameters --------------------------------------------------------------

num_counties_affected <- c(20, 50, 80)

percent_missing <- c(20, 50, 80)

col_to_pull_w_missing <- c(
  'exposed_8_hrs_0.005_20_p_missing',
  'exposed_8_hrs_0.005_50_p_missing',
  'exposed_8_hrs_0.005_80_p_missing'
)

col_to_pull_reference <- 'exposed_8_hrs_0.005_none_missing'

# Do ----------------------------------------------------------------------

# apply for each percent missing and combine all results
all_combined <- map2(percent_missing,
                     col_to_pull_w_missing,
                     ~ apply_and_combine(.x, .y))

all_combined <- Reduce(function(x, y)
  merge(x, y, by = intersect(names(x), names(y)), all.x = TRUE), all_combined)

# Write -------------------------------------------------------------------

write_rds(
  all_combined,
  here(
    'data',
    'power_outage_simulation_cleaned_data',
    'missingness_grid.RDS'
  )
)
