# Identify power outage events, defining power outage based on cut point 
# percentage of customers out and a duration. 

# Author: Heather
# Created: sometime in 2022
# Last updated: Oct 7th, 2024

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here, lubridate, data.table, fst)
source(here("code", 'functions', 'helpers_days_exposed_unexposed.R'))

# Read --------------------------------------------------------------------

counties <-
  read_fst(here("data",
                "power_outage_exposure_data_cleaning_output", 
                "hourly_data_with_coverage_exclusions.fst")) |>
  as.data.table()

counties <-
  counties[, .(
    clean_state_name,
    clean_county_name,
    five_digit_fips,
    year,
    hour,
    customers_out_hourly = customers_out_hourly_locf,
    customers_served_total = downscaled_county_estimate
  )]

# Get exposures -----------------------------------------------------------

# call the get_exposure function from helpers, on all cut points and durations
# of interest.

cut_points <- c(0.005, 0.01, 0.03, 0.05)

exposure_8_hrs <- lapply(cut_points, function(cut_point) {
  get_exposure(counties, cut_point, outage_duration = hours(8)) 
})

print('8 hrs done')

exposure_12_hrs <- lapply(cut_points, function(cut_point) {
  get_exposure(counties, cut_point, outage_duration = hours(12))  
})

print('12 hrs done')

exposure_4_hrs <- lapply(cut_points, function(cut_point) {
  get_exposure(counties, cut_point, outage_duration = hours(4))  
})

print('12 hrs done')

# Join --------------------------------------------------------------------

all_exposures <- list(exposure_8_hrs, exposure_4_hrs, exposure_12_hrs)

combined_df <- Reduce(function(x, y)
  merge(x, y, by = c(
    "clean_state_name", "clean_county_name", 'five_digit_fips', "day"
  )), all_exposures)

exposure_cols <- grep("^exposed", names(combined_df), value = TRUE)

combined_df <- combined_df %>% select(
  clean_state_name,
  clean_county_name,
  five_digit_fips,
  day,
  all_of(exposure_cols))
  
# Write -------------------------------------------------------------------

write_fst(
  combined_df,
  here(
    "data",
    "power_outage_exposure_data_cleaning_output",
    "all_days_exposed_unexposed.fst"
  )
)
