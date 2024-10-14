# Join customer estimates from EIA to the live customer estimate data,
# calculate person-coverage, and then eliminate counties with insufficient
# coverage.

# Author: Heather
# Last updated: Oct 4th, 2024

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here, lubridate, zoo, data.table, fst)

my.max <- function(x)
  ifelse(!all(is.na(x)), max(x, na.rm = T), NA)

person_coverage_threshold <- 0.5

# Do ----------------------------------------------------------------------

# read in EIA estimates downscaled to the county level in an earlier script 
eia_estimates <-
  read_rds(
    here(
      "data",
      "power_outage_exposure_data_cleaning_output",
      "downscaled_county_customer_estimates.RDS"
    )
  ) %>%
  select(year, five_digit_fips, downscaled_county_estimate)

# read in counties hourly data 
counties <-
  list.files(
    here(
      "data",
      "power_outage_exposure_data_cleaning_output",
      "hourly_county"
    ),
    pattern = "*.fst",
    full.names = TRUE
  )

hourly <- lapply(counties, read_fst)
hourly <- rbindlist(hourly)

# reduce to just customer estimates
pous_based_estimates <- hourly %>%
  select(
    clean_state_name,
    clean_county_name,
    five_digit_fips,
    year,
    customers_served_county,
    county_person_time_missing_hours,
  ) %>% distinct()

# Do ----------------------------------------------------------------------

pous_based_estimates <- pous_based_estimates %>% left_join(eia_estimates)

# mark when the pous data looks like it has an error 
pous_based_estimates <- pous_based_estimates %>%
  mutate(
    too_big = case_when(
      customers_served_county > downscaled_county_estimate ~ 
        customers_served_county /
        downscaled_county_estimate,
      T ~ 0
    )
  )

# select the appropriate columns 
pous_based_estimates <- pous_based_estimates[, .(
  clean_state_name,
  clean_county_name,
  five_digit_fips,
  year,
  hour,
  customers_served_county,
  county_person_time_missing_hours,
  downscaled_county_estimate,
  too_big
)]

# going to use the POUS estimates when they are less than twice the eia estimate
pous_based_estimates <- pous_based_estimates %>%
  mutate(
    customers_served_estimate_to_use = case_when(
      too_big < 2 ~ customers_served_county,
      T ~ downscaled_county_estimate
    )
  )

# exclude low person coverage - should update this to be person-time coverage,
# and as an indicator; first collect cols 
estimate_missing <-
  pous_based_estimates %>%
  select(
    clean_state_name,
    clean_county_name,
    five_digit_fips,
    year,
    customers_served_county,
    county_person_time_missing_hours,
    downscaled_county_estimate,
    customers_served_estimate_to_use
  ) %>%
  distinct()

# do calc
estimate_missing <-
  estimate_missing %>%
  mutate(
    expected_hrs = downscaled_county_estimate * 365 * 24,
    # hrs that should be in the dataset
    hrs_served = customers_served_estimate_to_use * 365 * 24,
    # person-hrs that should be in the dataset
    hrs_actually_served = hrs_served - (county_person_time_missing_hours),
    # subtract missing hrs
    p_present = hrs_actually_served / expected_hrs
  ) # percentage served out of total hrs


hourly <- hourly %>% left_join(estimate_missing)

# Write -------------------------------------------------------------------

write_fst(
  hourly,
  here(
    "data",
    "power_outage_exposure_data_cleaning_output",
    "hourly_data_with_coverage_exclusions.fst"
  )
)


# also write a denom frame
estimate_missing <-
  estimate_missing %>%
  select(five_digit_fips,
         year,
         county_customers = downscaled_county_estimate,
         percent_served = p_present)

write_fst(
  estimate_missing,
  here(
    'data',
    'power_outage_exposure_data_cleaning_output',
    'county_customer_denoms_and_p_missing.fst'
  )
)
