# Join customer estimates from EIA to the live customer estimate data,
# calculate person-coverage, and then eliminate counties with insufficient
# coverage.

# Libraries ---------------------------------------------------------------

library(tidyverse)
library(here)
library(lubridate)
library(zoo)
library(data.table)
library(fst)

my.max <- function(x)
  ifelse(!all(is.na(x)), max(x, na.rm = T), NA)

person_coverage_threshold <- 0.5

# Do ----------------------------------------------------------------------

# read in EIA estimates downscaled to the county level in an earlier script 
eia_estimates <-
  fread(
    here(
      "power_outage_medicare_data",
      "power_outage_medicare_data_cleaning_output",
      "downscaled_county_customer_estimates.csv"
    )
  )

# harmonize fips 
eia_estimates <-
  eia_estimates[, state_fips := str_pad(state_fips, 2, pad = "0")
  ][, county_fips := str_pad(county_fips, 3, pad = '0')]

eia_estimates <- 
  eia_estimates %>%
  mutate(fips = paste0(state_fips, county_fips)) %>%
  select(year, fips, downscaled_county_estimate)

# read in counties hourly data 
counties <-
  list.files(
    here(
      "power_outage_medicare_data",
      "power_outage_medicare_data_cleaning_output",
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
    fips,
    customers_served_hourly,
    county_person_time_missing
  ) %>% distinct()

# Do ----------------------------------------------------------------------

pous_based_estimates <- pous_based_estimates %>% left_join(eia_estimates)

# mark when the pous data looks like it has an error 
pous_based_estimates <- pous_based_estimates %>%
  mutate(
    too_big = case_when(
      customers_served_hourly > downscaled_county_estimate ~ customers_served_hourly /
        downscaled_county_estimate,
      T ~ 0
    )
  )

# select the appropriate columns 
pous_based_estimates <- pous_based_estimates[, .(
  clean_state_name,
  clean_county_name,
  fips,
  year,
  hour,
  customers_served_hourly,
  county_person_time_missing,
  downscaled_county_estimate,
  too_big
)]

# going to use the POUS estimates when they are less than twice the eia estimate
pous_based_estimates <- pous_based_estimates %>%
  mutate(
    customers_served_estimate_to_use = case_when(
      too_big < 2 ~ customers_served_hourly,
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
    fips,
    year,
    customers_served_hourly,
    county_person_time_missing,
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
    hrs_actually_served = hrs_served - (county_person_time_missing /
                                          6),
    # subtract missing hrs
    p_present = hrs_actually_served / expected_hrs
  ) # percentage served out of total hrs


hourly <- hourly %>% left_join(estimate_missing)

hourly <- hourly %>% filter(p_present > 0.5)

# Write -------------------------------------------------------------------

fwrite(
  hourly,
  here(
    "power_outage_medicare_data",
    "power_outage_medicare_data_cleaning_output",
    "hourly_data_with_coverage_exclusions.csv"
  )
)
