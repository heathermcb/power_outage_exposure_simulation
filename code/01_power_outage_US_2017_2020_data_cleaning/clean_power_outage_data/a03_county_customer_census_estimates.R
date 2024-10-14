# This script uses estimates from EIA of the number of electrical customers in 
# a state, and census estimates of the number of households and establishments 
# in counties within states, to get estimates of the number of electrical 
# customers in each county in each state. Each county is allocated customers 
# from EIA totals based on the percentage of households and establishments in 
# the state that are in that county. 

# Author: Heather
# Last date updated: Oct 2nd, 2024

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here)

# Read --------------------------------------------------------------------

# customer estimates
census_cust_estimates <-
  read_rds(
    here(
      "data",
      "power_outage_medicare_data_cleaning_output",
      "county_level_census_cust_estimates.RDS"
    ))

# eia totals
eia_totals <-
  read_rds(
    here(
      "data",
      "power_outage_medicare_data_cleaning_output",
      "eia_state_total_customers_by_year.RDS"
    )
  ) 

# Do ----------------------------------------------------------------------

# join state estimates to county frame
downscaled <- census_cust_estimates %>%
  left_join(eia_totals) %>%
  select(
    state_fips, 
    state_ab,
    five_digit_fips, 
    year,
    eia_state_total_cust,
    census_customer_estimate
  )

# calculate a census estimate for each state
downscaled <- downscaled %>%
  group_by(year, state_fips) %>%
  mutate(census_est_state_total = 
           sum(census_customer_estimate, na.rm = TRUE)) %>%
  ungroup()

# calculate the percentage of the state customer population in each county
downscaled <-
  downscaled %>%
  mutate(p_of_state = census_customer_estimate / census_est_state_total)

# calculate how many electrical customers should be in each county according 
# to the EIA estimates based on the proportion of the state pop in each county
downscaled <- downscaled %>%
  mutate(downscaled_county_estimate = round(p_of_state * eia_state_total_cust))

downscaled <- downscaled %>%
  select(year,
         state_ab,
         state_fips,
         five_digit_fips,
         downscaled_county_estimate)

# Write -------------------------------------------------------------------

write_rds(
  downscaled,
  here(
    "data",
    "power_outage_medicare_data_cleaning_output",
    "downscaled_county_customer_estimates.RDS"
  )
)


