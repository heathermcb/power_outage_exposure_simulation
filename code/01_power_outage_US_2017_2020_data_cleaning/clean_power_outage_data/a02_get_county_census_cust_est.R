# This script uses census data to find the number of households by county and 
# by year, and as well as the number of establishments by county and by year,
# which we will use to get estimates of the number of electrical customers by 
# county, for years 2017-2020.

# Author: Heather
# Date updated: Oct 2nd, 2024

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here)

# Read --------------------------------------------------------------------

# load fips of states and counties we wish to include
county_list <- read_rds(here('data', 'cotus_county_list_of_fips.RDS'))

# Households --------------------------------------------------------------

# 2017 occupied housing units
hh_2017 <-
  read_csv(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "households_census_data",
      "nhgis0006_ds233_20175_county_E.csv"
    )
  ) %>%
  select(
    state = STATE,
    state_fips = STATEA,
    county = COUNTY,
    county_fips = COUNTYA,
    total_occupied_hh = AH37E001
  ) %>%
  mutate(year = 2017)

# 2018 occupied hh
hh_2018 <-
  read_csv(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "households_census_data",
      "nhgis0006_ds239_20185_county_E.csv"
    )
  ) %>%
  select(
    state = STATE,
    state_fips = STATEA,
    county = COUNTY,
    county_fips = COUNTYA,
    total_occupied_hh = AJ1UE001
  ) %>%
  mutate(year = 2018)

# 2019 occupied hh
hh_2019 <-
  read_csv(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "households_census_data",
      "nhgis0006_ds244_20195_county_E.csv"
    )
  ) %>%
  select(
    state = STATE,
    state_fips = STATEA,
    county = COUNTY,
    county_fips = COUNTYA,
    total_occupied_hh = ALZLE001
  ) %>%
  mutate(year = 2019)

# 2019 occupied hh
hh_2020 <-
  read_csv(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "households_census_data",
      "nhgis0006_ds249_20205_county_E.csv"
    )
  ) %>%
  select(
    state = STATE,
    state_fips = STATEA,
    county = COUNTY,
    county_fips = COUNTYA,
    total_occupied_hh = AMUFE001
  ) %>%
  mutate(year = 2020)

# join all yrs
hh_all_years <- bind_rows(hh_2018, hh_2019, hh_2020)

# Establishments ----------------------------------------------------------

estab_2018 <-
  read_csv(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "establishments_census_data",
      "CBP2018.CB1800CBP_data_with_overlays_2022-07-22T114055.csv"
    ),
    skip = 1
  ) %>%
  select(
    id,
    county = `Geographic Area Name`,
    naics_code = `Meaning of NAICS code`,
    size = `Meaning of Employment size of establishments code`,
    num_of_estab = `Number of establishments`
  )

estab_2018 <- estab_2018 %>%
  filter(naics_code == 'Total for all sectors' &
           size == 'All establishments') %>%
  mutate(
    state_fips = str_sub(id, start = 10, end = 11),
    county_fips = str_sub(id, start = 12, end = 14)
  ) %>%
  select(county_name_long = county, state_fips, county_fips, num_of_estab) %>%
  mutate(year = 2018)


estab_2019 <-
  read_csv(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "establishments_census_data",
      "CBP2019.CB1900CBP_data_with_overlays_2022-07-22T114003.csv"
    ),
    skip = 1
  ) %>%
  select(
    id,
    county = `Geographic Area Name`,
    naics_code = `Meaning of NAICS code`,
    size = `Meaning of Employment size of establishments code`,
    num_of_estab = `Number of establishments`
  )

estab_2019 <- estab_2019 %>%
  filter(naics_code == 'Total for all sectors' &
           size == 'All establishments') %>%
  mutate(
    state_fips = str_sub(id, start = 10, end = 11),
    county_fips = str_sub(id, start = 12, end = 14)
  ) %>%
  select(county_name_long = county, state_fips, county_fips, num_of_estab) %>%
  mutate(year = 2019)

estab_2020 <- read_csv(
  here(
    "data",
    "power_outage_medicare_data_cleaning_raw_data",
    "establishments_census_data",
    "CBP2020.CB2000CBP_data_with_overlays_2022-06-22T143028.csv"
  ),
  skip = 1
) %>%
  select(
    id,
    county = `Geographic Area Name`,
    naics_code = `Meaning of NAICS code`,
    size = `Meaning of Employment size of establishments code`,
    num_of_estab = `Number of establishments`
  )

estab_2020 <- estab_2020 %>%
  filter(naics_code == 'Total for all sectors' &
           size == 'All establishments') %>%
  mutate(
    state_fips = str_sub(id, start = 10, end = 11),
    county_fips = str_sub(id, start = 12, end = 14)
  ) %>%
  select(county_name_long = county, state_fips, county_fips, num_of_estab) %>%
  mutate(year = 2020)

all_estab <- bind_rows(estab_2018, estab_2019, estab_2020)

census_estimates <- hh_all_years %>% 
  left_join(all_estab)

census_estimates <- census_estimates %>% 
  mutate(five_digit_fips = paste0(state_fips, county_fips))

# filter to contiguous US 
# all of what we want to be included is - the sum of the following expression 
# is 1
# sum(unique(county_list$five_digit_fips) %in% 
# census_estimates$five_digit_fips)/length(unique(county_list$five_digit_fips))

census_estimates <-
  census_estimates %>%
  filter(five_digit_fips %in% county_list$five_digit_fips) %>%
  select(
    state,
    state_fips,
    county,
    county_fips,
    five_digit_fips,
    county_name_long,
    year,
    total_occupied_hh,
    num_of_estab
  )

# STRONG NOTE: note that Hawaii and Alaskan estimates are not reliable.
# they are removed, along with other non-contiguous states 

census_estimates <-
  census_estimates %>%
  mutate(census_customer_estimate = total_occupied_hh + num_of_estab)

# Write -------------------------------------------------------------------

write_rds(
  census_estimates,
  here(
    "data",
    "power_outage_medicare_data_cleaning_output",
    "county_level_census_cust_estimates.RDS"
  )
)
