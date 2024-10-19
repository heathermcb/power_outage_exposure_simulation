# Create hospitalization data

# Libraries ---------------------------------------------------------------

pacman::p_load(here, tidyverse, data.table, gtools, arrow)

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

denom <- list.files(
  here(
    "data",
    "power_outage_simulation_cleaned_data",
    "hourly_county_data_with_missingness"
  ),
  full.names = T
)


# Add customer served estimates -------------------------------------------

denom <- rbindlist(lapply(FUN = read_parquet, X = denom))

denom <- denom %>%
  filter(!is.na(unique_county_id)) %>%
  select(county_id = unique_county_id, customers_served_hourly) %>%
  distinct() %>%
  mutate(county_id = as.numeric(county_id))

exp_data <- exp_data %>%
  left_join(denom) 

%>%
  mutate(customers_served_hourly = 
           ifelse(is.na(customers_served_hourly), 0, customers_served_hourly))


