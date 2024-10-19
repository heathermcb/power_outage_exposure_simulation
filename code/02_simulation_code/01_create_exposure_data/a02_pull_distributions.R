# This script pulls and saves key distributional statistics from the power 
# outage US national data that we need for the outage simulations.

# Author: Heather
# Last updated: Oct 14th, 2024

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here, lubridate, arrow, data.table)

# Customers served --------------------------------------------------------

# open dataset
city_utility_time_series <- open_dataset(here(
  "data",
  "power_outage_simulation_created_data",
  "city_utility_level_time_series"))

# city_utility_time_series <- rbindlist(lapply(X = list.files(
#   here(
#     'data',
#     'power_outage_simulation_created_data',
#     'city_utility_level_time_series'
#   ),
#   full.names = T
# )[1:10], FUN = read_parquet))

customers_served_frame <- 
  city_utility_time_series %>%
  select(
    clean_state_name,
    clean_county_name,
    city_name,
    utility_name,
    year,
    city_utility_customers_served_est
  ) %>%
  distinct() %>%
  collect()

customers_served_frame <- customers_served_frame %>%
  filter(city_utility_customers_served_est >= 0) %>% 
  filter(!is.na(city_utility_customers_served_est))

customers_served_by_city_utility_year_vector <- 
  customers_served_frame$city_utility_customers_served_est

# write vector of customers served values
write_rds(
  customers_served_by_city_utility_year_vector,
  here(
    "data",
    "power_outage_simulation_created_data",
    "distribution_vectors",
    "customers_served_by_city_utility_year.RDS"
  )
)

# Percentage of customers out by pod --------------------------------------

# then want the distribution of percentage of customers out over the study 
# period - start with customers out

p_customers_out <- city_utility_time_series %>%
  mutate(p_customers_out = new_locf_rep /
           city_utility_customers_served_est) %>%
  filter(!is.na(p_customers_out)) %>% 
  collect()

percentage_customers_out_vector <- p_customers_out$p_customers_out
percentage_customers_out_vector_sample <-
  sample(percentage_customers_out_vector,
         size = 500000000,
         replace = T)

write_rds(
  percentage_customers_out_vector_sample,
  here(
    "data",
    "power_outage_simulation_created_data",
    "distribution_vectors",
    "percent_customers_out_vector.RDS"
  )
)

# Pods by county ----------------------------------------------------------

pods <- 
  city_utility_time_series %>% 
  select(clean_state_name, clean_county_name, city_name, utility_name) %>%
  distinct() %>% 
  group_by(clean_state_name, clean_county_name) %>% 
  summarize(n_pods = n()) %>%
  collect()

pods_by_county <- pods$n_pods

write_rds(
  pods_by_county,
  here(
    "data",
    "power_outage_simulation_created_data",
    "distribution_vectors",
    "num_pods_by_county.RDS"
  )
)

# Get information on length of outages and distribution -------------------

# read the first 100 million rows only since R can't handle all
city_utility_time_series <- open_dataset(here(
  "data",
  "power_outage_simulation_created_data",
  "city_utility_level_time_series")) %>%
  head(100000000) %>%
  collect()

p_customers_out <- city_utility_time_series %>%
  mutate(p_customers_out = new_locf_rep /
           city_utility_customers_served_est) %>%
  select(p_customers_out,
         clean_state_name,
         clean_county_name,
         city_name,
         utility_name) %>%
  filter(!is.na(p_customers_out)) %>%
  mutate(over_0 = 
           case_when(p_customers_out > 0 ~ 1, TRUE ~ 0)) 


# get outage durations in 10 minute periods
# calculate outage_lengths as a vector
non_zero_lengths <- p_customers_out[, {
  rle_result <- rle(over_0)
  list(outage_lengths = rle_result$lengths[rle_result$values != 0])
}, by = .(clean_state_name, clean_county_name, city_name, utility_name)]

# extract outage_lengths as a vector
outage_lengths_vector <- unlist(non_zero_lengths$outage_lengths)

write_rds(outage_lengths_vector, here(
  "data",
  "power_outage_simulation_created_data",
  "distribution_vectors",
  "length_of_outages.RDS"
))

