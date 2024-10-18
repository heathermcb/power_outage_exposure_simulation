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

city_utility_time_series <- rbindlist(lapply(X = list.files(here('data', 'power_outage_simulation_created_data', 'city_utility_level_time_series'), full.names = T)[1:10], FUN = read_parquet))

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
    "z_sample_data",
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

write_rds(
  percentage_customers_out_vector,
  here(
    "data",
    "z_sample_data",
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
    "z_sample_data",
    "distribution_vectors",
    "num_pods_by_county.RDS"
  )
)

# Get information on length of outages and distribution -------------------

p_customers_out <- city_utility_time_series %>%
  mutate(p_customers_out = new_locf_rep /
           city_utility_customers_served_est) %>%
  select(p_customers_out,
         clean_state_name,
         clean_county_name,
         city_name,
         utility_name) %>%
  mutate(over_0 = 
           case_when(p_customers_out > 0 ~ 1, TRUE ~ 0)) 
%>%
  collect()

# get outage durations in 10 minute periods
all_periods <- 
  p_customers_out[, .(duration = rle(over_0)$lengths[rle(over_0)$values]), 
                  by = .(clean_state_name, 
                         clean_county_name, 
                         city_name, 
                         utility_name)]

write_rds(all_periods, here(
  "data",
  "z_sample_data",
  "distribution_vectors",
  "length_of_outages.RDS"
))



p_customers_out <- 
  city_utility_time_series %>%
  left_join(customers_served_frame) %>%
  mutate(p_customers_out = new_locf_rep / customers_served) %>%
  filter(!is.na(p_customers_out)) %>%
  select(p_customers_out,
         clean_state_name,
         clean_county_name,
         city_name,
         utility_name) %>%
  mutate(over_0 = 
           case_when(p_customers_out > 0 ~ 1, TRUE ~ 0)) %>%
  collect()

setDT(p_customers_out)
# get outage durations in 10 minute periods
all_periods <- 
  p_customers_out[, .(duration = rle(over_0)$lengths[rle(over_0)$values]), 
                  by = .(clean_state_name, 
                         clean_county_name, 
                         city_name, 
                         utility_name)]

write_rds(all_periods, here(
  "data",
  "z_sample_data",
  "distribution_vectors",
  "length_of_outages.RDS"
))


