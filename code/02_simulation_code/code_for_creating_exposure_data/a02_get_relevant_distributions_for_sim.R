# This script pulls and saves key distributional statistics from the power 
# outage US national data that we need for the outage simulations.

# Libraries ---------------------------------------------------------------

library(tidyverse)
library(here)
library(lubridate)

# Do ----------------------------------------------------------------------

# Customers served --------------------------------------------------------

# first want to get the distribution of customers served by city-utility-year
l <-
  list.files(
    here(
      "power_outage_medicare_data",
      "power_outage_medicare_data_cleaning_output",
      "county_cust_estimates"),
      full.names = T
  )
t <- lapply(
  l,
  FUN = read_csv,
  col_types = cols(
    clean_state_name = col_character(),
    clean_county_name = col_character(),
    city_name = col_character(),
    year = col_double(),
    customers_served = col_double()
  ),
  col_names = TRUE
)

customers_served_by_city_utility_year <-
  bind_rows(t) %>% filter(year > 2017) # removing less reliable 2017 data

customers_served_by_city_utility_year_vector <- 
  customers_served_by_city_utility_year$customers_served

write_rds(
  customers_served_by_city_utility_year_vector,
  here(
    "power_outage_medicare_data",
    "power_outage_simulation_created_data",
    "distribution_vectors",
    "customers_served_by_city_utility_year.RDS"
  )
)


# Percentage of customers out by pod --------------------------------------

# then want the distribution of percentage of customers out over the study 
# period - start with customers out
l <-
  list.files(
    here(
      "power_outage_medicare_data",
      "power_outage_simulation_created_data",
      "city-utility_level_time_series"
    ),
    full.names = T
  )
l <- sample(l, size = 30)

t <- lapply(l, readRDS)

customers_out_time_series <- bind_rows(t)

# join customers served 
customers_out_time_series <- 
  customers_out_time_series %>%
  mutate(year = year(date)) %>%
  filter(year > 2017)

customers_out_time_series <- 
  customers_out_time_series %>%
  left_join(customers_served_by_city_utility_year)

# correct customers served as best we can
customers_out_table <- customers_out_time_series %>%
  group_by(utility_name,
           clean_state_name,
           clean_county_name,
           city_name,
           fips,
           year) %>%
  summarize(
    customers_served = max(customers_served, na.rm = TRUE),
    customers_out = max(customers_out, na.rm = TRUE)
  ) %>%
  mutate(customers_served = case_when(
    is.na(customers_served) ~ customers_out,
    TRUE ~ customers_served
  ))

customers_out_table <- customers_out_table %>% 
  filter(customers_served != -Inf) %>%
  rename(new_customers_served = customers_served) %>%
  select(-c(customers_out))

customers_out_time_series <- customers_out_time_series %>%
  left_join(customers_out_table)

customers_out_time_series <- customers_out_time_series %>%
  mutate(p_customers_out = customers_out_api_on / new_customers_served) %>%
  filter(!is.na(p_customers_out))

percentage_customers_out_vector <- customers_out_time_series$p_customers_out

write_rds(
  percentage_customers_out_vector,
  here(
    "power_outage_medicare_data",
    "power_outage_simulation_created_data",
    "distribution_vectors",
    "percent_customers_out_vector.RDS"
  )
)


write_rds(
  customers_out_time_series,
  here(
    "power_outage_medicare_data",
    "power_outage_simulation_created_data",
    "distribution_vectors",
    "customers_out_time_series.RDS"
  )
)


# Pods by county ----------------------------------------------------------

l <-
  list.files(
    here(
      "power_outage_medicare_data",
      "power_outage_medicare_data_cleaning_output",
      "county_cust_estimates"),
    full.names = F
  )

s <- data.frame()
i = 0
for (file in l){
  d <- read_csv(here(
    "power_outage_medicare_data",
    "power_outage_medicare_data_cleaning_output",
    "county_cust_estimates", file),
                cols(
                  clean_state_name = col_character(),
                  clean_county_name = col_character(),
                  city_name = col_character(),
                  year = col_double(),
                  customers_served = col_double()),
                col_names = TRUE)
  print(i)
  i = i+1
  s <- bind_rows(s, d)
}

pods_by_county <-
  s %>% group_by(clean_county_name) %>% summarise(n = n())

pods_by_county <- pods_by_county$n

write_rds(
  pods_by_county,
  here(
    "power_outage_medicare_data",
    "power_outage_simulation_created_data",
    "distribution_vectors",
    "num_pods_by_county.RDS"
  )
)


# Get information on length of outages and distribution -------------------

time_series <- customers_out_time_series %>% 
  mutate(over_0 = case_when(percentage_customers_out_vector > 0 ~ 1, TRUE ~ 0))

time_series <- time_series %>%
  group_by(utility_name,
           clean_state_name,
           clean_county_name,
           city_name,
           fips) %>% group_split()

all_periods <- c()
for (county in time_series) {
  j = 0
  v <- c()
  for (i in 1:dim(county)[[1]]) {
    if (county[[i,14]] == 1){j = j + 1}
    else{
      if (j != 0){v <- c(v, j)}
      j = 0}
  }
  all_periods <- c(all_periods, v)
}

write_rds(all_periods, here(
  "power_outage_medicare_data",
  "power_outage_simulation_created_data",
  "distribution_vectors",
  "length_of_outages.RDS"
))


# Look at quantiles and produce length of outages -------------------------

quantile(percentage_customers_out_vector, probs = seq(.1, 1, by = .01))
