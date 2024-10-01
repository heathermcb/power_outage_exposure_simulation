# This script will take the simulated, generated data and clean it
# in the same way the real data is cleaned.

# It will also remove observations from the dataset incrementally to create
# synthetic data that are missing a certain percentage of observations by
# county.

# Libraries ---------------------------------------------------------------

library(tidyverse)
library(here)
library(data.table)
library(lubridate)
library(parallel)

source(here("power_outage_medicare",
            "simulation_data_cleaning",
            "helpers.R"))

# Constants ---------------------------------------------------------------

outage_duration <- hours(8)
num_cores <- detectCores()

# Read --------------------------------------------------------------------

sim_dat_L <- list.files(here("power_outage_medicare_data",
                             "power_outage_simulation_created_data",
                             "exposure_datasets_2"), full.names = T)


ps <- seq(1:length(sim_dat_L))

clean_simulated_data <- function(p){
  sim_dat_b <- readRDS(sim_dat_L[[p]]) 
  setnames(sim_dat_b, old = 'ten_min_times', new = 'datetimes')
  # Aggregate to county and hourly level ------------------------------------
 
  sim_dat_b <- sim_dat_b[order(
    counties,
    pod_id,
    customers_by_pod,
    datetimes
  )]
  
  county_cust_by_pod <-
    unique(sim_dat_b[, .(counties, pod_id, customers_by_pod)])
  
  county_cust <-
    county_cust_by_pod[, .(customers_by_county = sum(customers_by_pod)),
                       by = counties]
  
  county_cust[, `:=`(
    ten_p = floor(0.10 * customers_by_county),
    thirty_p = floor(0.30 * customers_by_county),
    fifty_p = floor(0.50 * customers_by_county),
    seventy_p = floor(0.70 * customers_by_county)
  )]
  
  
  county_cust_by_pod <-
    county_cust_by_pod[, cum_customers := cumsum(customers_by_pod),
                       by = counties]
  
  county_cust_by_pod <- county_cust[county_cust_by_pod, on = 'counties']
  
  county_cust_by_pod <- county_cust_by_pod[, `:=` (
    remove_for_10_p_missing = fifelse(cum_customers <= ten_p, 1, 0),
    remove_for_30_p_missing = fifelse(cum_customers <= thirty_p, 1, 0),
    remove_for_50_p_missing = fifelse(cum_customers <= fifty_p, 1, 0),
    remove_for_70_p_missing = fifelse(cum_customers <= seventy_p, 1, 0)
  ) ]
  
  county_cust_by_pod <-
    county_cust_by_pod[, .(
      counties,
      pod_id,
      customers_by_county,
      remove_for_10_p_missing,
      remove_for_30_p_missing,
      remove_for_50_p_missing,
      remove_for_70_p_missing
    )]
  
  sim_dat_b <- county_cust_by_pod[sim_dat_b, 
                                  on = c('counties', 'pod_id')]
  
  sim_dat_b[, `:=` (
    new_customer_counts_10p = fifelse(remove_for_10_p_missing == 1, 0, 
                                      customers_out_counts),
    new_customer_counts_30p = fifelse(remove_for_30_p_missing == 1, 0, 
                                      customers_out_counts),
    new_customer_counts_50p = fifelse(remove_for_50_p_missing == 1, 0, 
                                      customers_out_counts),
    new_customer_counts_70p = fifelse(remove_for_70_p_missing == 1, 0,
                                      customers_out_counts)
  )]
  
  
  sim_dat_b <-
    sim_dat_b[, .(
      customers_out_ten_min_by_county = sum(customers_out_counts),
      customers_served_ten_min_by_county = sum(customers_by_pod),
      customers_out_10_p_missing = sum(new_customer_counts_10p),
      customers_out_30_p_missing = sum(new_customer_counts_30p),
      customers_out_50_p_missing = sum(new_customer_counts_50p),
      customers_out_70_p_missing = sum(new_customer_counts_70p)),
      by = .(counties, datetimes)]
  
  
  sim_dat_b <- sim_dat_b[, hour := floor_date(datetimes, unit = 'hour')]
  
  sim_dat_b <-
    sim_dat_b[, .(
      customers_out_hourly = round(mean(customers_out_ten_min_by_county)),
      customers_served_hourly = round(mean(customers_served_ten_min_by_county)),
      customers_out_hourly_10_p_missing = 
        round(mean(customers_out_10_p_missing)),
      customers_out_hourly_30_p_missing = 
        round(mean(customers_out_30_p_missing)),
      customers_out_hourly_50_p_missing = 
        round(mean(customers_out_50_p_missing)),
      customers_out_hourly_70_p_missing = 
        round(mean(customers_out_70_p_missing))),
      by = .(counties, hour)
    ]
  
  
  # ID outages --------------------------------------------------------------
  
  sim_dat_b <-
    sim_dat_b[, .(
      counties,
      hour,
      customers_out_hourly,
      customers_out_hourly_10_p_missing,
      customers_out_hourly_30_p_missing,
      customers_out_hourly_50_p_missing,
      customers_out_hourly_70_p_missing,
      customers_served_hourly
    )]
  
  simed_counties <- split(sim_dat_b, by = "counties")
  
  for (i in 1:length(simed_counties)) {
    hourly_county_df <- simed_counties[[i]]
    hourly_county_df <- hourly_county_df %>%
      mutate(cutoff = customers_served_hourly * 0.005)
    
    
    hourly_county_df <- hourly_county_df %>%
      mutate(po_on_none_missing = case_when(customers_out_hourly > cutoff ~ 1,
                                            TRUE ~ 0),
             po_on_10_p_missing = case_when(customers_out_hourly_10_p_missing >
                                              cutoff ~ 1, TRUE ~ 0),
             po_on_30_p_missing = case_when(customers_out_hourly_30_p_missing > 
                                              cutoff ~ 1, TRUE ~ 0),
             po_on_50_p_missing = case_when(customers_out_hourly_50_p_missing > 
                                              cutoff ~ 1, TRUE ~ 0),
             po_on_70_p_missing = case_when(customers_out_hourly_70_p_missing >
                                              cutoff ~ 1, TRUE ~ 0)
      )
    
    hourly_county_df <- hourly_county_df %>%
      mutate(
        po_id = case_when((po_on_none_missing == 1) & 
                            (lag(po_on_none_missing) == 0) ~ 1, TRUE ~ 0),
        po_id_10_p_missing = case_when((po_on_10_p_missing == 1) &
                                         (lag(po_on_10_p_missing) == 0) ~ 1,
                                       TRUE ~ 0),
        po_id_30_p_missing = case_when((po_on_30_p_missing == 1) &
                                         (lag(po_on_30_p_missing) == 0) ~ 1,
                                       TRUE ~ 0),
        po_id_50_p_missing = case_when((po_on_50_p_missing == 1) &
                                         (lag(po_on_50_p_missing) == 0) ~ 1,
                                       TRUE ~ 0),
        po_id_70_p_missing = case_when((po_on_70_p_missing == 1) & 
                                         (lag(po_on_70_p_missing) == 0) ~ 1,
                                       TRUE ~ 0)
      ) 
    
    hourly_county_df <- hourly_county_df %>%
      mutate(
        po_id = case_when(po_on_none_missing == 1 ~ cumsum(po_id),
                          TRUE ~ 0),
        po_id_10_p_missing = case_when(po_on_10_p_missing == 1 ~ 
                                         cumsum(po_id_10_p_missing), TRUE ~ 0),
        po_id_30_p_missing = case_when(po_on_30_p_missing == 1 ~ 
                                         cumsum(po_id_30_p_missing), TRUE ~ 0),
        po_id_50_p_missing = case_when(po_on_50_p_missing == 1 ~ 
                                         cumsum(po_id_50_p_missing), TRUE ~ 0),
        po_id_70_p_missing = case_when(po_on_70_p_missing == 1 ~
                                         cumsum(po_id_70_p_missing), TRUE ~ 0)
      )
    
    
    simed_counties[[i]] <- hourly_county_df
    
  }
  
  # Count 8+ outages and days with 8+ hour outages -------------------------
  
  # do this part 4 times - once for each level of missingness
  # iteration 1
  
  county_num_id <- c('testcase')
  all_an_dat <- data.frame()
  
  for (county in simed_counties) {
    county <- county %>% select(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly,
      cutoff,
      po_on = po_on_none_missing,
      po_id
    )
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
  }
  
  all_an_dat_no_missingness <- all_an_dat
  
  # iteration 2 - 10% missingness
  
  county_num_id <- c('testcase')
  all_an_dat <- data.frame()
  
  for (county in simed_counties) {
    county <- county %>% select(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly,
      cutoff,
      po_on = po_on_10_p_missing,
      po_id = po_id_10_p_missing
    )
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
  }
  
  all_an_dat_10_p_missing <- all_an_dat
  
  # iteration 3 - 30P missingness
  
  county_num_id <- c('testcase')
  
  all_an_dat <- data.frame()
  for (county in simed_counties) {
    county <- county %>% select(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly,
      cutoff,
      po_on = po_on_30_p_missing,
      po_id = po_id_30_p_missing
    )
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
  }
  
  all_an_dat_30_p_missing <- all_an_dat
  
  # iteration 4 - 50P missingness
  
  county_num_id <- c('testcase')
  all_an_dat <- data.frame()
  
  for (county in simed_counties) {
    county <- county %>% select(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly,
      cutoff,
      po_on = po_on_50_p_missing,
      po_id = po_id_50_p_missing
    )
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    
    all_an_dat <- bind_rows(all_an_dat, an_dat)
  }
  
  all_an_dat_50_p_missing <- all_an_dat
  
  # iteration 5 - 70P missingness
  
  county_num_id <- c('testcase')
  all_an_dat <- data.frame()
  
  for (county in simed_counties) {
    county <- county %>% select(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly,
      cutoff,
      po_on = po_on_70_p_missing,
      po_id = po_id_70_p_missing
    )
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    
    all_an_dat <- bind_rows(all_an_dat, an_dat)
  }
  
  all_an_dat_70_p_missing <- all_an_dat
  
  all <-
    cbind(
      all_an_dat_no_missingness,
      all_an_dat_10_p_missing$exposed,
      all_an_dat_30_p_missing$exposed,
      all_an_dat_50_p_missing$exposed,
      all_an_dat_70_p_missing$exposed
    )
  
  colnames(all) <- c(
    "counties",
    "day",
    "exposed_none_missing",
    "exposed_10_p_missing",
    "exposed_30_p_missing",
    "exposed_50_p_missing",
    "exposed_70_p_missing"
  )
  
  # Write -------------------------------------------------------------------
  
  saveRDS(
    all,
    here(
      "power_outage_medicare_data",
      "power_outage_simulation_cleaned_data",
      "cleaned_with_missingness_2",
      paste0("days_exposed_unexposed_", p, ".RDS")
    )
  )
  
}


# Run script parallelized -------------------------------------------------

# create a cluster
cl <- makeCluster(num_cores) 

# export any objects needed by the computation to the cluster
clusterExport(cl, ls()) # quite of lot of objects are needed - I just exported
# everything 

# export libraries needed
clusterEvalQ(cl, {library(tidyverse)
  library(here)
  library(data.table)
  library(lubridate)})

# use parLapply to apply a function in parallel
result <- parLapply(cl, X = ps, fun = clean_simulated_data)

# stop the cluster
stopCluster(cl)
