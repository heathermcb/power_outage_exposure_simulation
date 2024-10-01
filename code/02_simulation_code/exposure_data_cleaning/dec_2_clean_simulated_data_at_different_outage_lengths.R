# This script will take the simulated, generated data and clean it
# in the same way the real data is cleaned.

# It will also clean it based on different clinically significant outage 
# lengths.

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

outage_duration_8 <- hours(8)
outage_duration_4 <- hours(4)
outage_duration_12 <- hours(12)
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
  
  sim_dat_b <-
    sim_dat_b[, .(
      customers_out_ten_min_by_county = sum(customers_out_counts),
      customers_served_ten_min_by_county = sum(customers_by_pod)),
      by = .(counties, datetimes)]
  
  sim_dat_b <- sim_dat_b[, hour := floor_date(datetimes, unit = 'hour')]
  
  sim_dat_b <-
    sim_dat_b[, .(
      customers_out_hourly = round(mean(customers_out_ten_min_by_county)),
      customers_served_hourly = round(mean(customers_served_ten_min_by_county))),
      by = .(counties, hour)
    ]
  
  
  # ID outages --------------------------------------------------------------
  
  sim_dat_b <-
    sim_dat_b[, .(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly
    )]
  
  simed_counties <- split(sim_dat_b, by = "counties")
  
  for (i in 1:length(simed_counties)) {
    hourly_county_df <- simed_counties[[i]]
    hourly_county_df <- hourly_county_df %>%
      mutate(cutoff = customers_served_hourly * 0.005)
    
    
    hourly_county_df <- hourly_county_df %>%
      mutate(po_on = case_when(customers_out_hourly > cutoff ~ 1,
                                            TRUE ~ 0))
    
    hourly_county_df <- hourly_county_df %>%
      mutate(
        po_id = case_when((po_on == 1) & 
                            (lag(po_on) == 0) ~ 1, TRUE ~ 0))
      
    hourly_county_df <- hourly_county_df %>%
      mutate(
        po_id = case_when(po_on == 1 ~ cumsum(po_id),
                          TRUE ~ 0))
    
    
    simed_counties[[i]] <- hourly_county_df
    
  }
  
  # Count 8+ outages and days with 8+ hour outages -------------------------
  
  # do this part 3 times - once each for each outage length 
  
  county_num_id <- c('testcase')
  all_an_dat <- data.frame()
  
  for (county in simed_counties) {
    county <- county %>% select(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly,
      cutoff,
      po_on,
      po_id
    )
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration_8)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
  }
  
  all_an_dat_8 <- all_an_dat
  
  # iteration 2 - 4 hours
  
  county_num_id <- c('testcase')
  all_an_dat <- data.frame()
  
  for (county in simed_counties) {
    county <- county %>% select(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly,
      cutoff,
      po_on,
      po_id 
    )
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration_4)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
  }
  
  all_an_dat_4 <- all_an_dat
  
  # iteration 3 - 12 hours
  
  county_num_id <- c('testcase')
  
  all_an_dat <- data.frame()
  for (county in simed_counties) {
    county <- county %>% select(
      counties,
      hour,
      customers_out_hourly,
      customers_served_hourly,
      cutoff,
      po_on,
      po_id 
    )
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration_12)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
  }
  
  all_an_dat_12 <- all_an_dat
  
  all <-
    cbind(
      all_an_dat_4,
      all_an_dat_8$exposed,
      all_an_dat_12$exposed
    )
  
  colnames(all) <- c(
    "counties",
    "day",
    "exposed_under_4_hr_def",
    "exposed_under_8_hr_def",
    "exposed_under_12_hr_def"
  )
  
  # Write -------------------------------------------------------------------
  
  saveRDS(
    all,
    here(
      "power_outage_medicare_data",
      "power_outage_simulation_cleaned_data",
      "cleaned_4_8_12_hrs_2",
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