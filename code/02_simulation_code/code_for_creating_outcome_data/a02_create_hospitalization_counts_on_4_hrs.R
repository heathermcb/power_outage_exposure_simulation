### Create fake medicare data hospitalization data, for all effect sizes 
### and select data for both study design analyses, but for misclassification
### scenario based on 4 hour outages. 

# Libraries ---------------------------------------------------------------

library(here)
library(tidyverse)
library(data.table)
library(gtools)

# Constants ---------------------------------------------------------------

baseline_hosp_rate <- 0.001 # putting this at .1% of people per day
# may update once we have real or simulated medicare data to pull from

# these are the rate changes with outages, for different effect sizes
effect_of_outage_0.5_p <- 0.000005
effect_of_outage_1_p <- 0.00001 
effect_of_outage_5_p <- 0.00005


# Read "exposure" data ----------------------------------------------------

exp <- list.files(
  here(
    "power_outage_medicare_data",
    "power_outage_simulation_cleaned_data",
    "cleaned_4_8_12_hrs"
  ),
  full.names = T
)


# Get denominator data ----------------------------------------------------

s <-
  list.files(
    here(
      "power_outage_medicare_data",
      "power_outage_simulation_created_data",
      "exposure_datasets"
    ),
    full.names = T
  )


s <- mixedsort(s)
exp <- mixedsort(exp)

# Create outcome ----------------------------------------------------------

create_outcome_data <- function(i) {
  # want to create data where we have a rate per county per day
  # can use backbone of outage days per county perhaps?
  s1 <- readRDS(s[[i]]) |> setDT()
  exp1 <- readRDS(exp[[i]]) |> setDT()
  
  s1 <- unique(s1[, .(counties, pod_id, customers_by_pod)])
  
  s1 <-
    s1 %>%
    group_by(counties) %>%
    summarize(n = sum(customers_by_pod))
  
  exp1 <- exp1 %>% left_join(s1)
  
  # calculate hospitalization counts for different rates
  
  # 0.5
  lambda_0.5_p <-
    baseline_hosp_rate + effect_of_outage_0.5_p * exp1$exposed_under_4_hr_def
  lambda_0.5_p <- lambda_0.5_p * exp1$n
  
  rate_0.5_p <- rpois(length(lambda_0.5_p), lambda_0.5_p)
  
  # 1
  lambda_1_p <-
    baseline_hosp_rate + effect_of_outage_1_p * exp1$exposed_under_4_hr_def
  lambda_1_p <- lambda_1_p * exp1$n
  
  rate_1_p <- rpois(length(lambda_1_p), lambda_1_p)
  
  # 5
  lambda_5_p <-
    baseline_hosp_rate + effect_of_outage_5_p * exp1$exposed_under_4_hr_def
  lambda_5_p <- lambda_5_p * exp1$n
  
  rate_5_p <- rpois(length(lambda_5_p), lambda_5_p)
  
  # add hospitalization counts to data
  exp1$hosp_count_0.5_p <- rate_0.5_p
  exp1$hosp_count_1_p <- rate_1_p
  exp1$hosp_count_5_p <- rate_5_p
  
  # Select exposed and control regions for DID setup -----------------------
  
  l <- setDT(exp1)
  days_exposed_8_hrs <- l[exposed_under_8_hr_def == 1]
  
  # control selection
  controls_8_hrs <- data.table()
  for (j in 1:length(days_exposed_8_hrs$day)){
    selected_day <- days_exposed_8_hrs$day[[j]]
    county <- days_exposed_8_hrs$counties[[j]]
    
    control_days <- l[weekdays(day) == weekdays(selected_day) &
                        month(day) == month(selected_day) &
                        exposed_under_8_hr_def == 0]
    control_days <- sample_n(control_days, size = 1)
    
    controls_8_hrs <- rbindlist(list(control_days, controls_8_hrs))
    
  }
  
  an_dat <- rbindlist(list(days_exposed_8_hrs, controls_8_hrs))
  # then all I have to do is select
  
  an_dat <-
    an_dat %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_under_8_hr_def,
           hosp_count_0.5_p,
           hosp_count_1_p,
           hosp_count_5_p)
  
  saveRDS(
    an_dat,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      '4_hrs_no_missingness_DID',
      paste0('outcome_data_4_hrs_no_missingness_', i, ".RDS")
    )
  )
  
  
  # Select control days for case-control setup -----------------------------
  l <- setDT(exp1)
  
  # 0.5
  an_dat_0.5_p <- l[hosp_count_0.5_p != 0]
  
  an_dat_0.5_p <-
    an_dat_0.5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_under_8_hr_def,
           hosp_count_0.5_p)
  
  saveRDS(
    an_dat_0.5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      '4_hrs_no_missingness_CC_0.5',
      paste0('outcome_data_4_hrs_no_missingness_', i, ".RDS")
    )
  )
  
  # 1
  an_dat_1_p <- l[hosp_count_1_p != 0]
  
  an_dat_1_p <-
    an_dat_1_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_under_8_hr_def,
           hosp_count_1_p)
  
  saveRDS(
    an_dat_1_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      '4_hrs_no_missingness_CC_1',
      paste0('outcome_data_4_hrs_no_missingness_', i, ".RDS")
    )
  )
  
  # 5
  an_dat_5_p <- l[hosp_count_5_p != 0]
  
  # then all I have to do is select
  an_dat_5_p <-
    an_dat_5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_under_8_hr_def,
           hosp_count_5_p)
  
  saveRDS(
    an_dat_5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      '4_hrs_no_missingness_CC_5',
      paste0('outcome_data_4_hrs_no_missingness_', i, ".RDS")
    )
  )
  
  
  
}


# Load the parallel package
library(parallel)
i <- seq(1:400)

# Determine the number of cores
num_cores <- detectCores()

# Create a cluster
cl <- makeCluster(num_cores) # don't want to overload memory

# Export any objects needed by the computation to the cluster
clusterExport(cl, ls()) # quite of lot of objects are needed

clusterEvalQ(cl, {
  library(tidyverse)
  library(here)
  library(EnvStats)
  library(lubridate)
  library(padr)
  library(data.table)
})


# Use parLapply to apply a function in parallel
result <- parLapply(cl, X = i, fun = create_outcome_data)

# Stop the cluster
stopCluster(cl)