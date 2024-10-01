### Create fake medicare data hospitalization data
### should probably code this in data.table

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
    "cleaned_with_missingness"
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
  s1 <- read_rds(s[[i]]) |> setDT()
  exp1 <- read_rds(exp[[i]]) |> setDT()
  
  s1 <- unique(s1[, .(counties, pod_id, customers_by_pod)])
  
  s1 <-
    s1 %>%
    group_by(counties) %>%
    summarize(n = sum(customers_by_pod))
  
  exp1 <- exp1 %>% left_join(s1)
  
  # calculate hospitalization counts for different rates
  # start with rates under 10 p missing scenario
  
  # 0.5
  lambda_0.5_p <-
    baseline_hosp_rate + effect_of_outage_0.5_p * exp1$exposed_10_p_missing
  lambda_0.5_p <- lambda_0.5_p * exp1$n
  
  rate_0.5_p <- rpois(length(lambda_0.5_p), lambda_0.5_p)
  
  # 1
  lambda_1_p <-
    baseline_hosp_rate + effect_of_outage_1_p * exp1$exposed_10_p_missing
  lambda_1_p <- lambda_1_p * exp1$n
  
  rate_1_p <- rpois(length(lambda_1_p), lambda_1_p)
  
  # 5
  lambda_5_p <-
    baseline_hosp_rate + effect_of_outage_5_p * exp1$exposed_10_p_missing
  lambda_5_p <- lambda_5_p * exp1$n
  
  rate_5_p <- rpois(length(lambda_5_p), lambda_5_p)
  
  # add hospitalization counts to data
  exp1$hosp_count_0.5_p_10_p_missing <- rate_0.5_p
  exp1$hosp_count_1_p_10_p_missing <- rate_1_p
  exp1$hosp_count_5_p_10_p_missing <- rate_5_p
  
  # 30 percent missing
  
  # 0.5
  lambda_0.5_p <-
    baseline_hosp_rate + effect_of_outage_0.5_p * exp1$exposed_30_p_missing
  lambda_0.5_p <- lambda_0.5_p * exp1$n
  
  rate_0.5_p <- rpois(length(lambda_0.5_p), lambda_0.5_p)
  
  # 1
  lambda_1_p <-
    baseline_hosp_rate + effect_of_outage_1_p * exp1$exposed_30_p_missing
  lambda_1_p <- lambda_1_p * exp1$n
  
  rate_1_p <- rpois(length(lambda_1_p), lambda_1_p)
  
  # 5
  lambda_5_p <-
    baseline_hosp_rate + effect_of_outage_5_p * exp1$exposed_30_p_missing
  lambda_5_p <- lambda_5_p * exp1$n
  
  rate_5_p <- rpois(length(lambda_5_p), lambda_5_p)
  
  # add hospitalization counts to data
  exp1$hosp_count_0.5_p_30_p_missing <- rate_0.5_p
  exp1$hosp_count_1_p_30_p_missing <- rate_1_p
  exp1$hosp_count_5_p_30_p_missing <- rate_5_p
  
  # 50 percent missing
  
  # 0.5
  lambda_0.5_p <-
    baseline_hosp_rate + effect_of_outage_0.5_p * exp1$exposed_50_p_missing
  lambda_0.5_p <- lambda_0.5_p * exp1$n
  
  rate_0.5_p <- rpois(length(lambda_0.5_p), lambda_0.5_p)
  
  # 1
  lambda_1_p <-
    baseline_hosp_rate + effect_of_outage_1_p * exp1$exposed_50_p_missing
  lambda_1_p <- lambda_1_p * exp1$n
  
  rate_1_p <- rpois(length(lambda_1_p), lambda_1_p)
  
  # 5
  lambda_5_p <-
    baseline_hosp_rate + effect_of_outage_5_p * exp1$exposed_50_p_missing
  lambda_5_p <- lambda_5_p * exp1$n
  
  rate_5_p <- rpois(length(lambda_5_p), lambda_5_p)
  
  # add hospitalization counts to data
  exp1$hosp_count_0.5_p_50_p_missing <- rate_0.5_p
  exp1$hosp_count_1_p_50_p_missing <- rate_1_p
  exp1$hosp_count_5_p_50_p_missing <- rate_5_p
  
  
  # 70 percent missing
  
  # 0.5
  lambda_0.5_p <-
    baseline_hosp_rate + effect_of_outage_0.5_p * exp1$exposed_70_p_missing
  lambda_0.5_p <- lambda_0.5_p * exp1$n
  
  rate_0.5_p <- rpois(length(lambda_0.5_p), lambda_0.5_p)
  
  # 1
  lambda_1_p <-
    baseline_hosp_rate + effect_of_outage_1_p * exp1$exposed_70_p_missing
  lambda_1_p <- lambda_1_p * exp1$n
  
  rate_1_p <- rpois(length(lambda_1_p), lambda_1_p)
  
  # 5
  lambda_5_p <-
    baseline_hosp_rate + effect_of_outage_5_p * exp1$exposed_70_p_missing
  lambda_5_p <- lambda_5_p * exp1$n
  
  rate_5_p <- rpois(length(lambda_5_p), lambda_5_p)
  
  # add hospitalization counts to data
  exp1$hosp_count_0.5_p_70_p_missing <- rate_0.5_p
  exp1$hosp_count_1_p_70_p_missing <- rate_1_p
  exp1$hosp_count_5_p_70_p_missing <- rate_5_p

  # Select exposed and control regions for DID setup, with missing data -------
  # baseline none missing 
  l <- setDT(exp1)
  days_exposed_8_hrs <- l[exposed_none_missing == 1] # find exposed days 
  
  # control selection
  controls_8_hrs <- data.table()
  for (j in 1:length(days_exposed_8_hrs$day)){
    selected_day <- days_exposed_8_hrs$day[[j]]
    county <- days_exposed_8_hrs$counties[[j]]
    
    control_days <- l[weekdays(day) == weekdays(selected_day) &
                        month(day) == month(selected_day) &
                        exposed_none_missing == 0]
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
           exposed_none_missing,
           hosp_count_0.5_p_10_p_missing,
           hosp_count_1_p_10_p_missing,
           hosp_count_5_p_10_p_missing,
           hosp_count_0.5_p_30_p_missing,
           hosp_count_1_p_30_p_missing,
           hosp_count_5_p_30_p_missing,
           hosp_count_0.5_p_50_p_missing,
           hosp_count_1_p_50_p_missing,
           hosp_count_5_p_50_p_missing,
           hosp_count_0.5_p_70_p_missing,
           hosp_count_1_p_70_p_missing,
           hosp_count_5_p_70_p_missing,
           )
  
  saveRDS(
    an_dat,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      '8_hrs_all_levels_missingness_DID',
      paste0('outcome_data_8_hrs_yes_missingness_', i, ".RDS")
    )
  )
  

  # Set up case control ----------------------------------------------------
  # going to need to do a separate one for each level of missingness and effect
  # size, which is a RIP. 
  
  l <- setDT(exp1)
  
  # 0.5, 10 p missing
  an_dat_0.5_p <- l[hosp_count_0.5_p_10_p_missing != 0]

  
  an_dat_0.5_p <-
    an_dat_0.5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_0.5_p_10_p_missing)
  
  saveRDS(
    an_dat_0.5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_10_p_missing_0.5',
      paste0('outcome_data_8_hrs_10_p_missing_', i, ".RDS")
    )
  )
  
  # 0.5, 30 p missing
  an_dat_0.5_p <- l[hosp_count_0.5_p_30_p_missing != 0]
  

  an_dat_0.5_p <-
    an_dat_0.5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_0.5_p_30_p_missing)
  
  saveRDS(
    an_dat_0.5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_30_p_missing_0.5',
      paste0('outcome_data_8_hrs_30_p_missing_', i, ".RDS")
    )
  )
  
  # 0.5, 50 p missing
  an_dat_0.5_p <- l[hosp_count_0.5_p_50_p_missing != 0]
  
  an_dat_0.5_p <-
    an_dat_0.5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_0.5_p_50_p_missing)
  
  saveRDS(
    an_dat_0.5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_50_p_missing_0.5',
      paste0('outcome_data_8_hrs_50_p_missing_', i, ".RDS")
    )
  )
  
  # 0.5, 70 p missing
  an_dat_0.5_p <- l[hosp_count_0.5_p_70_p_missing != 0]
  
  an_dat_0.5_p <-
    an_dat_0.5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_0.5_p_70_p_missing)
  
  saveRDS(
    an_dat_0.5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_70_p_missing_0.5',
      paste0('outcome_data_8_hrs_70_p_missing_', i, ".RDS")
    )
  )

    
  # 1, 10 p missing
  an_dat_1_p <- l[hosp_count_1_p_10_p_missing != 0]

  
  an_dat_1_p <-
    an_dat_1_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_1_p_10_p_missing)
  
  saveRDS(
    an_dat_1_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_10_p_missing_1',
      paste0('outcome_data_8_hrs_10_p_missing_', i, ".RDS")
    )
  )
  
  # 1, 30 p missing
  an_dat_1_p <- l[hosp_count_1_p_30_p_missing != 0]

  
  an_dat_1_p <-
    an_dat_1_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_1_p_30_p_missing)
  
  saveRDS(
    an_dat_1_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_30_p_missing_1',
      paste0('outcome_data_8_hrs_30_p_missing_', i, ".RDS")
    )
  )
  
  # 1, 50 p missing
  an_dat_1_p <- l[hosp_count_1_p_50_p_missing != 0]

  
  an_dat_1_p <-
    an_dat_1_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_1_p_50_p_missing)
  
  saveRDS(
    an_dat_1_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_50_p_missing_1',
      paste0('outcome_data_8_hrs_50_p_missing_', i, ".RDS")
    )
  )
  
  # 1, 70 p missing
  an_dat_1_p <- l[hosp_count_1_p_70_p_missing != 0]
  
  an_dat_1_p <-
    an_dat_1_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_1_p_70_p_missing)
  
  saveRDS(
    an_dat_1_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_70_p_missing_1',
      paste0('outcome_data_8_hrs_70_p_missing_', i, ".RDS")
    )
  )
  
  # 5, 10 p missing
  an_dat_5_p <- l[hosp_count_5_p_10_p_missing != 0]

  an_dat_5_p <-
    an_dat_5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_5_p_10_p_missing)
  
  saveRDS(
    an_dat_5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_10_p_missing_5',
      paste0('outcome_data_8_hrs_10_p_missing_', i, ".RDS")
    )
  )
  
  # 5, 30 p missing
  an_dat_5_p <- l[hosp_count_5_p_30_p_missing != 0]
  
  an_dat_5_p <-
    an_dat_5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_5_p_30_p_missing)
  
  saveRDS(
    an_dat_5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_30_p_missing_5',
      paste0('outcome_data_8_hrs_30_p_missing_', i, ".RDS")
    )
  )
  
  # 5, 50 p missing
  an_dat_5_p <- l[hosp_count_5_p_50_p_missing != 0]
 
  # then all I have to do is select
  an_dat_5_p <-
    an_dat_5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_5_p_50_p_missing)
  
  saveRDS(
    an_dat_5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_50_p_missing_5',
      paste0('outcome_data_8_hrs_50_p_missing_', i, ".RDS")
    )
  )
  
  # 5, 70 p missing
  an_dat_5_p <- l[hosp_count_5_p_70_p_missing != 0]
  
  an_dat_5_p <-
    an_dat_5_p %>%  
    mutate(unique_county_id = counties + (i - 1) * 25) %>%
    select(counties,
           unique_county_id,
           n,
           day,
           exposed_none_missing,
           hosp_count_5_p_70_p_missing)
  
  saveRDS(
    an_dat_5_p,
    here(
      "power_outage_medicare_data",
      "simulated_hospitalization_outcome_data_smaller",
      'CC_70_p_missing_5',
      paste0('outcome_data_8_hrs_70_p_missing_', i, ".RDS")
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


# list_of_nulls <- lapply(X = is, FUN = create_one_simed_county)



# then run a model?
# start_values <-
#   coef(glm(an_dat$hosp_rate ~ an_dat$exposed, family = poisson(link = 'log')))


# m <-
#   glm(an_dat$hosp_rate ~ an_dat$exposed + offset(log(an_dat$n)),
#       family = poisson(link = 'log'))
