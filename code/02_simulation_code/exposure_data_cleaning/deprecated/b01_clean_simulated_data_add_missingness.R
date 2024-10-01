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

# Constants ---------------------------------------------------------------

outage_duration = hours(8)

# Read --------------------------------------------------------------------

sim_dat_L <- list.files(here("power_outage_medicare_data",
                             "power_outage_simulation_created_data",
                             "exposure_datasets"), full.names = T)


ps <- seq(1:length(sim_dat_L))

clean_simulated_data <- function(p){
  sim_dat_b <- readRDS(sim_dat_L[[p]]) 
  setnames(sim_dat_b, old = 'ten_min_times', new = 'datetimes')
  # Aggregate to county and hourly level ------------------------------------
  
  # here I want to sample pods by county so I get about 10%, 20%, 30% of 
  # of customers, and then set those counts to 0
  
  
  
  # want to sum the number of customers out by county
  # and then average the number of customers out by pod
  
  
  sim_dat_b <- sim_dat_b[order(
    counties,
    pod_id,
    customers_by_pod,
    datetimes
  )]
  
  county_cust_by_pod <- unique(sim_dat_b[, .(counties, pod_id, customers_by_pod)])

  county_cust <- county_cust_by_pod[, .(customers_by_county = sum(customers_by_pod)), by = counties]
  
  county_cust[, `:=`(
    ten_p = floor(0.10 * customers_by_county),
    thirty_p = floor(0.30 * customers_by_county),
    fifty_p = floor(0.50 * customers_by_county)
  )]
  

  county_cust_by_pod <-
    county_cust_by_pod[, cum_customers := cumsum(customers_by_pod), by = counties]
  
  county_cust_by_pod <- county_cust[county_cust_by_pod, on = 'counties']
  
  county_cust_by_pod <- county_cust_by_pod[, `:=` (
    remove_for_10_p_missing = fifelse(cum_customers <= ten_p, 1, 0),
    remove_for_30_p_missing = fifelse(cum_customers <= thirty_p, 1, 0),
    remove_for_50_p_missing = fifelse(cum_customers <= fifty_p, 1, 0)
  ) ]
  
  county_cust_by_pod <-
    county_cust_by_pod[, .(
      counties,
      pod_id,
      customers_by_county,
      remove_for_10_p_missing,
      remove_for_30_p_missing,
      remove_for_50_p_missing
    )]
  
  sim_dat_b <- county_cust_by_pod[sim_dat_b, on = c('counties', 'pod_id')]
  
  sim_dat_b[, `:=` (
    new_customer_counts_10p = fifelse(remove_for_10_p_missing == 1, 0, customers_out_counts),
    new_customer_counts_30p = fifelse(remove_for_30_p_missing == 1, 0, customers_out_counts),
    new_customer_counts_50p = fifelse(remove_for_50_p_missing == 1, 0, customers_out_counts)
  )]
  
  
  sim_dat_b <-
    sim_dat_b[, .(
      customers_out_ten_min_by_county = sum(customers_out_counts),
      customers_served_ten_min_by_county = sum(customers_by_pod),
      customers_out_10_p_missing = sum(new_customer_counts_10p),
      customers_out_30_p_missing = sum(new_customer_counts_30p),
      customers_out_50_p_missing = sum(new_customer_counts_50p)),
      by = .(counties, datetimes)]
  
  
  sim_dat_b <- sim_dat_b[, hour := floor_date(datetimes, unit = 'hour')]
  
  sim_dat_b <-
    sim_dat_b[, .(
      customers_out_hourly = round(mean(customers_out_ten_min_by_county)),
      customers_served_hourly = round(mean(customers_served_ten_min_by_county)),
      customers_out_hourly_10_p_missing = round(mean(customers_out_10_p_missing)),
      customers_out_hourly_30_p_missing = round(mean(customers_out_30_p_missing)),
      customers_out_hourly_50_p_missing = round(mean(customers_out_50_p_missing))),
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
             po_on_10_p_missing = case_when(customers_out_hourly_10_p_missing > cutoff ~ 1,
                                            TRUE ~ 0),
             po_on_30_p_missing = case_when(customers_out_hourly_30_p_missing > cutoff ~ 1,
                                            TRUE ~ 0),
             po_on_50_p_missing = case_when(customers_out_hourly_50_p_missing > cutoff ~ 1,
                                            TRUE ~ 0)
      )
    
    hourly_county_df <- hourly_county_df %>%
      mutate(
        po_id = case_when((po_on_none_missing == 1) & (lag(po_on_none_missing) == 0) ~ 1,
                          TRUE ~ 0),
        po_id_10_p_missing = case_when((po_on_10_p_missing == 1) &
                                         (lag(po_on_10_p_missing) == 0) ~ 1,
                                       TRUE ~ 0),
        po_id_30_p_missing = case_when((po_on_30_p_missing == 1) &
                                         (lag(po_on_30_p_missing) == 0) ~ 1,
                                       TRUE ~ 0),
        po_id_50_p_missing = case_when((po_on_50_p_missing == 1) &
                                         (lag(po_on_50_p_missing) == 0) ~ 1,
                                       TRUE ~ 0)
      ) 
    
    hourly_county_df <- hourly_county_df %>%
      mutate(
        po_id = case_when(po_on_none_missing == 1 ~ cumsum(po_id),
                          TRUE ~ 0),
        po_id_10_p_missing = case_when(po_on_10_p_missing == 1 ~ cumsum(po_id_10_p_missing),
                                       TRUE ~ 0),
        po_id_30_p_missing = case_when(po_on_30_p_missing == 1 ~ cumsum(po_id_30_p_missing),
                                       TRUE ~ 0),
        po_id_50_p_missing = case_when(po_on_50_p_missing == 1 ~ cumsum(po_id_50_p_missing),
                                       TRUE ~ 0)
      )
             
      
    simed_counties[[i]] <- hourly_county_df
    
  }
  
  # Calculate outage duration -----------------------------------------------
  
  # Helpers -----------------------------------------------------------------
  
  # want to filter out outages shorter than the desired duration, so need
  # to find actual outage durations and compare to the desired duration
  find_po_durations <- function(hourly_county_df, outage_duration) {
    po_durations <- hourly_county_df %>%
      filter(po_id != 0) %>%
      group_by(counties, po_id) %>%
      # get first and last records of customers out in this outage
      slice(c(1, n())) %>%
      # calculate duration by subtracting first and last time stamps
      mutate(duration = difftime(lead(hour), hour, units = 'hours')) %>%
      # keep only start time rows, renaming timestamp to start time
      filter(row_number() == 1) %>%
      rename(start_time = hour) %>%
      # keep only outages that are longer than desired duration
      filter(duration >= outage_duration) %>%
      #filter(duration < weeks(2)) %>%
      select(counties,
             po_id,
             start_time,
             duration) %>%
      ungroup()
    return(po_durations)
  }
  
  # mark days on which a power outage starts, is going, or ends
  # note that this does not include days that are not in the dataset
  # it still correctly indicates the known start and end days, though, and
  # just adds multiple indicators if more than one outage exists on one day
  find_start_and_end_days <-
    function(county_dataframe, po_durations) {
      # gives days on which there's a duration or longer power outage
      po_st_nd <- county_dataframe %>%
        filter(po_id %in% po_durations$po_id) %>%
        mutate(day = date(hour)) %>%
        select(counties, day, po_id) %>%
        distinct()
      # fill in days in between
      if (dim(po_st_nd)[[1]] > 0) {
        po_st_nd <- po_st_nd %>%
          group_by(po_id) %>%
          padr::pad(interval = "day")
      }
      # marks day numbers of each power outage longer than duration
      po_st_nd <- po_st_nd %>%
        group_by(counties, po_id) %>%
        mutate(day_number = row_number()) %>%
        mutate(n_days = n()) %>%
        mutate(day_counter = case_when(day_number == 1 ~ 1,
                                       day_number > 1 &
                                         day_number < n_days ~ 2,
                                       TRUE ~ 3)) %>%
        select(day, po_id, day_counter) %>%
        ungroup()
      return(po_st_nd)
    }
  
  # want outage start dates and durations in a dataframe to calculate exposed days
  # using a fully expanded dataframe with all dates in the study period
  durations_and_dates <-
    function(county_dataframe, po_st_nd, po_durations) {
      durations_and_dates <- county_dataframe %>%
        mutate(day = floor_date(hour, unit = 'day')) %>%
        select(counties, day, po_id) %>%
        distinct() %>%
        group_by(po_id) %>%
        padr::pad(interval = 'day') %>%
        ungroup() %>%
        tidyr::fill(counties)
      durations_and_dates <- durations_and_dates %>%
        left_join(po_durations) %>%
        left_join(po_st_nd)
      return(durations_and_dates)
    }
  
  
  # on days where the outage is starting, check to see if it starts before 4
  # on days where it's going or ends, mark those days as exposed
  find_exposed_days <-
    function(durations_and_dates, outage_duration) {
      exposed_days <- durations_and_dates %>%
        mutate(cut_time = as_datetime(day) + days(1) - outage_duration) %>%
        mutate(start_before_cut = start_time < cut_time) %>% 
        # line below is tricky - to get correct cut point, need to add a day to get 12
        # midnight the next day, and then subtract the desired duration
        mutate(exposed = case_when(((day_counter == 1) &
                                      start_before_cut) ~ 1,
                                   day_counter == 2 ~ 1,
                                   day_counter == 3 ~ 1,
                                   TRUE ~ 0
        )) %>% 
        select(day, exposed) %>%
        group_by(day) %>%
        summarise(exposed = max(exposed))
      return(exposed_days)
    }
  
  # make exposure frame
  frame <- function(hourly_county_df) {
    frame <- hourly_county_df %>%
      mutate(day = floor_date(hour, unit = 'day')) %>%
      select(counties, day) %>%
      distinct()
    return(frame)
  }
  
  
  # return exposed days in a certain county
  create_analytic_data <-
    function(county_dataframe, outage_duration) {
      po_durations <-
        find_po_durations(hourly_county_df = county_dataframe,
                          outage_duration = outage_duration)
      start_and_end_days <-
        find_start_and_end_days(county_dataframe = county_dataframe,
                                po_durations = po_durations)
      durations_and_dates <-
        durations_and_dates(
          county_dataframe = county_dataframe,
          po_st_nd = start_and_end_days,
          po_durations = po_durations
        )
      exposed_days <-
        find_exposed_days(durations_and_dates = durations_and_dates,
                          outage_duration = outage_duration)
      
      frame <- frame(hourly_county_df = county_dataframe)
      
      all_data <- frame %>% left_join(exposed_days)
      
      return(all_data)
    }
  
  
  
  # Count 8+ outages and days with 8+ hour outages -------------------------
  
  # do this part 4 times - once for each level of missingness 
  # iteration 1
  num_outages <- c(0)
  county_num_id <- c('testcase')
  
  all_an_dat <- data.frame()
  num_outages_frame <- data.frame(county_num_id, num_outages)
  for (county in simed_counties) {
    county <- county %>% select(counties, hour, customers_out_hourly,
                                customers_served_hourly, cutoff,
                                po_on = po_on_none_missing, po_id)
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    num_outages <-
      dim(find_po_durations(county, outage_duration = outage_duration))[1]
    county_num_id <-
      paste0(unique(county$counties))
    num_out_row <- data.frame(county_num_id, num_outages)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
    num_outages_frame <- bind_rows(num_outages_frame, num_out_row)
  }
  
  num_outages_frame_no_missingness <- num_outages_frame %>% filter(county_num_id != 'testcase')
  all_an_dat_no_missingness <- all_an_dat 
  
  # iteration 2 - 10P missingness 
  num_outages <- c(0)
  county_num_id <- c('testcase')
  
  all_an_dat <- data.frame()
  num_outages_frame <- data.frame(county_num_id, num_outages)
  for (county in simed_counties) {
    county <- county %>% select(counties, hour, customers_out_hourly,
                                customers_served_hourly, cutoff,
                                po_on = po_on_10_p_missing, po_id = po_id_10_p_missing)
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    num_outages <-
      dim(find_po_durations(county, outage_duration = outage_duration))[1]
    county_num_id <-
      paste0(unique(county$counties))
    num_out_row <- data.frame(county_num_id, num_outages)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
    num_outages_frame <- bind_rows(num_outages_frame, num_out_row)
  }
  
  num_outages_frame_10_p_missing <- num_outages_frame %>% filter(county_num_id != 'testcase')
  all_an_dat_10_p_missing <- all_an_dat 
  
  # iteration 3 - 30P missingness 
  num_outages <- c(0)
  county_num_id <- c('testcase')
  
  all_an_dat <- data.frame()
  num_outages_frame <- data.frame(county_num_id, num_outages)
  for (county in simed_counties) {
    county <- county %>% select(counties, hour, customers_out_hourly,
                                customers_served_hourly, cutoff,
                                po_on = po_on_30_p_missing, po_id = po_id_30_p_missing)
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    num_outages <-
      dim(find_po_durations(county, outage_duration = outage_duration))[1]
    county_num_id <-
      paste0(unique(county$counties))
    num_out_row <- data.frame(county_num_id, num_outages)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
    num_outages_frame <- bind_rows(num_outages_frame, num_out_row)
  }
  
  num_outages_frame_30_p_missing <- num_outages_frame %>% filter(county_num_id != 'testcase')
  all_an_dat_30_p_missing <- all_an_dat 
  
  # iteration 4 - 50P missingness 
  num_outages <- c(0)
  county_num_id <- c('testcase')
  
  all_an_dat <- data.frame()
  num_outages_frame <- data.frame(county_num_id, num_outages)
  for (county in simed_counties) {
    county <- county %>% select(counties, hour, customers_out_hourly,
                                customers_served_hourly, cutoff,
                                po_on = po_on_50_p_missing, po_id = po_id_50_p_missing)
    an_dat <-
      create_analytic_data(county_dataframe = county,
                           outage_duration = outage_duration)
    num_outages <-
      dim(find_po_durations(county, outage_duration = outage_duration))[1]
    county_num_id <-
      paste0(unique(county$counties))
    num_out_row <- data.frame(county_num_id, num_outages)
    all_an_dat <- bind_rows(all_an_dat, an_dat)
    num_outages_frame <- bind_rows(num_outages_frame, num_out_row)
  }
  
  num_outages_frame_50_p_missing <- num_outages_frame %>% filter(county_num_id != 'testcase')
  all_an_dat_50_p_missing <- all_an_dat 
  
  all <- cbind(all_an_dat_no_missingness, all_an_dat_10_p_missing$exposed,
               all_an_dat_30_p_missing$exposed, 
               all_an_dat_50_p_missing$exposed)
  
  colnames(all) <- c("counties", "day", "exposed_none_missing",
                     "exposed_10_p_missing",
                     "exposed_30_p_missing",
                     "exposed_50_p_missing")
  
  # Write -------------------------------------------------------------------
  
  saveRDS(
    all,
    here(
      "power_outage_medicare_data",
      "power_outage_simulation_cleaned_data",
      "cleaned_with_missingness",
      paste0("days_exposed_unexposed_", p, ".RDS")
    )
  )
  
}

# Load the parallel package
library(parallel)

# Determine the number of cores
num_cores <- detectCores()

# Create a cluster
cl <- makeCluster(num_cores) # don't want to overload memory 

# Export any objects needed by the computation to the cluster
clusterExport(cl, ls()) # quite of lot of objects are needed

clusterEvalQ(cl, {library(tidyverse)
  library(here)
  library(data.table)
  library(lubridate)})


# Use parLapply to apply a function in parallel
result <- parLapply(cl, X = ps, fun = clean_simulated_data)

# Stop the cluster
stopCluster(cl)


# list_of_nulls <- lapply(X = is, FUN = create_one_simed_county)
# 
# saveRDS(
#   all_an_dat_no_missingness,
#   here(
#     "power_outage_medicare_data",
#     "power_outage_simulation_cleaned_data",
#     "cleaned_8_hrs_ref_no_missing",
#     paste0("days_exposed_unexposed_", p, ".RDS")
#   )
# )
# saveRDS(
#   num_outages_frame,
#   here(
#     "power_outage_medicare_data",
#     "power_outage_simulation_cleaned_data",
#     "cleaned_8_hrs_ref_no_missing",
#     paste0("num_outages_g_8_hrs_", p, ".RDS")
#   )
# )
# 
# # Write -------------------------------------------------------------------
# 
# saveRDS(
#   all_an_dat_10_p_missing,
#   here(
#     "power_outage_medicare_data",
#     "power_outage_simulation_cleaned_data",
#     "cleaned_8_hrs_10_p_missing",
#     paste0("days_exposed_unexposed_", p, ".RDS")
#   )
# )
# saveRDS(
#   num_outages_frame_10_p_missing,
#   here(
#     "power_outage_medicare_data",
#     "power_outage_simulation_cleaned_data",
#     "cleaned_8_hrs_10_p_missing",
#     paste0("num_outages_g_8_hrs_", p, ".RDS")
#   )
# )
# 
# 
# saveRDS(
#   all_an_dat_30_p_missing,
#   here(
#     "power_outage_medicare_data",
#     "power_outage_simulation_cleaned_data",
#     "cleaned_8_hrs_30_p_missing",
#     paste0("days_exposed_unexposed_", p, ".RDS")
#   )
# )
# saveRDS(
#   num_outages_frame_30_p_missing,
#   here(
#     "power_outage_medicare_data",
#     "power_outage_simulation_cleaned_data",
#     "cleaned_8_hrs_30_p_missing",
#     paste0("num_outages_g_8_hrs_", p, ".RDS")
#   )
# )