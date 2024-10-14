# This file contains all helper functions for power outage exposure data 
# cleaning.

# Author: Heather
# Last updated: Oct 2nd, 2024

pacman::p_load(sf, here, tidyverse)

# Functions ---------------------------------------------------------------

# helper lol - need this bc max needs more options in R :( 
# this function returns the maximum of a numeric variable if there is one, and 
# if all the entries in a vector are NA, it returns NA_real_ instead of Inf
my.max <- function(x) {
  if (sum(is.na(x)) == length(x)) {
    return(NA_real_)
  } else {
    return(max(x, na.rm = TRUE))
  }
}

# another helper - this function takes in a year and generates a data.table
# containing rows for a 10 min time series starting from jan 1 till dec 31
generate_intervals <- function(year) {
  start_time <- ymd_hms(paste0(year, "-01-01 00:00:00"))
  end_time <- ymd_hms(paste0(year, "-12-31 23:50:00"))
  seq(start_time, end_time, by = "10 mins")
}

# this pulls out a subset of the pous data for processing
get_chunk <- function(raw_pous_data, chunk_list, list_position){
  pous_chunk <- raw_pous_data[five_digit_fips %in% chunk_list[[list_position]]]
  pous_chunk <- pous_chunk[, year := lubridate::year(recorded_date_time)]
  pous_chunk <- pous_chunk[year < 2021 & year > 2017]
  return(pous_chunk)
}


# this creates city-utility ids for a chunk of the pous data - returns a frame
# with a unique ID for each city-utility
get_unique_city_utilities <- function(pous_dat_chunk) {
  id_frame <- unique(
    pous_dat_chunk[, .(clean_state_name,
                       clean_county_name,
                       five_digit_fips,
                       city_name,
                       utility_name)],
    by = c(
      "clean_state_name",
      "clean_county_name",
      "five_digit_fips",
      "city_name",
      "utility_name"
    )
  )
  id_frame <- id_frame[, city_utility_name_id := 1:.N]
  return(id_frame)
}

# this creates a ten-min time series backbone for a chunk of the POUS data
create_ten_min_series <- function(id_frame, intervals_dt){
  city_utility_time_series <-
    id_frame[, .(date = intervals_dt$date), by = .(
      city_utility_name_id
    )]
  return(city_utility_time_series)
}


# this identifies missing data in POUS dataframe 
id_missing_obs <- function(pous_dat_chunk){
  # order the pous data.table so we can correctly identify missing 
  # observation indicators 
  pous_dat_chunk <- pous_dat_chunk[order(
    clean_state_name,
    clean_county_name,
    five_digit_fips,
    city_name,
    utility_name,
    recorded_date_time,
    -customers_out
  )]
  
  # index observations to identify matching time stamps which are missing ob ind
  pous_dat_chunk[, index := 1:.N, by = c(
    "clean_state_name",
    "clean_county_name",
    "five_digit_fips",
    "city_name",
    "utility_name",
    "recorded_date_time"
  )]
  
  # replace missing indicators with a more obvious missingness indicator, NA,
  # and move them to the right spot in the time series
  pous_dat_chunk[index == 2, customers_out := -99]
  # remove duplicate rows (those are errors)
  pous_dat_chunk <- pous_dat_chunk[index < 3]
  
  # change datetime of missing obs to 10 minutes ahead of where they are
  # actually recorded in time series 
  pous_dat_chunk[, datetime := fifelse(
    index == 2,
    recorded_date_time + minutes(10),
    recorded_date_time
  )]
  return(pous_dat_chunk)
}

expand_to_10_min_intervals <- function(pous_dat_chunk){
  
  # change the datetime to correct class
  pous_dat_chunk[, datetime := lubridate::as_datetime(datetime)]
  
  # need to finagle getting the 10 minute floor of dates, which is a little 
  # tricky - using these next two code snippets from Matt
  # first create a floor date deconstructed
  pous_dat_chunk[, `:=` (
    year = lubridate::year(datetime),
    month = lubridate::month(datetime),
    day = lubridate::day(datetime),
    hour = lubridate::hour(datetime),
    minute = 10 * floor(lubridate::minute(datetime) / 10)
  )]
  
  # then reconstruct it
  pous_dat_chunk[, date := lubridate::ymd_hm(sprintf(
    "%s-%s-%s %s:%s",
    year,
    month,
    day,
    hour,
    minute))]
  
  # set the order of the datatable bc in ten minute periods with more than one
  # observation, we're going to keep the last one only
  setorder(
    pous_dat_chunk,
    clean_state_name,
    clean_county_name,
    five_digit_fips,
    city_name,
    utility_name,
    datetime
  )
  
  # and pick out the last observation
  pous_dat_chunk <-
    pous_dat_chunk[, .SD[.N], by = .(clean_state_name,
                                     clean_county_name,
                                     five_digit_fips,
                                     city_name,
                                     utility_name,
                                     date)]
  
  # delete old unnecessary cols
  pous_dat_chunk[, 
                 c("datetime",
                   "hour",
                   "day",
                   "month",
                   "index") := NULL]
  
  return(pous_dat_chunk)
}

add_NAs_to_chunk <- function(pous_dat_chunk){
  # replace -99 marker with NA 
  pous_dat_chunk[, customers_out_api_on :=
                   ifelse(customers_out == -99, NA, customers_out)]
  return(pous_dat_chunk)
  
}

expand_to_full_year <- function(pous_dat_chunk,
                                city_utilities,
                                city_utility_time_series) {
  setkey(pous_dat_chunk, clean_state_name, clean_county_name, five_digit_fips,
         city_name, utility_name)
  setkey(city_utilities, clean_state_name, clean_county_name, five_digit_fips,
         city_name, utility_name)

  # join
  pous_dat_chunk <- city_utilities[pous_dat_chunk, on = c(
    "clean_state_name",
    "clean_county_name",
    "five_digit_fips",
    "city_name",
    "utility_name"
  )]
  
  # need to join to original backbone now to get all time we're supposed to have 
  # flag - slow
  setkey(pous_dat_chunk, city_utility_name_id, date)
  setkey(city_utility_time_series, city_utility_name_id, date)
  
  pous_dat_chunk <- pous_dat_chunk[city_utility_time_series, on = c(
    "city_utility_name_id",
    "date"
  )]
  
  pous_dat_chunk[, year := lubridate::year(date)] 
  
  pous_dat_chunk <- pous_dat_chunk[, .(
    city_utility_name_id,
    date,
    year,
    recorded_date_time,
    customers_tracked,
    customers_out,
    customers_out_api_on
  )]

  return(pous_dat_chunk)
}

# add last observation carried forward to time series in chunk
add_locf_to_chunk <- function(pous_dat_chunk, max_nas_to_impute){
  
  # old
  pous_dat_chunk[, new_locf_rep := zoo::na.locf(
    customers_out_api_on,
    na.rm = FALSE,
    maxgap = max_nas_to_impute
  ), by = c("city_utility_name_id")]
  
  pous_dat_chunk[, c("recorded_date_time") := NULL]
  return(pous_dat_chunk)
}

calculate_customer_served_est <- function(pous_dat_chunk){
  
  # get max customers served by city-utility
  pous_dat_chunk[, 
                 c("max_customers_tracked_city_u", 
                   "max_customer_out_city_u") :=
                   list(my.max(customers_tracked), 
                        my.max(new_locf_rep)), 
                 by = c(
                   "city_utility_name_id",
                   "year")
  ]
  
  # select out into a separate datatable
  chunk <-
    pous_dat_chunk[, .(
      city_utility_name_id,
      year,
      max_customers_tracked_city_u,
      max_customer_out_city_u
    )]
  
  chunk <- unique(
    chunk,
    by =  c(
      "city_utility_name_id",
      "max_customers_tracked_city_u",
      "max_customer_out_city_u",
      "year"
    )
  )
  
  # estimate of county customers served will be the max of customers served 
  # and customers out within that year - do
  chunk[, city_utility_customers_served_est :=
          pmax(max_customers_tracked_city_u,
               max_customer_out_city_u,
               na.rm = T),  by = c(
                 "city_utility_name_id",
                 "year")]
  
  chunk[,c("max_customers_tracked_city_u",
           "max_customer_out_city_u") := NULL]
  
  # chunk <- chunk[, .(
  #   county_cust_served_est = sum(city_utility_customers_served_est, 
  #                                na.rm = TRUE)
  # ), by = .(city_utility_name_id, year)]
  # 
  # join back estimates to main datatable
  pous_dat_chunk <- pous_dat_chunk[chunk, on =  c(
    "city_utility_name_id",
    "year"
  )]  
  
  pous_dat_chunk[,c("max_customers_tracked_city_u",
           "max_customer_out_city_u") := NULL]
  
  return(pous_dat_chunk)
}

# add estimates of person time missing by city utility to pous dat chunk
add_person_time_missing <- function(pous_dat_chunk, city_utilities){
  
  pous_dat_chunk <- city_utilities[pous_dat_chunk, on = "city_utility_name_id"]
  
  # calculate the raw number of obs missing from each city-utility
  pous_dat_chunk[, n_ten_min_missing :=
                   sum(is.na(new_locf_rep)), 
                 by = c("clean_state_name",
                        "clean_county_name",
                        "five_digit_fips",
                        'city_utility_name_id',
                        'city_name', 
                        'utility_name',
                        'year')]
  
  # make separate data table w missingness to sum to county
  p_t_missing <- pous_dat_chunk[
    , .(clean_state_name, 
        clean_county_name, 
        five_digit_fips, 
        city_name, 
        utility_name, 
        city_utility_name_id, 
        year, 
        n_ten_min_missing)
  ][
    , unique(.SD)
  ][
    , .(county_person_time_missing_ten_min_periods = 
          sum(n_ten_min_missing, na.rm = TRUE)), 
    by = .(clean_state_name, clean_county_name, five_digit_fips, year)
  ]
  
  # join back to original data.table
  setkey(pous_dat_chunk,
         clean_state_name,
         clean_county_name,
         five_digit_fips,
         year)
  setkey(p_t_missing,
         clean_state_name,
         clean_county_name,
         five_digit_fips,
         year)
  
  # Perform the left join using the [] syntax
  pous_dat_chunk <- p_t_missing[pous_dat_chunk]
  
  return(pous_dat_chunk)
  
}

aggregate_customers_out_to_hour <- function(pous_dat_chunk) {
  # assign hour
  pous_dat_chunk[, hour := floor_date(date, unit = 'hour')]
  
  # alternative agg to hour
  pous_dat_chunk <- pous_dat_chunk[, .(
    customers_out_hourly_locf = round(mean(new_locf_rep, na.rm = TRUE), 
                                      digits = 0),
    customers_out_hourly_no_locf = round(mean(customers_out_api_on, na.rm = TRUE), 
                                         digits = 0)
  ), by = .(
    clean_state_name,
    clean_county_name,
    five_digit_fips,
    year,
    city_name,
    utility_name,
    city_utility_name_id,
    city_utility_customers_served_est,
    county_person_time_missing_ten_min_periods,
    hour
  )]
  return(pous_dat_chunk)
}


# add estimates of person time missing by city utility to pous dat chunk at 
# the hourly level 
add_person_time_missing_hourly <- function(pous_dat_chunk){
  
  # calculate the raw number of obs missing from each city-utility
  pous_dat_chunk[, n_hrs_missing :=
                   sum(is.na(customers_out_hourly_locf)), 
                 by = c("clean_state_name",
                        "clean_county_name",
                        "five_digit_fips",
                        'city_utility_name_id',
                        'city_name', 
                        'utility_name',
                        'year')]
  
  # make separate data table w missingness to sum to county
  p_t_missing <- pous_dat_chunk[
    , .(clean_state_name, 
        clean_county_name, 
        five_digit_fips, 
        city_name, 
        utility_name, 
        city_utility_name_id, 
        year, 
        n_hrs_missing)
  ][
    , unique(.SD)
  ][
    , .(county_person_time_missing_hours = sum(n_hrs_missing, na.rm = TRUE)), 
    by = .(clean_state_name, clean_county_name, five_digit_fips, year)
  ]
  
  # join back to original data.table
  setkey(pous_dat_chunk,
         clean_state_name,
         clean_county_name,
         five_digit_fips,
         year)
  setkey(p_t_missing,
         clean_state_name,
         clean_county_name,
         five_digit_fips,
         year)
  
  # Perform the left join using the [] syntax
  pous_dat_chunk <- p_t_missing[pous_dat_chunk]
  
  return(pous_dat_chunk)
  
}


sum_customers_out_to_county_hourly <- function(pous_dat_chunk) {
  # sum customers without power to county level
  pous_dat_chunk <- pous_dat_chunk[, .(
    customers_out_hourly_locf = sum(customers_out_hourly_locf, na.rm = TRUE),
    customers_out_hourly_no_locf = sum(customers_out_hourly_no_locf, 
                                              na.rm = TRUE),
    customers_served_county = sum(city_utility_customers_served_est, 
                                  na.rm = TRUE)
  ), by = .(
    clean_state_name,
    clean_county_name,
    five_digit_fips,
    hour,
    year,
    county_person_time_missing_hours
  )]
  return(pous_dat_chunk)
}



# ID outages functions ----------------------------------------------------

# identify hours that are exposed to power outage 
# can easily do all counties at once
identify_power_outage_on_all_counties <- function(counties, cut_point) {
  col_name <- paste0("po_on_", cut_point)
  counties[, cutoff := customers_served_total * cut_point, 
           by = five_digit_fips]
  counties[, po_on := case_when(customers_out_hourly > cutoff ~ 1, TRUE ~ 0), 
           by = five_digit_fips]
  counties[, po_id := case_when((po_on == 1) & (lag(po_on) == 0) ~ 1, TRUE ~ 0), 
           by = five_digit_fips]
  counties[, po_id := case_when(po_on == 1 ~ cumsum(po_id), TRUE ~ 0), 
           by = five_digit_fips]
  counties[, (col_name) := po_on]
  return(counties)
}

