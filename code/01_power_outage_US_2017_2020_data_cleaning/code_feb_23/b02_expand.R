# Expand Out Power Outages

# This script expands power outage data into a time series, from its current
# form where the dataset only includes entries for changes in customers_out.
# It does this one county at a time, and saves each expanded county
# in the 'expanded counties' folder in the 'data' folder.
iiijj1 <- Sys.time()

# Libraries ---------------------------------------------------------------

library(tidyverse)
library(zoo)
library(here)
library(lubridate)
library(data.table)
library(imputeTS)
library(fst)

set.seed(7)

# Constants ---------------------------------------------------------------

hour_thrshld <- dhours(4) # here we're saying we'll impute over 4 hours? we need both imputation and not 
max_nas_to_impute <- hour_thrshld / dminutes(x = 10)

# helper
my.max <- function(x) {
  max(x, na.rm = TRUE)
}


# Data --------------------------------------------------------------------


# read in data 
pous_data <-
  read_fst(
    here(
      "power_outage_medicare_data",
      "power_outage_medicare_data_cleaning_output",
      "raw_with_fips.fst"
    )
  ) |>
  as.data.table()

# have to process this in chunks due to R's memory limits
# want min number of chunks without exceeding the limit for max performance

l <- sort(unique(pous_data$fips))
l_split <- split(l, ceiling(seq_along(l) / 20)) # going with ~ 150 chunks

#l_split <- l_split[1:5] # sample for working
# this is the slow part, but hopefully it's fast enough to make all my work
# worth it lol


# Do ----------------------------------------------------------------------
final_customer_served_est <- vector(mode = "list", length = length(l_split))
#hourly_data <- vector(mode = "list", length = length(l_split))

for (i in 1:length(l_split)) {
  pous_dat_chunk <- pous_data[fips %in% l_split[[i]]]
  
  # order so we can correctly identify missing observations
  pous_dat_chunk[order(
    clean_state_name,
    clean_county_name,
    fips,
    city_name,
    utility_name,
    recorded_date_time,
    -customers_out
  )]
  
  # index observations to identify matching time stamps
  pous_dat_chunk[, index := 1:.N, by = c(
    "clean_state_name",
    "clean_county_name",
    "fips",
    "city_name",
    "utility_name",
    "recorded_date_time"
  )]
  
  # replace missing indicators with a more obvious missingness indicator,
  # and move them to the right spot in the time series
  pous_dat_chunk[index == 2, customers_out := -99]
  
  pous_dat_chunk <- pous_dat_chunk[, datetime := ifelse(index == 2,
                                recorded_date_time + lubridate::minutes(10),
                                recorded_date_time)]
  
  pous_dat_chunk <- 
    pous_dat_chunk[, datetime := lubridate::as_datetime(datetime)]
  
  # remove duplicate rows (those are errors)
  pous_dat_chunk <- pous_dat_chunk[index < 3]
  
  # might want to keep last observations in groups? instead of first?
  # need to finagle getting the 10 minute floor of dates, which is a little 
  # tricky - using this code snippet from Matt
  
  pous_dat_chunk <- pous_dat_chunk[, `:=` (
    year = lubridate::year(datetime),
    month = lubridate::month(datetime),
    day = lubridate::day(datetime),
    hour = lubridate::hour(datetime),
    minute = 10 * floor(lubridate::minute(datetime) / 10)
  )]
  
  pous_dat_chunk <-
    pous_dat_chunk[, date := lubridate::ymd_hm(sprintf("%s-%s-%s %s:%s",
                                                       year,
                                                       month,
                                                       day,
                                                       hour,
                                                       minute))]
  
  setorder(pous_dat_chunk, clean_state_name, clean_county_name, fips, city_name, utility_name, datetime)
  
  
  result_dt <- pous_dat_chunk[, .SD[.N], by = .(clean_state_name, clean_county_name, fips, city_name, utility_name, date)]
  
  pous_dat_chunk <- result_dt
  
  # # Using dplyr
  # result_dplyr <- pous_dat_chunk %>%
  #   group_by(clean_state_name, clean_county_name, fips, city_name, utility_name, date) %>%
  #   slice_tail(n = 1)
  # 
  # # Compare the results
  # print(result_dt)
  # print(result_dplyr)
  # 
  # 
  # pous_dat_chunk_2 <- pous_dat_chunk |>
  #   group_by(clean_state_name, clean_county_name, fips, city_name, utility_name, date) |>
  #   slice_tail(n = 1)
  # 
  # 
  # # # need something here to keep first observation in group only i think
  # pous_dat_chunk[, .SD[.N], by = .(clean_state_name, clean_county_name, fips, city_name, utility_name, date)]
  # result_dt <- pous_dat_chunk[, .SD[.N], by = .(clean_state_name, clean_county_name, fips, city_name, utility_name, date)]
  
  
  
  # # index 
  # pous_dat_chunk[, index := 1:.N, by = c(
  #   "clean_state_name",
  #   "clean_county_name",
  #   "fips",
  #   "city_name",
  #   "utility_name",
  #   "date"
  # )]
  # 
  # # and filter to only last ob
  
  pous_dat_chunk <-
    pous_dat_chunk[, c("datetime", 
                       "hour", 
                       "day", 
                       "month", 
                       "year", 
                       "index") := NULL]
  
  all_dates_in_data <-
    pous_dat_chunk[, .(date = seq(min(date),
                                  max(date), by = '10 mins')),
                   by =  c("clean_state_name",
                           "clean_county_name",
                           "fips",
                           "city_name",
                           "utility_name")]
  
  pous_dat_chunk <-
    pous_dat_chunk[all_dates_in_data, on = .(clean_state_name,
                                             clean_county_name,
                                             city_name,
                                             utility_name,
                                             fips,
                                             date), roll = TRUE]
  
  pous_dat_chunk <-
    pous_dat_chunk[, customers_out_api_on := ifelse(customers_out == -99,
                                                    NA, customers_out)]

  
# now we have a time series with NAs in it 
  # and we are adding some LOCF 
# check check
  result <- pous_dat_chunk[, .(n = .N), by = .(clean_state_name, clean_county_name, fips, city_name, utility_name, date)]
  
  result <- pous_dat_chunk[, .(n = .N), by = .(clean_state_name, clean_county_name, fips, city_name, utility_name)]

# NEW SCRIPT --------------------------------------------------------------

  # add locf by city-utility
  pous_dat_chunk <-
    pous_dat_chunk[, new_locf_rep := na_locf(
      customers_out_api_on,
      option = 'locf',
      na_remaining = 'keep',
      maxgap = max_nas_to_impute
    ), by = c("utility_name",
              "clean_state_name",
              "clean_county_name",
              "city_name",
              "fips")]
  
  pous_dat_chunk <- 
    pous_dat_chunk[, c("minute", "recorded_date_time") := NULL]
  
  # also add customers served estimates -------------------------------------
  # add year
  pous_dat_chunk <- pous_dat_chunk[, year := lubridate::year(date)]  
  
  # get max by city-utility
  pous_dat_chunk <-
    pous_dat_chunk[, c("max_customers_tracked_city_u", "max_customer_out_city_u") :=
                     list(my.max(customers_tracked), my.max(new_locf_rep)), by = c(
                       "clean_state_name",
                       "clean_county_name",
                       "fips",
                       "utility_name",
                       "city_name",
                       "state_county",
                       "year"
                     )]
  
  chunk <-
    pous_dat_chunk[, .(
      utility_name,
      clean_state_name,
      clean_county_name,
      city_name,
      fips,
      state_county,
      max_customers_tracked_city_u,
      max_customer_out_city_u,
      year
    )]
  
  chunk <- unique(
    chunk,
    by =  c(
      "utility_name",
      "clean_state_name",
      "clean_county_name",
      "city_name",
      "fips",
      "state_county",
      "max_customers_tracked_city_u",
      "max_customer_out_city_u",
      "year"
    )
  )
  
  chunk <-
    chunk[, city_utility_customers_served_est :=
            pmax(max_customers_tracked_city_u,
                 max_customer_out_city_u,
                 na.rm = T), by = 1:nrow(chunk)]
  
  chunk <- chunk[,c("max_customers_tracked_city_u",
                    "max_customer_out_city_u") := NULL]
  
  final_customer_served_est[[i]] <- chunk
  
  pous_dat_chunk <- pous_dat_chunk[chunk, on =  c(
    "utility_name",
    "clean_state_name",
    "clean_county_name",
    "city_name",
    "fips",
    "state_county",
    "year"
  )]  
  
# ok we made a new column w locf and we calculated customers served 
  
# NEW SCRIPT --------------------------------------------------------------

  make_id <- pous_dat_chunk[, .(utility_name,
                        clean_state_name,
                        clean_county_name,
                        city_name,
                        fips,
                        year)]
  
  make_id <- unique(
    make_id,
    by = c(
      "utility_name",
      "clean_state_name",
      "clean_county_name",
      "city_name",
      "fips",
      "year"
    )
  )
  
  make_id <- make_id[, city_utility_name_id := 1:.N]
  
  pous_dat_chunk <- make_id[pous_dat_chunk, on = c(
    "utility_name",
    "clean_state_name",
    "clean_county_name",
    "city_name",
    "fips",
    "year"
  )]
  
  # high_temporal_missingness <-
  #   pous_dat_chunk[, c('n', 'l')  :=
  #            list(.N, sum(is.na(new_locf_rep))), by = city_utility_name_id
  #   ][, p_missing := l / n
  #   ][n <= obs_threshold |
  #       p_missing > proportion_na_threshold]
  
  pous_dat_chunk <-
    pous_dat_chunk[, c('n_missing')  :=
                     sum(is.na(new_locf_rep)), by = city_utility_name_id]
  
  pous_dat_chunk <- 
    pous_dat_chunk[, person_time_missing := n_missing * city_utility_customers_served_est]
  
  county_customers_served <- pous_dat_chunk[, .(
    county_customers_served = sum(city_utility_customers_served_est, na.rm = TRUE),
    county_person_time_missing = sum(person_time_missing)
  ), by = .(clean_state_name, clean_county_name, fips, year)]
  
  pous_dat_chunk <- pous_dat_chunk[, .(
    customers_out_10_min_period_locf = sum(new_locf_rep, na.rm = TRUE),
    customers_out_10_min_period_no_locf = sum(customers_out_api_on, na.rm = TRUE)
  ), by = .(clean_state_name, clean_county_name, fips, year, date)]
  
  
  # Merge the result back to the original data
  pous_dat_chunk <- merge(
    pous_dat_chunk,
    county_customers_served,
    by = c("clean_state_name", "clean_county_name", "fips", "year")
  )
  
  pous_dat_chunk <- pous_dat_chunk[, hour := floor_date(date, unit = 'hour')]
  
  pous_dat_chunk <-
    pous_dat_chunk[, c("customers_out_hourly_locf",
                       "customers_out_hourly_no_locf",
                       "county_person_time_missing",
                       "customers_served_hourly") :=
             list(round(
               mean(customers_out_10_min_period_locf, na.rm = T), digits = 0),
             round(mean(customers_out_10_min_period_no_locf, na.rm = T), digits = 0),
                max(county_person_time_missing),
               round(
                 max(county_customers_served, na.rm = T),
                 digits = 0
               )),
           by = c("clean_state_name",
                  "clean_county_name",
                  "fips",
                  "year",
                  "hour")]
  
  
  pous_dat_chunk<- unique(
    pous_dat_chunk,
    by = c(
      "clean_state_name",
      "clean_county_name",
      "fips",
      "year",
      "hour",
      "customers_out_hourly_locf",
      "customers_out_hourly_no_locf",
      "county_customers_served",
      "county_person_time_missing"
    )
  )
  
  write_fst(
    x = pous_dat_chunk,
    path = here(
      "power_outage_medicare_data",
      "power_outage_medicare_data_cleaning_output",
      'hourly_county',
      paste0(i,"hourly_data.fst")
    )
  )
  
  rm(pous_dat_chunk)
  gc()
  print(i)
}


final_customer_served_est <- rbindlist(final_customer_served_est)

fwrite(
  x = final_customer_served_est,
  file = here(
    "power_outage_medicare_data",
    "power_outage_medicare_data_cleaning_output",
    "city_utility_cust_estimates.csv")
)

iiijj2 <- Sys.time()
