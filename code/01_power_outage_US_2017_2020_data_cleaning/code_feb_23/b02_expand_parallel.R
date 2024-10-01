# Expand Out Power Outages

# This script expands power outage data into a time series, from its raw
# form where the dataset only includes entries for changes in customers_out.
# It does this one county at a time, and saves each expanded county
# in the 'expanded counties' folder in the 'data' folder.

# Date: Sep 24th, 2024
# Updated: Sep 26th, 2024
# Author: Heather
# Memory to run: ~50 GB? sorry lol ten min periods for 4 years is a lot of data
# about 900 milion obs. no way around that when we're optimizing for speed

# Libraries ---------------------------------------------------------------

library(pacman) # i pacmaned for u lauren
# load and install the required packages
p_load(
  tidyverse,
  zoo,
  here,
  lubridate,
  data.table,
  imputeTS,
  fst,
  parallel
)

# Constants ---------------------------------------------------------------

# just to make sure we make the same chunks each time
set.seed(7)

# we will make a version of the time series with customers out counts that 
# have last obs carried forward, but we will impute a maximum of 4 hours 
hour_thrshld <- dhours(4)
max_nas_to_impute <- hour_thrshld / dminutes(x = 10)
start <- Sys.time()

# Helpers -----------------------------------------------------------------

# helper lol - need this bc max needs more options in R :( 
my.max <- function(x) {
  if (sum(is.na(x)) == length(x)) {
    return(NA_real_)
  } else {
    return(max(x, na.rm = TRUE))
  }
}

# another helper - this function takes in a year and generates a 10 min time 
# series starting from jan 1 till dec 31
generate_intervals <- function(year) {
  start_time <- ymd_hms(paste0(year, "-01-01 00:00:00"))
  end_time <- ymd_hms(paste0(year, "-12-31 23:50:00"))
  seq(start_time, end_time, by = "10 mins")
}

# Prep --------------------------------------------------------------------

# generate intervals for years we care about to join onto 
intervals_2017 <- generate_intervals(2017)
intervals_2018 <- generate_intervals(2018)
intervals_2019 <- generate_intervals(2019)
intervals_2020 <- generate_intervals(2020)

all_intervals <-
  c(intervals_2017,
    intervals_2018,
    intervals_2019,
    intervals_2020)

# make backbone data.table with 10 min time series for all relevant years
intervals_dt <- data.table(date = all_intervals)

# Data --------------------------------------------------------------------

# read in raw data with cleaned names 
pous_data <-
  read_fst(
    here(
      "power_outage_medicare_data",
      "power_outage_medicare_data_cleaning_output",
      "raw_with_fips.fst"
    )
  ) |>
  as.data.table() 

# unique ids: fips codes for county, and city_name and utility_name together 
# for the city-utility unit

# have to expand this data in chunks due to R's vector limits
# want min number of chunks without exceeding the limit for max performance
# going with ~ 150 chunks
# pous_list <- sort(unique(pous_data$fips))
# pous_l_split <- split(pous_list, ceiling(seq_along(pous_list) / 20))

# Do ----------------------------------------------------------------------

final_customer_served_est <- # going to save customers served est as we go
  vector(mode = "list", length = length(pous_data))

pous_data <- split(pous_data, by = 'fips')

process_chunk <- function(chunk_data, intervals_dt, max_nas_to_impute){
  start = Sys.time()
  # separate chunk
  pous_dat_chunk <- pous_data[[i]]
  
  # section 1: create timeseries  -------------------------------------------
  
  # make id for city-utility-year
  make_id <- unique(
    pous_dat_chunk[, .(clean_state_name,
                       clean_county_name,
                       fips,
                       city_name,
                       utility_name)],
    by = c(
      "clean_state_name",
      "clean_county_name",
      "fips",
      "city_name",
      "utility_name"
    )
  )
  
  # make year data.table
  years <- data.table(year = 2017:2020)
  
  # expand this to a datatable containing one entry for every city-utility
  # and year, so four entries per city-utility
  make_id <- 
    make_id[, .(year = years$year), by = .(clean_state_name,
                                           clean_county_name,
                                           fips,
                                           city_name,
                                           utility_name)]
  
  # add unique ID
  make_id <- make_id[, city_utility_name_id := 1:.N]
  
  
  # expand the datatable into a timeseries of intervals for every year - SLOW -----
  city_utility_time_series <-
    make_id[, .(date = intervals_dt$date), by = .(
      clean_state_name,
      clean_county_name,
      fips,
      city_name,
      utility_name,
      city_utility_name_id
    )]
  
  # order the pous data.table so we can correctly identify missing 
  # observation indicators 
  pous_dat_chunk <- pous_dat_chunk[order(
    clean_state_name,
    clean_county_name,
    fips,
    city_name,
    utility_name,
    recorded_date_time,
    -customers_out
  )]
  
  # index observations to identify matching time stamps which are missing ob ind
  pous_dat_chunk[, index := 1:.N, by = c(
    "clean_state_name",
    "clean_county_name",
    "fips",
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
    fips,
    city_name,
    utility_name,
    datetime
  )
  
  # and pick out the last observation
  pous_dat_chunk <-
    pous_dat_chunk[, .SD[.N], by = .(clean_state_name,
                                     clean_county_name,
                                     fips,
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
  
  # now expand out the dates that are in the data.table to make a complete 
  # list of ten-minute intervals, from the first to last observation for each 
  # city-utility in the data 
  all_dates_in_data <-
    pous_dat_chunk[, .(date = seq(min(date),
                                  max(date), by = '10 mins')),
                   by =  c("clean_state_name",
                           "clean_county_name",
                           "fips",
                           "city_name",
                           "utility_name")]
  
  # join the power outage data back to that date backbone, rolling to fill in 
  pous_dat_chunk <-
    pous_dat_chunk[all_dates_in_data, on = .(clean_state_name,
                                             clean_county_name,
                                             city_name,
                                             utility_name,
                                             fips,
                                             date), roll = TRUE]
  # replace -99 marker with NA 
  pous_dat_chunk[, customers_out_api_on :=
                   ifelse(customers_out == -99, NA, customers_out)]
  
  # need to join to original backbone now to get all time we're supposed to have # flag - slow
  pous_dat_chunk <- pous_dat_chunk[city_utility_time_series, on = c(
    'clean_state_name',
    "clean_county_name",
    "fips",
    "city_name",
    "utility_name",
    "date"
  )]
  
  pous_dat_chunk[, year := lubridate::year(date)] 
  
  # add locf by city-utility, max 4 hours
  pous_dat_chunk[, new_locf_rep := na_locf(
    customers_out_api_on,
    option = 'locf',
    na_remaining = 'keep',
    maxgap = max_nas_to_impute
  ), by = c("clean_state_name",
            "clean_county_name",
            "fips",
            "city_name",
            "utility_name")]
  
  pous_dat_chunk[, c("minute", "recorded_date_time") := NULL]
  
  # section 2: add customer served estimates --------------------------------
  
  # get max customers served by city-utility
  pous_dat_chunk[, 
                 c("max_customers_tracked_city_u", 
                   "max_customer_out_city_u") :=
                   list(my.max(customers_tracked), 
                        my.max(new_locf_rep)), 
                 by = c(
                   "clean_state_name",
                   "clean_county_name",
                   "fips",
                   "city_name",
                   "utility_name",
                   "year")
  ]
  
  # select out into a separate datatable
  chunk <-
    pous_dat_chunk[, .(
      clean_state_name,
      clean_county_name,
      fips,
      city_name,
      utility_name,
      year,
      max_customers_tracked_city_u,
      max_customer_out_city_u
    )]
  
  chunk <- unique(
    chunk,
    by =  c(
      "clean_state_name",
      "clean_county_name",
      "fips",
      "city_name",
      "utility_name",
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
               na.rm = T), by = 1:nrow(chunk)]
  
  chunk[,c("max_customers_tracked_city_u",
           "max_customer_out_city_u") := NULL]
  
  final_customer_served_est[[i]] <- chunk # save separately
  
  # join back estimates to main datatable
  pous_dat_chunk <- pous_dat_chunk[chunk, on =  c(
    "utility_name",
    "clean_state_name",
    "clean_county_name",
    "city_name",
    "fips",
    "year"
  )]  
  
  # section 3: calculate missingness ----------------------------------------
  
  # calculate the raw number of obs missing from each city-utility
  
  # we stopped here and this needs to be rewritten --------------------------
  
  pous_dat_chunk[, n_ten_min_missing :=
                   sum(is.na(new_locf_rep)), 
                 by = c('city_utility_name_id', 'year')]
  # 
  # # calculate person time missing by multiplying that by the number of customers
  # # served by that city-utility
  # pous_dat_chunk[,
  # person_time_missing := n_ten_min_missing * city_utility_customers_served_est]
  
  # create counts at county level both customers served and person-time missing 
  county_customers_served <- pous_dat_chunk[, .(
    county_customers_served = sum(city_utility_customers_served_est, na.rm = TRUE),
    county_person_time_missing = sum(n_ten_min_missing)
  ), by = .(clean_state_name, clean_county_name, fips, year)]
  
  # sum customers without power to county level 
  pous_dat_chunk <- pous_dat_chunk[, .(
    customers_out_10_min_period_locf = sum(new_locf_rep, na.rm = TRUE),
    customers_out_10_min_period_no_locf = sum(customers_out_api_on, na.rm = TRUE)
  ), by = .(clean_state_name, clean_county_name, fips, year, date)]
  
  
  # rejoin customer and person-time missing counts to county level 
  pous_dat_chunk <- merge(
    pous_dat_chunk,
    county_customers_served,
    by = c("clean_state_name", "clean_county_name", "fips", "year")
  )
  
  # section 4: aggregate to hour --------------------------------------------
  
  # assign hour
  pous_dat_chunk[, hour := floor_date(date, unit = 'hour')]
  
  # alternative agg to hour 
  pous_dat_chunk <- pous_dat_chunk[, .(
    customers_out_hourly_locf = round(mean(
      customers_out_10_min_period_locf, na.rm = TRUE
    ), digits = 0),
    customers_out_hourly_no_locf = round(
      mean(customers_out_10_min_period_no_locf, na.rm = TRUE),
      digits = 0
    ),
    county_person_time_missing = max(county_person_time_missing),
    # max because all the values are the same
    customers_served_hourly = round(max(county_customers_served, na.rm = TRUE), digits = 0)
  ), by = .(clean_state_name, clean_county_name, fips, year, hour)]
  
  write_fst(
    x = pous_dat_chunk,
    path = here(
      "power_outage_medicare_data",
      "power_outage_medicare_data_cleaning_output",
      'hourly_county',
      paste0(i,"_hourly_data.fst")
    )
  )
  
  rm(pous_dat_chunk)
  gc()
  print(i)
  end = Sys.time()
  print(start - end)
}


# try parallelizing -------------------------------------------------------

cl <- makeCluster(detectCores() - 1)

clusterExport(
  cl,
  c(
    "pous_data",
    "intervals_dt",
    "max_nas_to_impute",
    "my_max",
    "generate_intervals",
    "process_chunk"
  )
)

clusterEvalQ(cl, library(data.table))
clusterEvalQ(cl, library(lubridate))
clusterEvalQ(cl, library(imputeTS))

final_customer_served_est <- parLapply(cl, 1:length(pous_data), function(i) {
  chunk_data <- pous_data[[i]]
  process_chunk(chunk_data, intervals_dt, max_nas_to_impute)
})

stopCluster(cl)

final_customer_served_est <- rbindlist(final_customer_served_est)

fwrite(
  x = final_customer_served_est,
  file = here(
    "power_outage_medicare_data",
    "power_outage_medicare_data_cleaning_output",
    "city_utility_cust_estimates.csv")
)

iiijj2 <- Sys.time()