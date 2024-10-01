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
obs_threshold <- 1
proportion_na_threshold <- .5 # i don't think we want to remove counties rn either 

# helper
my.max <- function(x) ifelse( !all(is.na(x)), max(x, na.rm=T), NA)


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

l <- sort(unique(pous_data$state_county))
l_split <- split(l, ceiling(seq_along(l) / 20)) # going with ~ 150 chunks

#l_split <- l_split[1:5] # sample for working
# this is the slow part, but hopefully it's fast enough to make all my work
# worth it lol


# Do ----------------------------------------------------------------------
final_customer_served_est <- vector(mode = "list", length = length(l_split))
#hourly_data <- vector(mode = "list", length = length(l_split))

do_expand_process <- function(pous_data_chunk, i){
  pous_data_chunk <- as.data.table(pous_data_chunk)
  pous_dat_chunk <- pous_data[state_county %in% l_split[[i]]]
  # order so we can correctly identify missing observations
  pous_dat_chunk <- pous_dat_chunk[order(
    clean_state_name,
    clean_county_name,
    city_name,
    utility_name,
    fips,
    recorded_date_time,
    -customers_out
  )]
  
  # index observations to identify matching time stamps
  pous_dat_chunk <- pous_dat_chunk[, index := 1:.N, by = c(
    "clean_state_name",
    "clean_county_name",
    "city_name",
    "utility_name",
    "fips",
    "recorded_date_time"
  )]
  
  # replace missing indicators with a more obvious missingness indicator,
  # and move them to the right spot in the time series
  pous_dat_chunk <-
    pous_dat_chunk[, customers_out := ifelse(index == 2, -99, customers_out)]
  pous_dat_chunk <-
    pous_dat_chunk[, datetime := ifelse(index == 2,
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
  
  pous_dat_chunk <-
    pous_dat_chunk[, c("datetime", 
                       "hour", 
                       "day", 
                       "month", 
                       "year", 
                       "index") := NULL]
  
  all_dates_in_data <- # here we're creating a data.table with all the dates we 
    # want in the data 
    pous_dat_chunk[, .(date = seq(min(date),
                                  max(date), by = '10 mins')),
                   by =  c("clean_state_name",
                           "clean_county_name",
                           "city_name",
                           "utility_name",
                           "fips")]
  
  pous_dat_chunk <- # and here we're joining that to the data 
    pous_dat_chunk[all_dates_in_data, on = .(clean_state_name,
                                             clean_county_name,
                                             city_name,
                                             utility_name,
                                             fips,
                                             date), roll = TRUE]
  
  pous_dat_chunk <-
    pous_dat_chunk[, customers_out_api_on := ifelse(customers_out == -99,
                                                    NA, customers_out)]
  
  # replacing 99s with NAs 
  # this should give us a timeseries of dates, with customers out, and 
  # NAs. it should contain complete dates 
  
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
                     list(my.max(customers_tracked),
                          my.max(new_locf_rep)), by = c("utility_name",
                                                        "clean_state_name",
                                                        "clean_county_name",
                                                        "city_name",
                                                        "fips",
                                                        "state_county",
                                                        "year")]
  
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
  
  # this did locf, I'm not sure exactly how, and also added customer served est
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
  ) # this creates a backbone dataframe with all the utilites and cities that
  # shoudl be there 
  
  make_id <- make_id[, city_utility_name_id := 1:.N]
  # this gives them an ID
  
  pous_dat_chunk <- make_id[pous_dat_chunk, on = c(
    "utility_name",
    "clean_state_name",
    "clean_county_name",
    "city_name",
    "fips",
    "year"
  )] # joins it to the data 
  
  high_temporal_missingness <-
    pous_dat_chunk[, c('n', 'l')  :=
                     list(.N, sum(is.na(new_locf_rep))), by = city_utility_name_id
    ][, p_missing := l / n
    ][n <= obs_threshold |
        p_missing > proportion_na_threshold]
  
  high_temporal_missingness <-
    unique(high_temporal_missingness$city_utility_name_id)
  
  pous_dat_chunk <- 
    pous_dat_chunk[, htempmiss := ifelse(city_utility_name_id %in% high_temporal_missingness, 1, 0)]  
  # pous_dat_chunk <- # don't want this for now
  #   pous_dat_chunk[!(city_utility_name_id %in% high_temporal_missingness)]
  
  pous_dat_chunk <-
    pous_dat_chunk[, c("customers_out_10_min_period",
                       "customers_served_estimate_10_min_period") :=
                     list(sum(new_locf_rep, na.rm = T),
                          sum(city_utility_customers_served_est, na.rm = T)),
                   by = c("clean_state_name",
                          "clean_county_name",
                          "state_county",
                          "fips",
                          "year",
                          "date")]
  
  
  pous_dat_chunk<- unique(
    pous_dat_chunk,
    by = c(
      "clean_state_name",
      "clean_county_name",
      "state_county",
      "fips",
      "year",
      "date",
      "customers_out_10_min_period",
      "customers_served_estimate_10_min_period"
    )
  )
  
  pous_dat_chunk <- pous_dat_chunk[, hour := floor_date(date, unit = 'hour')]
  
  pous_dat_chunk <-
    pous_dat_chunk[, c("customers_out_hourly", "customers_served_hourly") :=
                     list(round(
                       mean(customers_out_10_min_period, na.rm = T), digits = 0),
                       round(
                         mean(customers_served_estimate_10_min_period, na.rm = T),
                         digits = 0
                       )),
                   by = c("clean_state_name",
                          "clean_county_name",
                          "state_county",
                          "fips",
                          "year",
                          "hour")]
  
  
  pous_dat_chunk<- unique(
    pous_dat_chunk,
    by = c(
      "clean_state_name",
      "clean_county_name",
      "state_county",
      "fips",
      "year",
      "hour",
      "customers_out_hourly",
      "customers_served_hourly"
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

library(foreach)
library(doParallel)

cl <- makeCluster(detectCores() - 1)  # Use one less than the total number of cores
registerDoParallel(cl)

clusterExport(cl, varlist = c("pous_data", "do_expand_process"))
clusterEvalQ(cl, {
  library(data.table)
  library(tidyverse)
  library(zoo)
  library(here)
  library(lubridate)
  library(data.table)
  library(imputeTS)
  library(fst)
  
})

# Parallelize the processing of the list
foreach(i = 1:length(l_split)) %dopar% {
  do_expand_process(pous_data, i)
}

# Stop the cluster
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




# ok ----------------------------------------------------------------------






for (i in 1:length(l_split)) {
  pous_dat_chunk <- pous_data[state_county %in% l_split[[i]]]
  
  # order so we can correctly identify missing observations
  pous_dat_chunk <- pous_dat_chunk[order(
    clean_state_name,
    clean_county_name,
    city_name,
    utility_name,
    fips,
    recorded_date_time,
    -customers_out
  )]
  
  # index observations to identify matching time stamps
  pous_dat_chunk <- pous_dat_chunk[, index := 1:.N, by = c(
    "clean_state_name",
    "clean_county_name",
    "city_name",
    "utility_name",
    "fips",
    "recorded_date_time"
  )]
  
  # replace missing indicators with a more obvious missingness indicator,
  # and move them to the right spot in the time series
  pous_dat_chunk <-
    pous_dat_chunk[, customers_out := ifelse(index == 2, -99, customers_out)]
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
                           "city_name",
                           "utility_name",
                           "fips")]
  
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
                     list(my.max(customers_tracked),
                          my.max(new_locf_rep)), by = c("utility_name",
                                                        "clean_state_name",
                                                        "clean_county_name",
                                                        "city_name",
                                                        "fips",
                                                        "state_county",
                                                        "year")]
  
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
  
  high_temporal_missingness <-
    pous_dat_chunk[, c('n', 'l')  :=
                     list(.N, sum(is.na(new_locf_rep))), by = city_utility_name_id
    ][, p_missing := l / n
    ][n <= obs_threshold |
        p_missing > proportion_na_threshold]
  
  high_temporal_missingness <-
    unique(high_temporal_missingness$city_utility_name_id)
  
  #pous_dat_chunk <- pous_dat_chunk[, htempmiss := ]
  
  # pous_dat_chunk <- # don't want this for now
  #   pous_dat_chunk[!(city_utility_name_id %in% high_temporal_missingness)]
  
  pous_dat_chunk <-
    pous_dat_chunk[, c("customers_out_10_min_period",
                       "customers_served_estimate_10_min_period") :=
                     list(sum(new_locf_rep, na.rm = T),
                          sum(city_utility_customers_served_est, na.rm = T)),
                   by = c("clean_state_name",
                          "clean_county_name",
                          "state_county",
                          "fips",
                          "year",
                          "date")]
  
  
  pous_dat_chunk<- unique(
    pous_dat_chunk,
    by = c(
      "clean_state_name",
      "clean_county_name",
      "state_county",
      "fips",
      "year",
      "date",
      "customers_out_10_min_period",
      "customers_served_estimate_10_min_period"
    )
  )
  
  pous_dat_chunk <- pous_dat_chunk[, hour := floor_date(date, unit = 'hour')]
  
  pous_dat_chunk <-
    pous_dat_chunk[, c("customers_out_hourly", "customers_served_hourly") :=
                     list(round(
                       mean(customers_out_10_min_period, na.rm = T), digits = 0),
                       round(
                         mean(customers_served_estimate_10_min_period, na.rm = T),
                         digits = 0
                       )),
                   by = c("clean_state_name",
                          "clean_county_name",
                          "state_county",
                          "fips",
                          "year",
                          "hour")]
  
  
  pous_dat_chunk<- unique(
    pous_dat_chunk,
    by = c(
      "clean_state_name",
      "clean_county_name",
      "state_county",
      "fips",
      "year",
      "hour",
      "customers_out_hourly",
      "customers_served_hourly"
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

