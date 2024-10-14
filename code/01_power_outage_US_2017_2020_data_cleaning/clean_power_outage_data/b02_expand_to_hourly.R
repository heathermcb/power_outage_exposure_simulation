# Expand Out Power Outages

# This script expands power outage data into a time series, from its raw
# form where the dataset only includes entries for changes in customers_out 
# (see the POUS documentation for an explanation of how the raw data is
# structured). It does this one county at a time, and saves each expanded county
# in the 'hourly county' folder in the 'data' folder. Need to clean up 
# ordering of this script. 

# Last updated: Oct 3rd, 2024
# Author: Heather

# Libraries ---------------------------------------------------------------

# i pacmaned in all these scripts for u lauren
pacman::p_load(tidyverse, zoo, here, lubridate, data.table, fst)

source(here("code", "functions", "exposure_data_cleaning_helpers_oct_9.R"))

# Constants ---------------------------------------------------------------

# just to make sure we make the same chunks each time
set.seed(7)

# we will make a version of the time series with customers out counts that 
# have last obs carried forward, but we will impute a maximum of 4 hours 
hour_thrshld <- dhours(4)
max_nas_to_impute <- hour_thrshld / dminutes(x = 5)

# fastest to do all years at once, taking advantage of data.table's 
# infrastructure - excluding 2017 though bc it's garbage
#intervals_2017 <- generate_intervals(2017)
intervals_2018 <- generate_intervals(2018)
intervals_2019 <- generate_intervals(2019)
intervals_2020 <- generate_intervals(2020)

intervals_dt <-
  data.table(date = c(
    #intervals_2017,
    intervals_2018,
    intervals_2019,
    intervals_2020
  ))
 
# Wrapper for processing --------------------------------------------------
process_chunk <- function(i, pous_data) {
  
  # get chunk to process
  pous_dat_chunk <-
    get_chunk(raw_pous_data = pous_data, chunk_list = pous_l_split,
              list_position = i)
  
  # get city-utility id frame
  city_utilities <- get_unique_city_utilities(pous_dat_chunk = pous_dat_chunk)
  
  # get ten min time series 
  # intervals <- 
  #   data.table(date = generate_intervals(year))
  
  ten_min_time_series <- 
    create_ten_min_series(id_frame = city_utilities, 
                          intervals_dt = intervals_dt)
  
  # id missing observations
  pous_dat_chunk <- 
    id_missing_obs(pous_dat_chunk = pous_dat_chunk)
  
  # expand the data to ten min intervals
  pous_dat_chunk <- 
    expand_to_10_min_intervals(pous_dat_chunk = pous_dat_chunk)
  
  # replace -99 missing data indicators with NAs
  pous_dat_chunk <- add_NAs_to_chunk(pous_dat_chunk = pous_dat_chunk)
  
  # expand to a full year
  pous_dat_chunk <- 
    expand_to_full_year(pous_dat_chunk = pous_dat_chunk, 
                        city_utility_time_series = ten_min_time_series,
                        city_utilities = city_utilities)
  
  # add an additional time series with locf to the data 
  pous_dat_chunk <- 
    add_locf_to_chunk(pous_dat_chunk = pous_dat_chunk,
                      max_nas_to_impute = max_nas_to_impute)
  
  # add customer served estimates by city_utility to chunk 
  pous_dat_chunk <- calculate_customer_served_est(pous_dat_chunk)
  
  # calculate person-time missing by city-utility ID and add it to data 
  pous_dat_chunk <- add_person_time_missing(pous_dat_chunk, 
                                            city_utilities = city_utilities)
  
  # want instead to sum customers out to hour here 
  pous_dat_chunk <- aggregate_customers_out_to_hour(pous_dat_chunk)
  
  # want to calculate person time missing at the hourly level here 
  pous_dat_chunk <- add_person_time_missing_hourly(pous_dat_chunk)
  
  # sum customers out to county
  pous_dat_chunk <- sum_customers_out_to_county_hourly(pous_dat_chunk)
  
  # write the chunk
  write_fst(
    x = pous_dat_chunk,
    path = here(
      "data",
      "power_outage_exposure_data_cleaning_output",
      'hourly_county',
      paste0(i,'_', "hourly_data.fst")
    )
  )
  print(paste("Processed chunk", i))
}

# Data --------------------------------------------------------------------

# read in raw data with cleaned names 
pous_data <-
  read_fst(
    here(
      "data",
      "power_outage_exposure_data_cleaning_output",
      "raw_with_fips.fst"
    )
  ) |>
  as.data.table() 

# unique ids: five_digit_fips codes for county, and city_name and utility_name 
# together for the city-utility unit

# have to expand this data in chunks due to R's vector limits
# want min number of chunks without exceeding the limit for max performance
# going with ~ 150 chunks; as few chunks as possible is best

pous_list <- sort(unique(pous_data$five_digit_fips))
pous_l_split <- split(pous_list, ceiling(seq_along(pous_list) / 15))

# Do ----------------------------------------------------------------------

lapply(FUN = process_chunk, X = 1:length(pous_l_split), pous_data = pous_data)

