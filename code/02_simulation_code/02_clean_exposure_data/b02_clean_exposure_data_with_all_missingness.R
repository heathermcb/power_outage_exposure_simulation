# This script will take the simulated, generated data and clean it
# in the same way the real data is cleaned.

# It will also clean it based on different health-relevant outage
# durations.

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here, data.table, lubridate, parallel, arrow)

source(
  here(
    "code",
    "01_power_outage_US_2017_2020_data_cleaning",
    "functions",
    "helpers_days_exposed_unexposed.R"
  )
)

# Read --------------------------------------------------------------------

simed_dat <- list.files(
  here(
    "data",
    'power_outage_simulation_created_data',
    'hourly_county_data_with_missingness'
  ),
  full.names = TRUE
) |>
  lapply(read_parquet) |>
  rbindlist()

# Constants ---------------------------------------------------------------

cut_points <- c(0.005)

# Add unique county ID ----------------------------------------------------

distinct_dat <- unique(simed_dat[, .(counties, chunk_id)])
distinct_dat[, unique_id := .I]

simed_dat[distinct_dat, on = .(counties, chunk_id), 
          `:=`(unique_id = i.unique_id)]

# sample ------------------------------------------------------------------

unique_counties <- unique(simed_dat$unique_id)
sampled_counties <- sample(unique_counties, size = 10)  # Sample 10 counties

# Filter data to include only sampled counties
sampled_simed_dat <- simed_dat[unique_id %in% sampled_counties]

# Function to process each chunk ------------------------------------------

process_chunk <- function(chunk, cut_point, duration) {
  get_exposure(chunk, cut_point = cut_point, outage_duration = duration)
}

# Helper function to prepare data -----------------------------------------

prepare_data <- function(data, customers_out_col) {
  # get the correct column for missingness 
  data[, `:=`(
    clean_county_name = as.character(unique_id),
    clean_state_name = as.character(unique_id),
    five_digit_fips = as.character(unique_id),
    year = year(hour),
    customers_served_total = customers_served_hourly
  )]
  
  # select relevant cols to look like real POUS data
  data <- data[, .(
    clean_county_name,
    clean_state_name,
    five_digit_fips,
    year,
    hour,
    customers_served_total,
    customers_out_hourly = get(customers_out_col)
  )]
  return(data)
}

# Process data with different missingness ---------------------------------

process_missingness <- function(data, customers_out_col, suffix) {
  
  # process chunk wise bc of memory
  chunks <- split(data, cut(as.integer(
    factor(data$five_digit_fips)
  ), 10, labels = FALSE))
  
  exposure <- rbindlist(lapply(
    chunks,
    process_chunk,
    cut_point = 0.005,
    duration = hours(8)
  ))
  
  # rename exposure columns to include the suffix
  setnames(
    exposure,
    old = grep("^exposed", names(exposure), value = TRUE),
    new = paste0(grep("^exposed", names(exposure), value = TRUE), "_", suffix)
  )
  
  return(exposure)
}

# None missing
simed_dat_none_missing <- 
  prepare_data(data = simed_dat, 
               customers_out_col = "customers_out_hourly")
exposure_none_missing <- 
  process_missingness(data = simed_dat_none_missing, 
                      customers_out_col = "customers_out_hourly", 
                      suffix = "none_missing")

# 20% missing
simed_dat_20_p_missing <-
  prepare_data(data = simed_dat, 
               customers_out_col = "customers_out_20_p_missing_hourly")
exposure_20_p_missing <-
  process_missingness(data = simed_dat_20_p_missing,
                      customers_out_col = "customers_out_20_p_missing_hourly",
                      suffix = "20_p_missing")

# 50% missing
simed_dat_50_p_missing <- 
  prepare_data(data = simed_dat, 
               customers_out_col = "customers_out_50_p_missing_hourly")
exposure_50_p_missing <- 
  process_missingness(data = simed_dat_50_p_missing,
                      customers_out_col = "customers_out_50_p_missing_hourly", 
                      suffix = "50_p_missing")

# 80% missing
simed_dat_80_p_missing <- 
  prepare_data(data = simed_dat, 
               customers_out_col = "customers_out_80_p_missing_hourly")
exposure_80_p_missing <- 
  process_missingness(data = simed_dat_80_p_missing, 
                      customers_out_col = "customers_out_80_p_missing_hourly", 
                      suffix = "80_p_missing")

# Combine exposures -------------------------------------------------------

all_exposures <- list(
  exposure_none_missing,
  exposure_20_p_missing,
  exposure_50_p_missing,
  exposure_80_p_missing
)

combined_df <- Reduce(function(x, y) {
  merge(x,
        y,
        by = c(
          "clean_state_name",
          "clean_county_name",
          'five_digit_fips',
          "day"
        ))
}, all_exposures)

exposure_cols <- grep("^exposed", names(combined_df), value = TRUE)

combined_df <- combined_df %>%
  select(county_id = five_digit_fips, day, all_of(exposure_cols))

# Write -------------------------------------------------------------------

saveRDS(
  combined_df,
  here(
    "data",
    "power_outage_simulation_cleaned_data",
    "days_exposed_unexposed_all_missingness.RDS"
  )
)