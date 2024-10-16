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
    'hourly_county_data'
  ),
  full.names = TRUE
)

simed_dat <- lapply(FUN = read_parquet, X = simed_dat)
simed_dat <- rbindlist(simed_dat)

# Constants ---------------------------------------------------------------

cut_points <- c(0.005)
num_cores <- detectCores()

# Do ----------------------------------------------------------------------

# add unique county id
distinct_dat <- unique(simed_dat[, .(counties, chunk_id)])
# add unique ID column
distinct_dat[, unique_id := .I]

# join id
simed_dat[distinct_dat, on = .(counties, chunk_id), 
          `:=`(unique_id = i.unique_id)]

# make structure same as real POUS data
simed_dat[, `:=`(
  clean_county_name = as.character(unique_id),
  clean_state_name = as.character(unique_id),
  five_digit_fips = as.character(unique_id),
  year = year(hour),
  customers_served_total = customers_served_hourly
)]

simed_dat <- simed_dat[, .(
  clean_county_name,
  clean_state_name,
  five_digit_fips,
  year,
  hour,
  customers_out_hourly,
  customers_served_total
)]


# Get exposure chunk wise -------------------------------------------------

# split data into 10 chunks ensuring counties with the same five_digit_fips
# stay together
simed_dat_chunks <- split(simed_dat, cut(as.integer(factor(
  simed_dat$five_digit_fips
)), 10, labels = FALSE))

# Function to process each chunk
process_chunk <- function(chunk, cut_point, duration) {
  exposure <- get_exposure(chunk, cut_point = cut_point, 
                           outage_duration = duration)
  return(exposure)
}

# Process each chunk for different durations
exposure_8_hrs <- rbindlist(lapply(
  simed_dat_chunks,
  process_chunk,
  cut_point = 0.005,
  duration = hours(8)
))

exposure_12_hrs <- rbindlist(lapply(
  simed_dat_chunks,
  process_chunk,
  cut_point = 0.005,
  duration = hours(12)
))
exposure_4_hrs <- rbindlist(lapply(
  simed_dat_chunks,
  process_chunk,
  cut_point = 0.005,
  duration = hours(4)
))

# Join --------------------------------------------------------------------

all_exposures <- list(exposure_8_hrs, exposure_4_hrs, exposure_12_hrs)

combined_df <- Reduce(function(x, y)
  merge(
    x,
    y,
    by = c(
      "clean_state_name",
      "clean_county_name",
      'five_digit_fips',
      "day"
    )
  ), all_exposures)

exposure_cols <- grep("^exposed", names(combined_df), value = TRUE)

combined_df <- 
  combined_df %>%
  select(county_id = five_digit_fips, day, all_of(exposure_cols))


# leaving this here so we can match structure of previous ones
colnames(combined_df) <- c(
  "counties",
  "day",
  "exposed_under_8_hr_def",
  "exposed_under_4_hr_def",
  "exposed_under_12_hr_def"
)

# Write -------------------------------------------------------------------

saveRDS(
  all,
  here(
    "data",
    "power_outage_simulation_cleaned_data",
    "days_exposed_unexposed_all_durations.RDS")
  )

