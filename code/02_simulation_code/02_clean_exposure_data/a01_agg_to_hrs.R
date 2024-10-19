# This script will take the simulated, generated data and aggregate it to the
# hourly county level, as well as add a county ID for the simulation. 

# It will also clean it based on different health-relevant outage 
# durations.

# Libraries ---------------------------------------------------------------

pacman::p_load(data.table, arrow, here, tidyverse, lubridate, parallel)

# Read info ---------------------------------------------------------------

# source and dest 
source_folder <-
  here('data',
       'power_outage_simulation_created_data',
       'exposure_datasets')

destination_folder <-
  here('data',
       'power_outage_simulation_cleaned_data',
       'hourly_county_data')

# get files to read - sort so same id assigned in both scripts
simed_data_files <- 
  sort(list.files(source_folder, pattern = "\\.RDS$", full.names = TRUE))

# Wrapper for aggregation -------------------------------------------------

# function to process each file
process_file <- function(file_path, destination_folder) {
  # read rds file
  po_data <- read_rds(file_path) |> setDT()
  setnames(po_data, old = 'ten_min_times', new = 'datetimes')
  
  # aggregate to hour 
  po_data <- po_data[order(
    counties,
    pod_id,
    customers_by_pod,
    datetimes
  )]
  
  po_data <-
    po_data[, .(
      customers_out_ten_min_by_county = sum(customers_out_counts),
      customers_served_ten_min_by_county = sum(customers_by_pod)),
      by = .(counties, datetimes)]
  
  po_data <- po_data[, hour := floor_date(datetimes, unit = 'hour')]
  
  po_data <-
    po_data[, .(
      customers_out_hourly = round(mean(customers_out_ten_min_by_county)),
      customers_served_hourly = round(mean(customers_served_ten_min_by_county))
    ), by = .(counties, hour)]
  
  # define the output file path
  output_file_path <-
    file.path(destination_folder,
              paste0("processed_", basename(file_path), ".parquet"))
  
  # write the processed data to a Parquet file
  write_parquet(po_data, output_file_path)
  
  # print a message indicating the file has been processed
  print(paste("Processed file:", file_path))
}

# Do ----------------------------------------------------------------------

num_cores <- detectCores()
mclapply(
  X = simed_data_files,
  FUN = process_file,
  destination_folder = destination_folder,
  mc.cores = num_cores
)


