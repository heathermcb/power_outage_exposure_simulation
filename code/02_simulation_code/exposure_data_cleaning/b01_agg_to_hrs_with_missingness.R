# This script will take the simulated, generated data and aggregate it to the
# hourly county level, as well as add a county ID for the simulation, and
# create versions of each county that are missing increasing percentages
# of exposure data. 

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
       'power_outage_simulation_created_data',
       'hourly_county_data_with_missingness')

# get files to read
simed_data_files <- 
  list.files(source_folder, pattern = "\\.RDS$", full.names = TRUE)

# Wrapper for aggregation -------------------------------------------------

# function to process each file
process_file <- function(file_path, destination_folder) {
  # read rds file
  po_data <- read_rds(file_path) |> setDT()
  setnames(po_data, old = 'ten_min_times', new = 'datetimes')

# here introduce missingness ----------------------------------------------
  
  # find number of customers in each county 
  county_cust_by_pod <-
    unique(po_data[, .(counties, pod_id, customers_by_pod)])

  county_cust <-
    county_cust_by_pod[, .(customers_by_county = sum(customers_by_pod)),
                       by = counties]
  
  # find what 10 percent of county customers looks like for each county, 
  # etc, for different levels of missingness 
  county_cust[, `:=`(
    ten_p = floor(0.10 * customers_by_county),
    thirty_p = floor(0.30 * customers_by_county),
    fifty_p = floor(0.50 * customers_by_county),
    seventy_p = floor(0.70 * customers_by_county)
  )]
  
  # get a running total of how manu customers were in this pod and the pods 
  # before it 
  county_cust_by_pod <-
    county_cust_by_pod[, cum_customers := cumsum(customers_by_pod),
                       by = counties]
  
  county_cust_by_pod <- county_cust[county_cust_by_pod, on = 'counties']
  
  county_cust_by_pod <- county_cust_by_pod[, `:=` (
    remove_for_10_p_missing = fifelse(cum_customers <= ten_p, 1, 0),
    remove_for_30_p_missing = fifelse(cum_customers <= thirty_p, 1, 0),
    remove_for_50_p_missing = fifelse(cum_customers <= fifty_p, 1, 0),
    remove_for_70_p_missing = fifelse(cum_customers <= seventy_p, 1, 0)
  ) ]
  
  county_cust_by_pod <-
    county_cust_by_pod[, .(
      counties,
      pod_id,
      customers_by_county,
      remove_for_10_p_missing,
      remove_for_30_p_missing,
      remove_for_50_p_missing,
      remove_for_70_p_missing
    )]
  
  po_data <- county_cust_by_pod[po_data, 
                                  on = c('counties', 'pod_id')]
  
  po_data[, `:=` (
    new_customer_counts_10p = fifelse(remove_for_10_p_missing == 1, 0, 
                                      customers_out_counts),
    new_customer_counts_30p = fifelse(remove_for_30_p_missing == 1, 0, 
                                      customers_out_counts),
    new_customer_counts_50p = fifelse(remove_for_50_p_missing == 1, 0, 
                                      customers_out_counts),
    new_customer_counts_70p = fifelse(remove_for_70_p_missing == 1, 0,
                                      customers_out_counts)
  )]
  
  
  po_data <-
    po_data[, .(
      customers_out_ten_min_by_county = sum(customers_out_counts),
      customers_served_ten_min_by_county = sum(customers_by_pod),
      customers_out_10_p_missing = sum(new_customer_counts_10p),
      customers_out_30_p_missing = sum(new_customer_counts_30p),
      customers_out_50_p_missing = sum(new_customer_counts_50p),
      customers_out_70_p_missing = sum(new_customer_counts_70p)),
      by = .(counties, datetimes)]
  
# back to agg -------------------------------------------------------------
  
  # add chunk_id column based on file index
  chunk_id <- which(simed_data_files == file_path)
  po_data[, chunk_id := chunk_id]
  
  # aggregate to hour
  po_data <- po_data[, hour := floor_date(datetimes, unit = 'hour')]
  
  po_data <-
    po_data[, .(
      customers_out_hourly = round(mean(customers_out_ten_min_by_county)),
      customers_served_hourly = round(mean(customers_served_ten_min_by_county)),
      customers_out_10_p_missing_hourly = round(mean(customers_out_10_p_missing)),
      customers_out_30_p_missing_hourly = round(mean(customers_out_30_p_missing)),
      customers_out_50_p_missing_hourly = round(mean(customers_out_50_p_missing)),
      customers_out_70_p_missing_hourly = round(mean(customers_out_70_p_missing))
    ), by = .(counties, hour, chunk_id)]
  
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


