
# Libraries ---------------------------------------------------------------

library(here)
library(tidyverse)
library(data.table)


# Read --------------------------------------------------------------------

ls <- list.files(here("power_outage_medicare_data",
                      "power_outage_medicare_data_cleaning_output",
                      "days_exposed_unexposed"), full.names = T)
outages <- lapply(FUN = readRDS, X = ls)

# combine 
combined_df <- Reduce(function(x, y)
  merge(x, y, by = c(
    "clean_state_name", "clean_county_name", "day"
  )), outages)


write_rds(x = combined_df, file= here("power_outage_medicare_data",
               "power_outage_medicare_data_cleaning_output",
               'days_exposed_unexposed_for_all_cut_points_and_durations.RDS'))
