# This script estimates the number of electrical customers by state using the 
# EIA data, so eventually we can generate county electrical customer estimates 
# and coverage information.

# Author: Heather
# Last updated: Oct 2nd, 2024

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, readxl, here)

# Read --------------------------------------------------------------------

# read list of states we hope to include
state_fips_abbrev <- read_rds(here('data', 'cotus_state_fips_abbrev.RDS'))

# read customer counts
# list eia dataset names - these are datasets containing electrical customer
# counts by state
eia_sets <-
  list.files(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "EIA"
    ),
    full.names = TRUE
  )

# read into a list for easy processing
cust_counts <-
  lapply(eia_sets,
         read_excel,
         skip = 3,
         col_names = F)

# set column types so that we can bind rows
cust_counts <-
  lapply(X = cust_counts,
         FUN = mutate,
         across(everything(), as.character))

# remove column that weirdly appears in only one year so we can bind rows
# i guess 2019 was a long form year for the short-form utilities
cust_counts[[2]] <- cust_counts[[2]] %>% select(-c("...10"))
colnames(cust_counts[[2]]) <- colnames(cust_counts[[1]])

# get columns we want to count customers by state and year
cust_counts <- lapply(
  X = cust_counts,
  FUN = select,
  year = "...1",
  utility_name = "...3",
  part = "...4",
  customer_counts = "...24",
  state_ab = "...7"
)

# set types for adding customer counts
cust_counts <-
  bind_rows(cust_counts) %>%
  mutate(customer_counts = as.numeric(na_if(x = customer_counts, y = '.'))) %>%
  drop_na(customer_counts)

# get final customer counts by state and year - for correct counts, we drop part
# C, but total the other counts 
cust_counts <- cust_counts %>%
  filter(part != 'C') %>%
  group_by(state_ab, year) %>%
  summarise(eia_state_total_cust = sum(as.numeric(customer_counts), 
                                       na.rm = TRUE))

# all the states we expect to be there are there 
# sum(state_fips_abbrev$state_ab %in%
#       unique(cust_counts$state_ab))/length(unique(cust_counts$state_ab))

# filter to only contiguous US states and add state fips
cust_counts <- cust_counts %>%
  filter(state_ab %in% state_fips_abbrev$state_ab) 

cust_counts <- cust_counts %>%
  left_join(state_fips_abbrev) %>%
  select(state_ab, state_fips, state_name, year, eia_state_total_cust) %>%
  mutate(year = as.numeric(year))

# Write -------------------------------------------------------------------

write_rds(cust_counts,
          here(
            "data",
            "power_outage_medicare_data_cleaning_output",
            "eia_state_total_customers_by_year.RDS"
          ))

