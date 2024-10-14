# This script reads in the raw power outage data and cleans the state names and 
# county names, with the goal to assign as many accurate county FIPS codes to 
# places referred to by text state and county names in the POUS data file. 
# It also trims down the data to the contiguous US. It doesn't modify the data 
# in any other way. The county fips already in POUS are unreliable so we are 
# trying to replace them here. 

# Author: Heather
# Last updated: Oct 3rd, 2024

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here, janitor, tidytext, snakecase, readxl, 
               data.table, fst)

# Read --------------------------------------------------------------------

# load fips of states and counties we wish to include
county_list <- read_rds(here('data', 'cotus_county_list_of_fips.RDS'))

# raw data from POUS
raw_pous_read <-
  fread(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "POUS_Export_Raw_CityByUtility_20170101_20201231.csv"
    )
  )#, n_max = 10000)

# FIPS crosswalk
fips_xwalk <-
  read_csv(
    here(
      "data",
      "power_outage_medicare_data_cleaning_raw_data",
      "nhgis0009_ds244_20195_county.csv"
    )
  )

# manual name spelling correction file - I made this for remaining names that
# still were not corrected by my automated efforts, to get as many names matched
# as possible.
spell_correction_file <-
  read_xlsx(
    here(
      "data",
      "power_outage_medicare_data_cleaning_output",
      "missing_county_key.xlsx"
    )
  ) %>%
  select(clean_state_name = clean_state, clean_county_name, correct_county)

# state reassignment correction file - manual correction file for reassigning 
# counties that are misattributed to neighbouring states to the right states
state_reassignment <-
  read_csv(here("data",
                "power_outage_medicare_data_cleaning_output",
                "state_reassignment_fix.csv"))

# Tidy --------------------------------------------------------------------

# clean column names and also get clean names of states and counties in POUS
# remove punctuation and other threats to matching like stop words
# 'municipality', 'township', etc. - make names all lowercase w no spaces or 
# punctuation

raw_pous <- raw_pous_read %>%
  clean_names()

# clean state names 
clean_pous_state_names <-
  data.frame(state_name = c(unique(raw_pous$state_name))) %>%
  mutate(clean_state_name = str_replace_all(state_name, "[^[:alnum:]]", "")) %>%
  mutate(clean_state_name = str_replace_all(clean_state_name, "[:space:]", "")) %>%
  mutate(clean_state_name = tolower(clean_state_name))

# clean county names 
clean_pous_county_names <-
  data.frame(county_name = c(unique(raw_pous$county_name))) %>%
  mutate(clean_county_name = str_replace_all(county_name, "[^[:alnum:]]", "")) %>%
  mutate(clean_county_name = str_replace_all(clean_county_name, "[:space:]", "")) %>%
  mutate(clean_county_name = tolower(clean_county_name))

# add clean names to pous data - reattach
raw_pous <- raw_pous[clean_pous_state_names, on = "state_name"
                     ][clean_pous_county_names, on = "county_name"]


# # check that the state county combinations created are unique - don't want to
# be messing up the data accidentally
# s <- raw_pous %>%
#   select(state_name, county_name, clean_state_name, clean_county_name) %>%
#   distinct() %>%
#   group_by(clean_state_name, clean_county_name) %>%
#   mutate(n = n())

# clean column names and get clean names of states and counties in xwalk in the
# same way
fips_xwalk <-
  fips_xwalk %>%
  mutate(fips = paste0(substr(GISJOIN, 2, 3), substr(GISJOIN, 5, 7))) %>%
  select(state_name = STATE, county_name = COUNTY, fips)

xwalk_state_names <-
  data.frame(state_name = c(unique(fips_xwalk$state_name))) %>%
  mutate(clean_state = str_replace_all(state_name, "[^[:alnum:]]", "")) %>%
  mutate(clean_state = str_replace_all(clean_state, "[:space:]", "")) %>%
  mutate(clean_state = tolower(clean_state))

xwalk_county_names <-
  data.frame(county_name = c(unique(fips_xwalk$county_name))) %>%
  mutate(clean_county = str_replace(county_name, " [Cc]{1}ounty", "")) %>%
  mutate(clean_county = str_replace(clean_county, " [Pp]{1}arish", "")) %>%
  mutate(clean_county = str_replace(clean_county, " [Bb]{1}orough", "")) %>%
  mutate(clean_county = str_replace(clean_county, " Census Area", "")) %>%
  mutate(clean_county = str_replace(clean_county, " [Mm]{1}unicipality", "")) %>%
  mutate(clean_county = str_replace_all(clean_county, "[^[:alnum:]]", ""))

xwalk_county_names <- xwalk_county_names  %>%
  mutate(clean_county = str_replace_all(clean_county, "[:space:]", "")) %>%
  mutate(clean_county = tolower(clean_county))

# change everything to data.table for fast join
xwalk_state_names <- as.data.table(xwalk_state_names)
xwalk_county_names <- as.data.table(xwalk_county_names)
fips_xwalk <- as.data.table(fips_xwalk)

# rejoin clean xwalk names
fips_xwalk <- fips_xwalk[xwalk_state_names, on = c("state_name")]
fips_xwalk <- fips_xwalk[xwalk_county_names, on = "county_name"]
fips_xwalk <- fips_xwalk[,.(clean_county, clean_state, fips)]

# correct colnames 
setnames(
  fips_xwalk,
  old = c("clean_county", "clean_state"),
  new = c("clean_county_name", "clean_state_name")
)

# # check that the state county combinations created are unique
# s <- fips_xwalk %>%
#   select(state_name, county_name, clean_state, clean_county) %>%
#   distinct() %>%
#   group_by(clean_state, clean_county) %>%
#   mutate(n = n())

# save which state-county combos appear in the FIPS file, so we can find out
# which combinations in the POUS file are not in here and need to be manually
# replaced 
state_county_combos <- fips_xwalk %>%
  mutate(state_county = paste0(clean_state_name, clean_county_name)) %>%
  select(state_county)

# County names ------------------------------------------------------------

# want to clean county names so they match our FIPS crosswalk file
# replace names spelled differently with good spelling
raw_pous <-
  raw_pous %>%
  mutate(state_county = paste0(clean_state_name, clean_county_name))

# use manual spell correction file
raw_pous <- raw_pous %>%
  left_join(spell_correction_file) %>%
  mutate(
    clean_county_name = case_when(
      state_county %in% state_county_combos$state_county ~ clean_county_name,
      (!is.na(correct_county)) ~ correct_county,
      TRUE ~ clean_county_name
    )
  ) %>%
  select(-c(correct_county, state_county))

# replace state-county combinations in the POUS file based on data errors with 
# the correct state and county names using manual reassignment file
raw_pous <- raw_pous %>%
  left_join(state_reassignment) %>%
  mutate(clean_state_name = case_when(
    !is.na(new_clean_state_name) ~ new_clean_state_name,
    TRUE ~ clean_state_name
  )) %>%
  select(-c(new_state_name, new_clean_state_name))

# add FIPS codes based on good spelling and state reassignment
raw_pous <-
  raw_pous %>%
  left_join(fips_xwalk) %>%
  mutate(state_county = paste0(clean_state_name, clean_county_name))

# filter using the county list down to contiguous US fips 
raw_pous <-
  raw_pous %>% 
  rename(five_digit_fips = fips) %>% 
  filter(five_digit_fips %in% county_list$five_digit_fips)

# select out relevant cols
raw_pous <-
  raw_pous %>% select(
    clean_state_name,
    clean_county_name,
    five_digit_fips,
    city_name,
    utility_name,
    recorded_date_time,
    customers_tracked,
    customers_out,
  ) %>% 
  ungroup() 

# Write -------------------------------------------------------------------

write_fst(
  raw_pous,
  here(
    "data",
    "power_outage_medicare_data_cleaning_output",
    "raw_with_fips.fst"
  )
)


