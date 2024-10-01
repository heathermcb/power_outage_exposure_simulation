# Identify power outage events, defining power outage based on percentage
# of customers out. Want the 50th percentile of customers out.


# Libraries ---------------------------------------------------------------

library(tidyverse)
library(here)
library(lubridate)
library(data.table)

# Constants ---------------------------------------------------------------

outage_duration = hours(8)

# Read --------------------------------------------------------------------

# counties <-
#   list.files(here("data_proc", "data", "hourly_county"),
#              full.names = TRUE)
# counties <- lapply(counties, read_rds)
# counties <- lapply(counties, ungroup)

counties <-
  fread(here("power_outage_medicare_data",
                "power_outage_medicare_data_cleaning_output", 
                "hourly_data_with_coverage_exclusions.csv"))

counties <- counties %>%
  select(
    clean_state_name,
    clean_county_name,
    fips,
    year,
    hour,
    customers_out_hourly = customers_out_hourly_locf,
    customers_served_total = customers_served_estimate_to_use
  )

counties <-
  counties %>%
  group_by(clean_state_name, clean_county_name) %>%
  group_split()

counties <- as.list(counties)


# ID outages --------------------------------------------------------------

# we have hourly_county_df
for (i in 1:length(counties)) {
  hourly_county_df <- counties[[i]]
  hourly_county_df <- hourly_county_df %>%
    mutate(cutoff = customers_served_total * 0.01) # going to vary this and the duration
  
  # TBD: a quantile-based outage cutoff from simulations
  # cutoff <- hourly_county_df %>% 
  #   group_by(clean_state_name, clean_county_name, year) %>%
  #   summarise(cutoff = quantile(percent_out, 0.90))
  # 
  # hourly_county_df <- hourly_county_df %>% left_join(cutoff)
  
  # cutoff <-
  #   quantile(hourly_county_df$customers_out_total, 0.05, na.rm = TRUE)
  
  hourly_county_df <- hourly_county_df %>%
    mutate(po_on = case_when(customers_out_hourly > cutoff ~ 1,
                             TRUE ~ 0))
  
  hourly_county_df <- hourly_county_df %>%
    mutate(po_id = case_when((po_on == 1) & (lag(po_on) == 0) ~ 1,
                             TRUE ~ 0)) %>%
    mutate(po_id = case_when(po_on == 1 ~ cumsum(po_id),
                             TRUE ~ 0))
  counties[[i]] <- hourly_county_df
}


# Calculate outage duration -----------------------------------------------

# Helpers -----------------------------------------------------------------

# want to filter out outages shorter than the desired duration, so need
# to find actual outage durations and compare to the desired duration
find_po_durations <- function(hourly_county_df, outage_duration) {
  po_durations <- hourly_county_df %>%
    filter(po_id != 0) %>%
    group_by(clean_state_name, clean_county_name, po_id) %>%
    # get first and last records of customers out in this outage
    slice(c(1, n())) %>%
    # calculate duration by subtracting first and last time stamps
    mutate(duration = difftime(lead(hour), hour, units = 'hours')) %>%
    # keep only start time rows, renaming timestamp to start time
    filter(row_number() == 1) %>%
    rename(start_time = hour) %>%
    # keep only outages that are longer than desired duration
    filter(duration >= outage_duration) %>%
    #filter(duration < weeks(2)) %>%
    select(clean_state_name,
           clean_county_name,
           po_id,
           start_time,
           duration) %>%
    ungroup()
  return(po_durations)
}

# mark days on which a power outage starts, is going, or ends
# note that this does not include days that are not in the dataset
# it still correctly indicates the known start and end days, though, and
# just adds multiple indicators if more than one outage exists on one day
find_start_and_end_days <-
  function(county_dataframe, po_durations) {
    # gives days on which there's a duration or longer power outage
    po_st_nd <- county_dataframe %>%
      filter(po_id %in% po_durations$po_id) %>%
      mutate(day = date(hour)) %>%
      select(clean_state_name, clean_county_name, day, po_id) %>%
      distinct()
    # fill in days in between
    if (dim(po_st_nd)[[1]] > 0) {
      po_st_nd <- po_st_nd %>%
        group_by(po_id) %>%
        padr::pad(interval = "day")
    }
    # marks day numbers of each power outage longer than duration
    po_st_nd <- po_st_nd %>%
      group_by(clean_state_name, clean_county_name, po_id) %>%
      mutate(day_number = row_number()) %>%
      mutate(n_days = n()) %>%
      mutate(day_counter = case_when(day_number == 1 ~ 1,
                                     day_number > 1 &
                                       day_number < n_days ~ 2,
                                     TRUE ~ 3)) %>%
      select(day, po_id, day_counter) %>%
      ungroup()
    return(po_st_nd)
  }

# want outage start dates and durations in a dataframe to calculate exposed days
# using a fully expanded dataframe with all dates in the study period
durations_and_dates <-
  function(county_dataframe, po_st_nd, po_durations) {
    durations_and_dates <- county_dataframe %>%
      mutate(day = floor_date(hour, unit = 'day')) %>%
      select(clean_state_name, clean_county_name, day, po_id) %>%
      distinct() %>%
      group_by(po_id) %>%
      padr::pad(interval = 'day') %>%
      ungroup() %>%
      tidyr::fill(clean_state_name, clean_county_name)
    durations_and_dates <- durations_and_dates %>%
      left_join(po_durations) %>%
      left_join(po_st_nd)
    return(durations_and_dates)
  }


# on days where the outage is starting, check to see if it starts before 4
# on days where it's going or ends, mark those days as exposed
find_exposed_days <-
  function(durations_and_dates, outage_duration) {
    exposed_days <- durations_and_dates %>%
      mutate(cut_time = as_datetime(day) + days(1) - outage_duration) %>%
      mutate(start_before_cut = start_time < cut_time) %>% 
      # line below is tricky - to get correct cut point, need to add a day to get 12
      # midnight the next day, and then subtract the desired duration
      mutate(exposed = case_when(((day_counter == 1) &
                                    start_before_cut) ~ 1,
                                 day_counter == 2 ~ 1,
                                 day_counter == 3 ~ 1,
                                 TRUE ~ 0
      )) %>% 
      select(day, exposed) %>%
      group_by(day) %>%
      summarise(exposed = max(exposed))
    return(exposed_days)
  }

# make exposure frame
frame <- function(hourly_county_df) {
  frame <- hourly_county_df %>%
    mutate(day = floor_date(hour, unit = 'day')) %>%
    select(clean_state_name, clean_county_name, day) %>%
    distinct()
  return(frame)
}


# return exposed days in a certain county
create_analytic_data <-
  function(county_dataframe, outage_duration) {
    po_durations <-
      find_po_durations(hourly_county_df = county_dataframe,
                        outage_duration = outage_duration)
    start_and_end_days <-
      find_start_and_end_days(county_dataframe = county_dataframe,
                              po_durations = po_durations)
    durations_and_dates <-
      durations_and_dates(
        county_dataframe = county_dataframe,
        po_st_nd = start_and_end_days,
        po_durations = po_durations
      )
    exposed_days <-
      find_exposed_days(durations_and_dates = durations_and_dates,
                        outage_duration = outage_duration)
    
    frame <- frame(hourly_county_df = county_dataframe)
    
    all_data <- frame %>% left_join(exposed_days)
    
    return(all_data)
  }



# Count 8+ outages and days with 8+ hour outages -------------------------

num_outages <- c(0)
state_county <- c('testcase')

all_an_dat <- data.frame()
num_outages_frame <- data.frame(state_county, num_outages)
for (county in counties) {
  an_dat <-
    create_analytic_data(county_dataframe = county,
                         outage_duration = outage_duration)
  num_outages <-
    dim(find_po_durations(county, outage_duration = outage_duration))[1]
  state_county <-
    paste0(unique(county$clean_state_name),
           unique(county$clean_county_name))
  num_out_row <- data.frame(state_county, num_outages)
  all_an_dat <- bind_rows(all_an_dat, an_dat)
  num_outages_frame <- bind_rows(num_outages_frame, num_out_row)
  print(unique(county$clean_state_name))
  print(unique(county$clean_county_name))
}

num_outages_frame <- num_outages_frame %>% filter(state_county != 'testcase')

all_an_dat <- all_an_dat %>% rename(exposed_8_hrs_1_p = exposed)

# Write -------------------------------------------------------------------

saveRDS(
  all_an_dat,
  here(
    "power_outage_medicare_data",
    "power_outage_medicare_data_cleaning_output",
    "days_exposed_unexposed_1p_8_hrs.RDS"
  )
)
saveRDS(
  num_outages_frame,
  here(
    "power_outage_medicare_data",
    "power_outage_medicare_data_cleaning_output",
    "num_outages_g_8_hrs.RDS"
  )
)





