# This file contains all helper functions for identifying days exposed and
# unexposed to power outages.

# Author: Heather
# Last updated: Oct 2nd, 2024

pacman::p_load(sf, here, tidyverse)

# get exposure ------------------------------------------------------------
# wrapper for the whole process of identifying days exposed and unexposed
get_exposure_on_percentile <- function(counties, outage_duration) {
  counties <- id_power_outage_on_percentile(counties)
  po_durations <- get_po_durations(counties, outage_duration = outage_duration)
  start_end_dates <- get_start_and_end_dates(counties, po_durations)
  durations_and_dates <- get_durations_and_dates(
    counties = counties,
    po_durations = po_durations,
    start_end_dates = start_end_dates
  )
  exposed_days <- get_exposed_days(durations_and_dates, 
                                   outage_duration = outage_duration)
  exposure <- return_exposure_percentile(
    exposed_days = exposed_days,
    counties = counties,
    outage_duration = outage_duration
  )
  return(exposure)
}

# id power outage on ------------------------------------------------------
# this takes data with hours of counts of customers without power, and a cut
# point, and identifies which hours are power outages
id_power_outage_on_percentile <- function(counties) {
  #col_name <- paste0("po_on_", cut_point)
  counties[, cutoff := customers_served_total * cut_point, by = five_digit_fips]
  counties[, po_on := case_when(customers_out_hourly > cutoff ~ 1, TRUE ~ 0), 
           by = five_digit_fips]
  counties[, po_id := case_when((po_on == 1) &
                                  (lag(po_on) == 0) ~ 1, TRUE ~ 0), 
           by = five_digit_fips]
  counties[, po_id := case_when(po_on == 1 ~ cumsum(po_id), TRUE ~ 0), 
           by = five_digit_fips]
  # counties[, (col_name) := po_on]
  counties[, po_id_fips_combo := paste0(five_digit_fips, po_id)]
  return(counties)
}

# get po durations --------------------------------------------------------
# this calculates how long power outages last and returns a frame with the
# durations
get_po_durations <- function(counties, outage_duration) {
  po_durations <- counties[po_id != 0]
  po_durations <- po_durations[, .SD[c(1, .N)], by = c('clean_state_name',
                                                       'clean_county_name',
                                                       'five_digit_fips',
                                                       'po_id')]
  
  po_durations[, duration := difftime(lead(hour), hour, units = 'hours'), 
               by = c('clean_state_name',
                      'clean_county_name',
                      'five_digit_fips',
                      'po_id')]
  
  po_durations <- po_durations[, .SD[1], by = c('clean_state_name',
                                                'clean_county_name',
                                                'five_digit_fips',
                                                'po_id')]
  
  setnames(po_durations, old = "hour", new = "start_time")
  
  po_durations <- po_durations[duration >= outage_duration]
  
  po_durations <- po_durations[, list(clean_state_name,
                                      clean_county_name,
                                      five_digit_fips,
                                      po_id,
                                      start_time,
                                      duration)]
  
  po_durations[, po_id_fips_combo := paste0(five_digit_fips, po_id)]
}

# get start and end dates -------------------------------------------------
# this function finds the start and end dates of power outages identified by the
# pipeline so far
get_start_and_end_dates <- function(counties, po_durations) {
  po_st_nd_2 <- counties %>%
    filter(po_id_fips_combo %in% po_durations$po_id_fips_combo) %>%
    mutate(day = date(hour)) %>%
    select(
      clean_state_name,
      clean_county_name,
      five_digit_fips,
      day,
      po_id,
      po_id_fips_combo
    ) %>%
    distinct()
  # fill in days in between
  if (dim(po_st_nd_2)[[1]] > 0) {
    po_st_nd_2 <- po_st_nd_2 %>%
      group_by(po_id) %>%
      padr::pad(interval = "day")
  }
  # marks day numbers of each power outage longer than duration
  po_st_nd_2 <- po_st_nd_2 %>%
    group_by(clean_state_name,
             clean_county_name,
             five_digit_fips,
             po_id,
             po_id_fips_combo) %>%
    mutate(day_number = row_number()) %>%
    mutate(n_days = n()) %>%
    mutate(day_counter = case_when(day_number == 1 ~ 1, day_number > 1 &
                                     day_number < n_days ~ 2, TRUE ~ 3)) %>%
    select(day, po_id_fips_combo, day_counter) %>%
    ungroup()
  return(po_st_nd_2)
}

# get durations and dates -------------------------------------------------
# this function returns both the dates of power outages and the durations
# in one dataframe
get_durations_and_dates <- function(counties, po_durations, start_end_dates) {
  durations_and_dates <- counties %>%
    mutate(day = floor_date(hour, unit = 'day')) %>%
    select(clean_state_name,
           clean_county_name,
           day,
           five_digit_fips,
           po_id_fips_combo) %>%
    distinct() %>%
    group_by(po_id_fips_combo) %>%
    padr::pad(interval = 'day', break_above = 500000000) %>%
    ungroup() %>%
    tidyr::fill(clean_state_name, clean_county_name, five_digit_fips)
  
  durations_and_dates <- durations_and_dates %>%
    left_join(po_durations) %>%
    left_join(start_end_dates)
  
  return(durations_and_dates)
}

# get exposed days --------------------------------------------------------
# this function identifies exposed days based on the dates and durations of
# power outages
get_exposed_days <- function(durations_and_dates, outage_duration) {
  # this heavily depends on the days before and after, or does it??
  exposed_days <- durations_and_dates %>%
    mutate(cut_time = as_datetime(day) + days(1) - outage_duration) %>%
    mutate(start_before_cut = start_time < cut_time) %>%
    # line below is tricky - to get correct cut point, need to add a day to
    # get 12 midnight the next day, and then subtract the desired duration
    mutate(exposed = case_when(((day_counter == 1) &
                                  start_before_cut) ~ 1,
                               day_counter == 2 ~ 1,
                               day_counter == 3 ~ 1,
                               TRUE ~ 0
    )) %>%
    select(five_digit_fips, day, exposed) %>%
    group_by(five_digit_fips, day) %>%
    summarise(exposed = max(exposed))
  return(exposed_days)
}

# return exposure ---------------------------------------------------------
# this function returns a list of days and their exposure status
return_exposure_percentile <- function(exposed_days,
                            counties,
                            outage_duration) {
  # get name for exposure col based on parameters
  exposure_col <- paste0("exposed_",
                         stringr::str_sub(start = 1, end = 1, 
                                          paste0(outage_duration)),
                         '_hrs_percentile')
  frame <- counties %>%
    mutate(day = floor_date(hour, unit = 'day')) %>%
    select(clean_state_name, clean_county_name, five_digit_fips, day) %>%
    distinct()
  frame <- frame %>% left_join(exposed_days)
  frame <- frame %>% rename({{ exposure_col }} := exposed)
  return(frame)
}
