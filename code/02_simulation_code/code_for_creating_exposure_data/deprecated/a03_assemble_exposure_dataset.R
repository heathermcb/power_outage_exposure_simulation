# Makes 400 datasets with 25 simulated counties and a random number of
# simulated city-utilities, and populates simulated outage data.
# These will be aggregated into 100 datasets of 100 counties later. 

# Libraries ---------------------------------------------------------------

library(tidyverse)
library(here)
library(EnvStats)
library(lubridate)
library(padr)
library(data.table)


# Read --------------------------------------------------------------------

customers_served <-
  readRDS(
    here(
      "power_outage_medicare_data",
      "power_outage_simulation_created_data",
      "distribution_vectors",
      "customers_served_by_city_utility_year.RDS"
    )
  )

pods_by_county <- 
  readRDS(
    here(
      "power_outage_medicare_data",
      "power_outage_simulation_created_data",
      "distribution_vectors",
      "num_pods_by_county.RDS"
    )
  )

p_customers_out <- 
  readRDS(
    here(
      "power_outage_medicare_data",
      "power_outage_simulation_created_data",
      "distribution_vectors",
      "percent_customers_out_vector.RDS"
    )
  )

samples_length_of_outage <- read_rds(here(
  "power_outage_medicare_data",
  "power_outage_simulation_created_data",
  "distribution_vectors",
  "length_of_outages.RDS"
))


# Do ----------------------------------------------------------------------
create_one_simed_county <- function(){
# first make dataset backbone --------------------------------------------
# create counties
  dat <- data.table(counties = 1:100)
  
  # add number of pods by county
  dat <-
    dat[, .(list_of_pods = list(seq(1:remp(25, obs = pods_by_county)))),
        by = counties]
  # unlist them so there is one row per pod
  dat <-
    dat[, list(list_of_pods = as.character(unlist(list_of_pods))),
        by = list(counties)]
  
# add customers by pod
dat[, customers_by_pod := remp(nrow(.SD), obs = customers_served)]

# make a pod ID
dat[, pod_id := seq(1:nrow(.SD))]

# make ten-minute intervals for a year ------------------------------------

# get sequence of 10 minute intervals
second_seq <- seq(from = 0, to = 86400, by =  600)

# get dates for a year
k <-
  as.numeric(as.POSIXct(seq.Date(
    from = ymd("2019-01-01"),
    to = ymd('2019-12-31'),
    by = 'day'
  )))

# combine to get 10-minute intervals over a year to join to other stuff 
ten_min_intervals <- CJ(k, second_seq)

ten_min_intervals <-
  ten_min_intervals[, ten_min_seq := k + second_seq
  ][, ten_min_seq := as_datetime(ten_min_seq)
  ][, list(ten_min_seq)]

# this is when it gets a bit big bc we're giving a ten min sequence to every 
# pod for a year, and that is HUGE. 
new_dat <- CJ(dat$pod_id, ten_min_intervals$ten_min_seq)
new_dat <- new_dat[, .(pod_id = V1, ten_min_seq = V2)]

# add this so we have a dataframe with the ten minute sequences and also the 
# county, pod, pod_id, and customers in a pod
# join used to be here 

object.size(new_dat)

# add outages -------------------------------------------------------------

# find out about how many outage lengths we need to sample to have around 0.06%
# of our observations out; based on data investigation we did about the 
# distribution of outages in the data

# the label here is like is it the first outage, second, etc. 
# it indexes the outage number 
n1 <- floor(dim(new_dat)[[1]]*0.06 / mean(samples_length_of_outage))
outage_lengths <- remp(n1, samples_length_of_outage)
label <- 1:length(outage_lengths)

# table with outage number and outage length
outage_lengths <- data.table(label, outage_lengths)

# here we sample n1 dates to give a start date to each outage 
# this is what will distribute these among the pod_ids
dates <- sample_n(new_dat, size = n1)
# index them 
outage_dates <- data.table(dates, label)
# say what length they are 
outage_dates <- outage_dates[outage_lengths, on = 'label']

# give a length in mins
outage_dates[, outage_length_mins := dminutes(x = 10*outage_lengths)][
    , outage_end := ten_min_seq + outage_length_mins
  ]

# this makes them into start and end times
outage_dates <-
  melt(
    outage_dates,
    id.vars = c('pod_id', 'label'),
    measure.vars = c('ten_min_seq', 'outage_end'),
    value.name = 'datetimes'
  )

# this uses some memory - I wonder if there's a better way to do this. 
# maybe we could join it to the backbone 
# this is when we need to join? 
power_is_out <-
  outage_dates[, .(datetimes = seq(min(datetimes),
                                   max(datetimes), by = '10 mins')), 
               by = label]


outage_dates_test <-
  outage_dates[power_is_out, on = .(label, datetimes), roll = TRUE]

non_zero_p <- data.table(p_customers_out = p_customers_out)[p_customers_out > 0
][,p_customers_out]


outage_dates_test <- outage_dates_test[, .(pod_id, label, datetimes)]

outage_dates_test <-
  outage_dates_test[, customers_out := remp(nrow(.SD), obs = non_zero_p)]

# have to join here to not get an error 
new_dat <- dat[new_dat, on = 'pod_id']

setnames(new_dat, old = "ten_min_seq", new = "datetimes")

new_dat <-
  merge(new_dat,
        outage_dates_test,
        all.x = TRUE,
        by = c('datetimes', 'pod_id'))

new_dat <- new_dat[order(counties, list_of_pods, pod_id, datetimes)]

mult <- dat2$customers_out * dat2$customers_by_pod # should do inside data.table

dat2 <- dat2[,customers_out_counts := customers_out * customers_by_pod] 

dat2 <-
  dat2[, .SD, .SDcols = c("counties",
                          "pod_id",
                          "customers_by_pod",
                          'datetimes',
                          'customers_out_counts')]

dat2[is.na(dat2)] <- 0

dat2[, ('customers_out_counts') := round(.SD, 0),
     .SDcols = c('customers_out_counts')]


# Write -------------------------------------------------------------------

saveRDS(
  dat2,
  here(
    "power_outage_medicare_data",
    "power_outage_simulation_created_data",
    "exposure_datasets",
    paste0("100_counties_", i, ".RDS")
  )
)

}

  