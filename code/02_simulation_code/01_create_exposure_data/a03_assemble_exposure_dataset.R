# Makes 400 datasets with 25 simulated counties and a random number of
# simulated city-utilities, and populates simulated outage data.
# These will be aggregated into 100 datasets of 100 counties later. 

# Author: Heather
# Last updated: sometime in 2021 lol

# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here, EnvStats, lubridate, padr, data.table)

# Read --------------------------------------------------------------------

# this is a vector containing all the counts of customers served in each city
# utility from the real data 
customers_served <-
  readRDS(
    here(
      "data",
      "power_outage_simulation_created_data",
      "distribution_vectors",
      "customers_served_by_city_utility_year.RDS"
    )
  )

# this is a vector containing all the counts of the number of pods from the real 
# data 
pods_by_county <- 
  readRDS(
    here(
      "data",
      "power_outage_simulation_created_data",
      "distribution_vectors",
      "num_pods_by_county.RDS"
    )
  )

# this is the distribution of the percentage of customers out
p_customers_out <- 
  readRDS(
    here(
      "data",
      "power_outage_simulation_created_data",
      "distribution_vectors",
      "percent_customers_out_vector.RDS"
    )
  )

# gets a distributional vector to sample from - eliminate 0 entries
non_zero_p <-
  data.table(p_customers_out = p_customers_out)[p_customers_out > 0][
    , p_customers_out]

# empirical lengths of outage
samples_length_of_outage <- read_rds(
  here(
    "data",
    "power_outage_simulation_created_data",
    "distribution_vectors",
    "length_of_outages.RDS"
  )
) %>%
  filter(!is.na(duration))

samples_length_of_outage <- samples_length_of_outage$duration


# Same for every iteration ------------------------------------------------
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


# Function to create simed counties ---------------------------------------

create_one_simed_county <- function(i){
  
  # first make dataset backbone --------------------------------------------
  # create counties
  dat <- data.table(counties = 1:25)
  
  # add number of pods by county
  dat <-
    dat[, .(list_of_pods = list(seq(1:remp(1, obs = pods_by_county)))),
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
  
  county_specific_backbone <-
    CJ(dat$pod_id, ten_min_intervals$ten_min_seq, sorted = F)
  
  setnames(county_specific_backbone, "V1", "pod_id")
  setnames(county_specific_backbone, "V2", "ten_min_seq")
  
  # add outages -------------------------------------------------------------
  
  # need to sample this many outage lengths we need to sample to have around 
  # 1% of our observations out; based on data investigation we did about the 
  # distribution of outages in the data

  n1 <-
    floor(dim(county_specific_backbone)[[1]] * 0.02 / 
            mean(samples_length_of_outage)) 
  # mean outage length is 36 mins
  # n1 gives us the number of outages to average 6% of time out
  # this is actual outage lengths according to empirical dist
  outage_lengths <- remp(n1, samples_length_of_outage) 
  # give the outage lengths a label
  label <- 1:length(outage_lengths)
  # give them a date
  dates <- sample_n(county_specific_backbone, size = n1)
  # make that all a table
  outages <- data.table(label, outage_lengths, dates)
 
  # give a length in mins
  outages[, outage_length_mins := dminutes(x = 10 * outage_lengths)][
    , outage_end := ten_min_seq + outage_length_mins
  ]
  
  # this makes them into start and end times
  outages <-
    melt(
      outages,
      id.vars = c('pod_id', 'label'),
      measure.vars = c('ten_min_seq', 'outage_end'),
      value.name = 'ten_min_times'
    )
  
  # this uses some memory - I wonder if there's a better way to do this. 
  # this creates a frame that creates a row for every ten min period
  # maybe we could join it to the backbone 
  # this is when we need to join? 
  setkey(outages, label)
  power_is_out <-
    outages[, .(ten_min_times = seq(min(ten_min_times),
                                     max(ten_min_times), by = '10 mins')), 
                 by = label]
  
  # this joins it back 
  # Set keys on outages
  setkey(outages, label, ten_min_times)
  setkey(power_is_out, label, ten_min_times)
  outages <-
    outages[power_is_out, on = .(label, ten_min_times), roll = TRUE]

  # here we're assigning a number of customers out to each outage 
  outages <- outages[, .(pod_id, label, ten_min_times)]
  
  outages[, customers_out := remp(nrow(.SD), obs = non_zero_p)]
  
  outages <- unique(outages, by = c('pod_id', 'ten_min_times'))
  
  # here we're joining the outages back to the data backbone - should set 
  # keys to make this faster 
  setkey(county_specific_backbone, pod_id)
  setkey(dat, pod_id)
  county_specific_backbone <- dat[county_specific_backbone]
  
  setnames(county_specific_backbone, old = "ten_min_seq", 
           new = "ten_min_times")
  setkey(county_specific_backbone, ten_min_times, pod_id)
  setkey(outages, ten_min_times, pod_id)
  
  county_specific_backbone <- outages[county_specific_backbone]
  
  setorder(county_specific_backbone,
           counties,
           list_of_pods,
           pod_id,
           ten_min_times)
  
  # create counts rather than proportions
  county_specific_backbone[, customers_out_counts 
                           := customers_out * customers_by_pod] 
  
  # this is selecting cols 
    county_specific_backbone <-
      county_specific_backbone[, .(counties,
                                   pod_id,
                                   customers_by_pod,
                                   ten_min_times,
                                   customers_out_counts)]

  county_specific_backbone[is.na(county_specific_backbone)] <- 0
  
  county_specific_backbone[, ('customers_out_counts') := round(.SD, 0),
       .SDcols = c('customers_out_counts')]
  
  # Write -------------------------------------------------------------------
  
  saveRDS(
    county_specific_backbone,
    here(
      "data",
      "power_outage_simulation_created_data",
      paste0("exposure_datasets"),
      paste0("400_counties_", i, ".RDS")
    )
  )
  
}

# Do ----------------------------------------------------------------------

# Load the parallel package
library(parallel)
is <- seq(1:400)

# Determine the number of cores
num_cores <- detectCores()

# Create a cluster
cl <- makeCluster(4) # don't want to overload memory 

# Export any objects needed by the computation to the cluster
clusterExport(cl, ls()) # quite of lot of objects are needed

clusterEvalQ(cl, {library(tidyverse)
             library(here)
             library(EnvStats)
             library(lubridate)
             library(padr)
             library(data.table)})

result <- parLapply(cl,
                    X = is,
                    fun = create_one_simed_county)

# Stop the cluster
stopCluster(cl)



