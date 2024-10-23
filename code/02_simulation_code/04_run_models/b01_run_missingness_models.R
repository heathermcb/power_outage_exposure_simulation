# This script selects data to run difference in differences and 
# conditional poisson models. 


# Libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, data.table, gnm, here)

# Read --------------------------------------------------------------------

an_dat <- read_rds(here(
  "data",
  'simulated_hospitalization_outcome_data',
  'all_levels_missingness.RDS'
))

# create group ID to use for grouping models 
n <- seq(1:100)
k <- rep(n, 100)
m <- data.frame(counties = seq(1:10000), group_id = k)

# join county ids 
an_dat <- an_dat %>% left_join(m)

# want to select control days for DID analysis, and also create a stratum 
# variable for the conditional poisson. 
# this script should maybe just do the DID analysis. 

# Select control days -----------------------------------------------------
# for DID analysis 

# select all the days that are exposed. goal with DID is to select a control 
# day for each exposed day, and the control day will be in the same weeday and 
# in the same month as the exposed day, but in any county.
an_dat <- setDT(an_dat)

# Add a column to an_dat to indicate the weekday and month
an_dat[, `:=`(weekday = weekdays(day), month = month(day))]

# find exposed days 
days_exposed_8_hrs <- an_dat[exposed_8_hrs_0.005_none_missing == 1]
days_exposed_8_hrs[, case_control := 1]

# Count the number of exposed days for each weekday and month grouping
exposed_counts <- days_exposed_8_hrs[, .N, by = .(weekday, month, group_id)]

# Sample the required number of control days for each grouping
sampled_controls <- exposed_counts[, {
  control_days <- an_dat[weekday == .BY$weekday 
                         & month == .BY$month 
                         & group_id == .BY$group_id 
                         & exposed_8_hrs_0.005_none_missing == 0]
  if (nrow(control_days) > 0) {
    control_days <- control_days[sample(.N, min(.N, N))]
    # remove redundant columns
    control_days[, `:=`(weekday = NULL, month = NULL, group_id = NULL)]  
  }
  control_days
}, by = .(weekday, month, group_id)]

sampled_controls[, case_control := 0]

cases_and_controls <- 
  rbindlist(list(days_exposed_8_hrs, sampled_controls), use.names = T)

cases_and_controls <- split(cases_and_controls, by = 'group_id')
l <- colnames(cases_and_controls[[1]])[5:34]

# Model -------------------------------------------------------------------

# create flexible function to run models for DID analysis 
run_poisson_model <- function(data,
                              response_col,
                              predictor_col,
                              offset_col) {
  formula <- as.formula(paste(
    response_col,
    "~",
    predictor_col,
    "+ offset(log(",
    offset_col,
    "))"
  ))
  m <- glm(formula, data = data, family = poisson(link = 'log'))
  print('ran model')
  return(m)
}


run_models_for_group <- function(data, colnames) {
  print('running models for group')
  tryCatch({
    lapply(colnames, function(colname) {
      result <- run_poisson_model(data = data,
                                  predictor_col = 'exposed_8_hrs_0.005_none_missing',
                                  offset_col = 'customers_served_hourly',
                                  response_col = colname)
      print(paste('ran model for', colname))
      return(result)
    })
  }, error = function(e) {
    message("Skipping model due to error: ", e$message)
    return(NULL)
  })
}

# Run models for each group using lapply
results <- lapply(cases_and_controls, function(group_data) {
  run_models_for_group(group_data, l)
})


# results <- lapply(l, function(colname) {
#   run_poisson_model(data = cases_and_controls,
#                     predictor_col = 'exposed_8_hrs_0.005_none_missing',
#                     offset_col = 'customers_served_hourly',
#                     response_col = colname)
# })

extract_model_results <- function(model_list, colnames) {
  results_list <- lapply(seq_along(model_list), function(i) {
    model <- model_list[[i]]
    if (!is.null(model)) {
      coef <- coef(summary(model))
      conf_int <- confint(model)
      data.frame(
        outcome = colnames[i],
        estimate = coef[2, "Estimate"],
        lower_ci = conf_int[2, 1],
        upper_ci = conf_int[2, 2]
      )
    } else {
      data.frame(
        outcome = colnames[i],
        estimate = NA,
        lower_ci = NA,
        upper_ci = NA
      )
    }
  })
  do.call(rbind, results_list)
}

# Combine results from all groups
all_results <- lapply(results, function(group_results) {
  extract_model_results(group_results, l)
})

final_results <- do.call(rbind, all_results)

# Write -------------------------------------------------------------------

write_rds(
  final_results,
  here(
    'results',
    'simulation_model_output',
    'results_all_missingness_DID.RDS'
  )
)


