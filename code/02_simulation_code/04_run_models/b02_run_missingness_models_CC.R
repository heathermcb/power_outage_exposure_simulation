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

# just need to add stratum column which is county - but we already have.

cases_and_controls <- split(an_dat, by = 'group_id')

l <- colnames(cases_and_controls[[1]])[5:31]

# Model -------------------------------------------------------------------

# Flexible function to run conditional Poisson models
run_conditional_poisson_model <- function(data,
                                          response_col,
                                          predictor_col,
                                          offset_col,
                                          stratum_col) {
  # Create the formula
  formula <- as.formula(paste(response_col, 
                              "~", predictor_col, "+ offset(", offset_col, ")"))
  
  # Convert the stratum column to a factor
  stratum_factor <- factor(data[[stratum_col]])
  
  # Run the gnm model
  m <- gnm(formula,
           data = data,
           family = poisson,
           eliminate = stratum_factor)
  
  return(m)
}

run_models_for_group <- function(data, colnames) {
  print('running models for group')
  tryCatch({
    lapply(colnames, function(colname) {
      result <- run_conditional_poisson_model(data = data,
                                  predictor_col = 'exposed_8_hrs_0.005_none_missing',
                                  offset_col = 'customers_served_hourly',
                                  response_col = colname,
                                  stratum_col = 'counties')
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


extract_model_results <- function(model_list, colnames) {
  results_list <- lapply(seq_along(model_list), function(i) {
    model <- model_list[[i]]
    if (!is.null(model)) {
      tryCatch({
        coef <- coef(summary(model))
        conf_int <- confint(model)
        data.frame(
          rowname = paste0("model_", unique(model$data$group_id), "_", colnames[i]),
          group_id = unique(model$data$group_id),
          outcome = colnames[i],
          estimate = coef[2],
          lower_ci = as.numeric(conf_int[1]),
          upper_ci = as.numeric(conf_int[2])
        )
      }, error = function(e) {
        message("Skipping model due to error: ", e$message)
        data.frame(
          rowname = paste0("model_", unique(model$data$group_id), "_", colnames[i]),
          group_id = unique(model$data$group_id),
          outcome = colnames[i],
          estimate = NA,
          lower_ci = NA,
          upper_ci = NA
        )
      })
    } else {
      data.frame(
        rowname = paste0("model_", NA, "_", colnames[i]),
        group_id = NA,
        outcome = colnames[i],
        estimate = NA,
        lower_ci = NA,
        upper_ci = NA
      )
    }
  })
  do.call(rbind, results_list)
}

# check to see if this works
extract_model_results <- function(model_list, colnames) {
  results_list <- lapply(seq_along(model_list), function(i) {
    model <- model_list[[i]]
    if (!is.null(model)) {
      coef <- coef(summary(model))
      conf_int <- confint(model)
      data.frame(
        rowname = paste0("model_", unique(model$data$group_id), "_", colnames[i]),
        group_id = unique(model$data$group_id),
        outcome = colnames[i],
        estimate = coef[2],
        lower_ci = as.numeric(conf_int[1]),
        upper_ci = as.numeric(conf_int[2])
      )
    } else {
      data.frame(
        rowname = paste0("model_", NA, "_", colnames[i]),
        group_id = NA,
        outcome = colnames[i],
        estimate = NA,
        lower_ci = NA,
        upper_ci = NA
      )
    }
  })
  do.call(rbind, results_list)
}

extract_model_results <- function(model_list, colnames) {
  results_list <- lapply(seq_along(model_list), function(i) {
    model <- model_list[[i]]
    if (!is.null(model)) {
      coef <- coef(summary(model))
      data.frame(
        rowname = paste0("model_", unique(model$data$group_id), "_", colnames[i]),
        group_id = unique(model$data$group_id),
        outcome = colnames[i],
        estimate = coef[2],
        std_error = coef[2, "Std. Error"]
      )
    } else {
      data.frame(
        rowname = paste0("model_", NA, "_", colnames[i]),
        group_id = NA,
        outcome = colnames[i],
        estimate = NA,
        std_error = NA
      )
    }
  })
  do.call(rbind, results_list)
}

# combine results from all groups
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
    'results_all_missingness_CC.RDS'
  )
)
