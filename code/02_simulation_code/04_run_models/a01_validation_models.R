# This script will run models on the unbiased dataset to confirm that
# we can get back the right beta when we don't manipulate the data/
# introduce bias in any way. It will run both study designs. 

# Libraries ---------------------------------------------------------------

library(here)
library(tidyverse)
library(data.table)
library(gtools)
library(gnm)
library(parallel)


# Read --------------------------------------------------------------------
# First do DID  -----------------------------------------------------------

# all effect size, DID 
ls_1 <- list.files(
  here(
    "data",
    "simulated_hospitalization_outcome_data",
    "8_hrs_no_missingness_DID"
  ),
  pattern = "^outcome_data_8_hrs_no_missingness",
  full.names = T
)

# sort list of files into correct order
ls_1 <- mixedsort(ls_1)

# read in files 
l_1 <- lapply(FUN = readRDS, X = ls_1)
l_1 <- rbindlist(l_1)

# group counties into groups of 4 so that there will be 100 counties in each 
# model 
n <- seq(1:100)
k <- rep(n, 100)
m <- data.frame(unique_county_id = seq(1:10000), group_id = k)

# join county ids 
l_1 <- l_1 %>% left_join(m)

# split dataframe into list of counties so there are 100 counties in each group
l_1 <- l_1 |> group_by(group_id) |> group_split()


# Model -------------------------------------------------------------------

# create flexible function to run models 
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
  return(m)
}


# Loop --------------------------------------------------------------------
# run all models in parallel in a cluster 

# determine the number of cores
num_cores <- detectCores()

# create a cluster
cl <- makeCluster(num_cores) # don't want to overload memory 

# export any objects needed by the computation to the cluster
clusterExport(cl, ls()) # quite of lot of objects are needed

clusterEvalQ(cl, {library(tidyverse)
  library(here)
  library(data.table)
  library(lubridate)})


# use parLapply to apply function running models in parallel
result_1p <- parLapply(
  cl,
  X = l_1,
  fun = run_poisson_model,
  predictor_col = 'exposed_under_8_hr_def',
  response_col = 'hosp_count_1_p',
  offset_col = 'n'
)

# for all effect sizes 
result_5p <- parLapply(
  cl,
  X = l_1,
  fun = run_poisson_model,
  predictor_col = 'exposed_under_8_hr_def',
  response_col = 'hosp_count_5_p',
  offset_col = 'n'
)


result_0.5p <- parLapply(
  cl,
  X = l_1,
  fun = run_poisson_model,
  predictor_col = 'exposed_under_8_hr_def',
  response_col = 'hosp_count_0.5_p',
  offset_col = 'n'
)



# stop the cluster
stopCluster(cl)


# Get the results ---------------------------------------------------------
# tabulate results into readble RDS files 

results_df_1 <- data.frame(beta = numeric(),
                           lower = numeric(),
                           upper = numeric())
results_df_5 <- data.frame(beta = numeric(),
                           lower = numeric(),
                           upper = numeric())
results_df_05 <- data.frame(beta = numeric(),
                            lower = numeric(),
                            upper = numeric())

# effect size 1p
# loop over the list of model results
for (model in result_1p) {
  # Extract the beta coefficient
  beta <- coef(model)[2]  # Assuming the beta coefficient is the second element
  
  # Extract the confidence interval
  ci <- confint(model)[2, ]  # Assuming the CI for the beta coefficient is the second row
  
  # Add the beta coefficient and confidence interval to the data frame
  results_df_1 <- rbind(results_df_1, data.frame(beta = beta, lower = ci[1], upper = ci[2]))
}

rownames(results_df_1) <- NULL
colnames(results_df_1) <-
  c(
    "beta_validation_model_1p",
    "lower_CI_validation_model_1p",
    "upper_CI_validation_model_1p"
  )


# save results 
saveRDS(
  results_df_1,
  here(
    "power_outage_medicare",
    "simulation_model_output",
    "validation_models_beta_CIs_1p.RDS"
  )
)

# effect size 5p
# loop over the list of model results
for (model in result_5p) {
  # extract the beta coefficient
  beta <- coef(model)[2]  
  # extract the confidence interval
  ci <- confint(model)[2, ]  
  # add the beta coefficient and confidence interval to the data frame
  results_df_5 <- rbind(results_df_5, data.frame(
    beta = beta,
    lower = ci[1],
    upper = ci[2]
  ))
}

rownames(results_df_5) <- NULL
colnames(results_df_5) <-
  c(
    "beta_validation_model_5p",
    "lower_CI_validation_model_5p",
    "upper_CI_validation_model_5p"
  )

# save results 
saveRDS(
  results_df_5,
  here(
    "power_outage_medicare",
    "simulation_model_output",
    "validation_models_beta_CIs_5p.RDS"
  )
)

# effect size 0.5p
# loop over the list of model results
for (model in result_0.5p) {
  # extract the beta coefficient
  beta <- coef(model)[2]
  # Extract the confidence interval
  ci <- confint(model)[2, ]
  # add the beta coefficient and confidence interval to the data frame
  results_df_05 <- rbind(results_df_05, data.frame(
    beta = beta,
    lower = ci[1],
    upper = ci[2]
  ))
}

rownames(results_df_05) <- NULL
colnames(results_df_05) <-
  c(
    "beta_validation_model_05p",
    "lower_CI_validation_model_05p",
    "upper_CI_validation_model_05p"
  )

saveRDS(
  results_df_05,
  here(
    "power_outage_medicare",
    "simulation_model_output",
    "validation_models_beta_CIs_05p.RDS"
  )
)



# Do case crossover conditional poisson -----------------------------------
# also get data for CC setup ----------------------------------------------

# 0.5 CC
ls_2 <- list.files(
  here(
    "power_outage_medicare_data",
    "simulated_hospitalization_outcome_data_smaller",
    "8_hrs_no_missingness_CC_0.5"
  ),
  pattern = "^outcome_data_8_hrs_no_missingness",
  full.names = T
)

ls_2 <- mixedsort(ls_2)

l_2 <- lapply(FUN = readRDS, X = ls_2)

l_2 <- rbindlist(l_2)

# group counties into groups of 4 so that there will be 100 counties in each 
# model 
n <- seq(1:100)
k <- rep(n, 100)
m <- data.frame(unique_county_id = seq(1:10000), group_id = k)

l_2 <- l_2 %>% left_join(m)

l_2 <- l_2 |> group_by(group_id) |> group_split()


# 1, CC

ls_3 <- list.files(
  here(
    "power_outage_medicare_data",
    "simulated_hospitalization_outcome_data_smaller",
    "8_hrs_no_missingness_CC_1"
  ),
  pattern = "^outcome_data_8_hrs_no_missingness",
  full.names = T
)

ls_3 <- mixedsort(ls_3)

l_3 <- lapply(FUN = readRDS, X = ls_3)
l_3 <- rbindlist(l_3)

# group counties into groups of 4 so that there will be 100 counties in each 
# model 
n <- seq(1:100)
k <- rep(n, 100)
m <- data.frame(unique_county_id = seq(1:10000), group_id = k)

l_3 <- l_3 %>% left_join(m)

l_3 <- l_3 |> group_by(group_id) |> group_split()

# 5, CC

ls_4 <- list.files(
  here(
    "power_outage_medicare_data",
    "simulated_hospitalization_outcome_data_smaller",
    "8_hrs_no_missingness_CC_5"
  ),
  pattern = "^outcome_data_8_hrs_no_missingness",
  full.names = T
)


ls_4 <- mixedsort(ls_4)

l_4 <- lapply(FUN = readRDS, X = ls_4)
l_4 <- rbindlist(l_4)

# group counties into groups of 4 so that there will be 100 counties in each 
# model 
n <- seq(1:100)
k <- rep(n, 100)
m <- data.frame(unique_county_id = seq(1:10000), group_id = k)

l_4 <- l_4 %>% left_join(m)

l_4 <- l_4 |> group_by(group_id) |> group_split()

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


# Loop --------------------------------------------------------------------

# Load the parallel package
library(parallel)

# Determine the number of cores
num_cores <- detectCores()

# Create a cluster
cl <- makeCluster(num_cores) # don't want to overload memory 

# Export any objects needed by the computation to the cluster
clusterExport(cl, ls()) # quite of lot of objects are needed

clusterEvalQ(cl, {library(tidyverse)
  library(here)
  library(data.table)
  library(lubridate)
  library(gnm)})


# CC models 
cc_result_0.5p <- parLapply(
  cl,
  X = l_2,
  fun = run_conditional_poisson_model,
  predictor_col = 'exposed_under_8_hr_def',
  response_col = 'hosp_count_0.5_p',
  stratum_col = 'unique_county_id',
  offset_col = 'n'
)

cc_result_1p <- parLapply(
  cl,
  X = l_3,
  fun = run_conditional_poisson_model,
  predictor_col = 'exposed_under_8_hr_def',
  response_col = 'hosp_count_1_p',
  stratum_col = 'unique_county_id',
  offset_col = 'n'
)


cc_result_5p <- parLapply(
  cl,
  X = l_4,
  fun = run_conditional_poisson_model,
  predictor_col = 'exposed_under_8_hr_def',
  response_col = 'hosp_count_5_p',
  stratum_col = 'unique_county_id',
  offset_col = 'n'
)




# Stop the cluster
stopCluster(cl)

# get results 

results_cc_df_1 <- data.frame(beta = numeric(), lower = numeric(), upper = numeric())
results_cc_df_5 <- data.frame(beta = numeric(), lower = numeric(), upper = numeric())
results_cc_df_05 <- data.frame(beta = numeric(), lower = numeric(), upper = numeric())



# # effect size 0.5p case crossover
# # Loop over the list of model results
# for (model in cc_result_0.5p) {
#   # Extract the beta coefficient
#   beta <- model$coefficients[1]  # Assuming the beta coefficient is the second element
#   
#   # Extract the confidence interval
#   ci <- confint(model)  # Assuming the CI for the beta coefficient is the second row
#   
#   # Add the beta coefficient and confidence interval to the data frame
#   results_cc_df_05 <- rbind(results_cc_df_05, data.frame(beta = beta, lower = ci[1], upper = ci[2]))
# }

for (model in cc_result_0.5p) {
  # Use tryCatch to handle errors
  tryCatch({
    # Extract the beta coefficient
    beta <- model$coefficients[1]  # Assuming the beta coefficient is the first element
    
    # Extract the confidence interval
    ci <- confint(model)  # Assuming the CI for the beta coefficient is the first row
    
    # Add the beta coefficient and confidence interval to the data frame
    results_cc_df_05 <- rbind(results_cc_df_05, data.frame(beta = beta, lower = ci[1], upper = ci[2]))
  }, error = function(e) {
    message("Skipping model due to error: ", e$message)
  })
}


rownames(results_cc_df_05) <- NULL
colnames(results_cc_df_05) <-
  c(
    "beta_validation_model_05p_cc",
    "lower_CI_validation_model_05p_cc",
    "upper_CI_validation_model_05p_cc"
  )

# could also translate this into a rate?

saveRDS(
  results_cc_df_05,
  here(
    "power_outage_medicare",
    "simulation_model_output",
    "validation_models_case_crossover_beta_CIs_05p.RDS"
  )
)

# # effect size 1p case crossover
# # Loop over the list of model results
# for (model in cc_result_1p) {
#   # Extract the beta coefficient
#   beta <- model$coefficients[1]  # Assuming the beta coefficient is the second element
#   
#   # Extract the confidence interval
#   ci <- confint(model)  # Assuming the CI for the beta coefficient is the second row
#   
#   # Add the beta coefficient and confidence interval to the data frame
#   results_cc_df_1 <- rbind(results_cc_df_1, data.frame(beta = beta, lower = ci[1], upper = ci[2]))
# }

for (model in cc_result_1p) {
  # Use tryCatch to handle errors
  tryCatch({
    # Extract the beta coefficient
    beta <- model$coefficients[1]  # Assuming the beta coefficient is the first element
    
    # Extract the confidence interval
    ci <- confint(model)  # Assuming the CI for the beta coefficient is the first row
    
    # Add the beta coefficient and confidence interval to the data frame
    results_cc_df_1 <- rbind(results_cc_df_1, data.frame(beta = beta, lower = ci[1], upper = ci[2]))
  }, error = function(e) {
    message("Skipping model due to error: ", e$message)
  })
}


rownames(results_cc_df_1) <- NULL
colnames(results_cc_df_1) <-
  c(
    "beta_validation_model_1p_cc",
    "lower_CI_validation_model_1p_cc",
    "upper_CI_validation_model_1p_cc"
  )

# could also translate this into a rate?

saveRDS(
  results_cc_df_1,
  here(
    "power_outage_medicare",
    "simulation_model_output",
    "validation_models_case_crossover_beta_CIs_1p.RDS"
  )
)


# effect size 5p case crossover
# Loop over the list of model results


for (model in cc_result_5p) {
  # Use tryCatch to handle errors
  tryCatch({
    # Extract the beta coefficient
    beta <- model$coefficients[1]  # Assuming the beta coefficient is the first element
    
    # Extract the confidence interval
    ci <- confint(model)  # Assuming the CI for the beta coefficient is the first row
    
    # Add the beta coefficient and confidence interval to the data frame
    results_cc_df_5 <- rbind(results_cc_df_5, data.frame(beta = beta, lower = ci[1], upper = ci[2]))
  }, error = function(e) {
    message("Skipping model due to error: ", e$message)
  })
}


# for (model in cc_result_5p) {
#   # Extract the beta coefficient
#   beta <- model$coefficients[1]  # Assuming the beta coefficient is the second element
#   
#   # Extract the confidence interval
#   ci <- confint(model)  # Assuming the CI for the beta coefficient is the second row
#   
#   # Add the beta coefficient and confidence interval to the data frame
#   results_cc_df_5 <- rbind(results_cc_df_5, data.frame(beta = beta, lower = ci[1], upper = ci[2]))
# }

rownames(results_cc_df_5) <- NULL
colnames(results_cc_df_5) <-
  c(
    "beta_validation_model_5p_cc",
    "lower_CI_validation_model_5p_cc",
    "upper_CI_validation_model_5p_cc"
  )

# could also translate this into a rate?

saveRDS(
  results_cc_df_5,
  here(
    "power_outage_medicare",
    "simulation_model_output",
    "validation_models_case_crossover_beta_CIs_5p.RDS"
  )
)


