# Create hospitalization data

# Libraries ---------------------------------------------------------------

pacman::p_load(here, tidyverse, data.table, gtools, arrow)

# Constants ---------------------------------------------------------------

baseline_hosp_rate <- 0.001 # putting this at .1% of people per day
# is similar to real medicare hospitalization rate

# Read --------------------------------------------------------------------

exp_data <- readRDS(here(
  'data',
  'power_outage_simulation_cleaned_data',
  'days_exposed_unexposed_all_durations.RDS'
)) |> 
  mutate(counties = as.numeric(counties))

denom <- list.files(
  here(
    "data",
    "power_outage_simulation_cleaned_data",
    "hourly_county_data"
  ),
  full.names = T
)

# want to write a helper that takes in an exposure column and an affect size,
# and an output colname and creates 

add_outcome_column <- function(data,
                               effect_size,
                               exposure_col,
                               output_col_name,
                               effect_size_name) {
  # calculate lambdas - baseline rate on days unexposed, and baseline rate plus
  # effect size on exposed days
  lambda <- baseline_hosp_rate + effect_size * data[[exposure_col]]
  
  # multiply lamba by the customer population size of each county
  lambda <- lambda * data$customers_served_hourly
  
  # generate hospitalization rate using Poisson distribution
  rate <- rpois(length(lambda), lambda)
  
  # create the dynamic outcome column name
  dynamic_output_col_name <- paste0(output_col_name, '_', effect_size_name)
  
  # add the outcome column to the data under the dynamic name
  data[[dynamic_output_col_name]] <- rate
  
  return(data)
}


# Add customer served estimates -------------------------------------------

denom <- rbindlist(lapply(FUN = read_parquet, X = denom))

denom <- denom %>%
  filter(!is.na(counties)) %>%
  select(counties, customers_served_hourly) %>%
  distinct()


exp_data <- exp_data %>% left_join(denom)

# Add outcome generated based on no missing and also all cols of missingness 
# grid --------------------------------------------------------------------

# exposure cols to use 
to_generate_outcome_on <- 
  c(colnames(exp_data)[3:5])

# outcome col names
names_of_outcome <- paste0('outcome_', to_generate_outcome_on)

# these are the rate changes with outages, for different effect sizes
effects_of_outage <- c(0.0000005, 0.00001, 0.000005)
effect_names <- c('0.5p', '1p', '5p')

for (i in seq_along(to_generate_outcome_on)) {
  for (j in seq_along(effects_of_outage)) {
    exposure_col <- to_generate_outcome_on[i]
    output_col_name <- names_of_outcome[i]
    effect_size_name <- effect_names[[j]]
    effect_size <- effects_of_outage[[j]]
    
    exp_data <- add_outcome_column(
      exp_data,
      effect_size = effect_size,
      exposure_col = exposure_col,
      output_col_name = output_col_name,
      effect_size_name = effect_size_name
    )
  }
}

outcome_data <- exp_data %>%
  select(
    counties,
    day,
    customers_served_hourly,
    exposed_under_8_hr_def,
    outcome_exposed_under_8_hr_def_0.5p:outcome_exposed_under_12_hr_def_5p
  )

write_rds(outcome_data,
          here(
            "data",
            'simulated_hospitalization_outcome_data',
            'all_durations.RDS'
          )
)



