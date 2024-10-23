

pacman::p_load(here, tidyverse, data.table)

oo <- read_rds(
  here(
    'results',
    'simulation_model_output',
    'results_all_missingness_DID.RDS'
  )
)

glimpse(oo)

means <- oo %>% group_by(outcome) %>% summarize(mean_beta = mean(estimate),
                                                mean_lower_ci = mean(lower_ci),
                                                mean_upper_ci = mean(upper_ci))

bias <- oo %>% mutate(percent_bias = exp(estimate) - 1.05/1.05)

coverage <- oo %>% mutate(covered = ifelse(1.05 > exp(lower_ci) &
                                             1.05 < exp(upper_ci), 1, 0))
coverage


bias %>% ggplot() + geom_boxplot(aes(bias)) + facet_grid(rows = )