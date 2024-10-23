


pacman::p_load(here, tidyverse, data.table)

oo <- read_rds(here(
  'results',
  'simulation_model_output',
  'results_all_missingness_DID.RDS'
))

glimpse(oo)

means <- oo %>% group_by(outcome) %>% summarize(
  mean_beta = mean(estimate),
  mean_lower_ci = mean(lower_ci),
  mean_upper_ci = mean(upper_ci)
)

bias <- oo
setDT(bias)

bias[, `:=`(
  percent_affected = sub(".*_(\\d+\\.?\\d*)p_.*", "\\1", outcome),
  percent_missing = sub(".*_missing_(\\d+)_.*", "\\1", outcome),
  effect_size = sub(".*_(\\d+\\.?\\d*p)$", "\\1", outcome)
)]

bias[, percent_affected := ifelse(
  percent_affected == 'outcome_exposed_8_hrs_0.005_none_missing_5p' |
    percent_affected == 'outcome_exposed_8_hrs_0.005_none_missing_0.5p' |
    percent_affected == 'outcome_exposed_8_hrs_0.005_none_missing_1p',
  0,
  percent_affected
)]

bias[, percent_missing := ifelse(
  percent_missing == 'outcome_exposed_8_hrs_0.005_none_missing_5p' |
    percent_missing == 'outcome_exposed_8_hrs_0.005_none_missing_0.5p' |
    percent_missing == 'outcome_exposed_8_hrs_0.005_none_missing_1p',
  0,
  percent_missing
)]

bias[, effect := case_when(effect_size == '5p' ~ 0.05,
                           effect_size == '1p' ~ 0.01,
                           effect_size == '0.5p' ~ 0.005)]

bias[, exp_estimate := exp(estimate)]

bias <- bias %>% mutate(percent_bias = (estimate - effect) / effect)





# Assuming bias is a data frame with columns: percent_bias, effect_size, percent_affected, percent_missing
as.data.frame(bias) %>% 
  filter(percent_affected != 0) %>%
  filter(percent_missing != 0) %>% 
  ggplot() + 
  geom_boxplot(aes(y = percent_bias, color = effect_size)) + 
  facet_grid(rows = vars(percent_affected), cols = vars(percent_missing))

as.data.frame(bias) %>% 
  filter(percent_affected == 0) %>%
  filter(percent_missing == 0) %>% 
  ggplot() + 
  geom_boxplot(aes(y = percent_bias, color = effect_size))


