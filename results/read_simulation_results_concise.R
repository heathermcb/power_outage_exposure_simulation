# Read, tabulate, and plot simulation results 

# Libraries ---------------------------------------------------------------

library(tidyverse)
library(here)
library(data.table)
library(stringr)


# Calculate bias ----------------------------------------------------------

# Functions ---------------------------------------------------------------

extend_to_max_length <- function(df){
  while (nrow(df) < 100){
    df <- rbind(df, rep(NA, ncol(df)))
  }
  return(df)
}


# function to read and bind RDS files with coefficients and CIs 
read_and_bind_rds <- function(path, pattern = "\\.RDS$") {
  all_files <- list.files(path, full.names = TRUE, pattern = pattern) 
  all_files <- lapply(FUN = readRDS, all_files)
  af <- lapply(FUN = extend_to_max_length, all_files)
  results <- bind_cols(af)
}

# function to select and rename columns based on effect size and study design
# suffix indicates study design and effect size
select_and_rename <- function(data, suffix) {
  data %>%
    dplyr::select(
      beta_validation_model = paste0("beta_validation_model_", suffix),
      beta_4_hr_model = paste0("beta_4_hr_outcome_model_", suffix),
      beta_12_hr_model = paste0("beta_12_hr_outcome_model_", suffix),
      beta_10_p_missing = paste0("beta_10_p_missing_model_", suffix),
      beta_30_p_missing = paste0("beta_30_p_missing_model_", suffix),
      beta_50_p_missing = paste0("beta_50_p_missing_model_", suffix),
      beta_70_p_missing = paste0("beta_70_p_missing_model_", suffix)
    )
}

# function to calculate bias, select relevant columns, and add metadata
calculate_bias <- function(data, effect_size, study_design) {
  data %>%
    mutate(across(everything(), ~ ((.x - effect_size) / effect_size) * 100,
                  .names = "bias_{.col}")) %>%
    mutate(effect_size = as.character(effect_size),
           study_design = study_design)
}


# DO ----------------------------------------------------------------------

# read results
results <- 
  read_and_bind_rds(here("power_outage_medicare", "simulation_model_output"))

# apply functions to read and bind coefficients for different effect sizes
results_betas_DID_05 <- select_and_rename(results, "05p")
results_betas_DID_1 <- select_and_rename(results, "1p")
results_betas_DID_5 <- select_and_rename(results, "5p")
results_betas_CC_05 <- select_and_rename(results, "05p_cc")
results_betas_CC_1 <- select_and_rename(results, "1p_cc")
results_betas_CC_5 <- select_and_rename(results, "5p_cc")

# apply functions to calculate bias
results_bias_DID_05_p <- calculate_bias(results_betas_DID_05, 0.005, "DID")
results_bias_DID_1_p <- calculate_bias(results_betas_DID_1, 0.01, "DID")
results_bias_DID_5_p <- calculate_bias(results_betas_DID_5, 0.05, "DID")
results_bias_CC_05_p <- calculate_bias(results_betas_CC_05, 0.005, "CC")
results_bias_CC_1_p <- calculate_bias(results_betas_CC_1, 0.01, "CC")
results_bias_CC_5_p <- calculate_bias(results_betas_CC_5, 0.05, "CC")

# make results plots
# DID

pt <- bind_rows(results_bias_DID_05_p,
                results_bias_DID_1_p,
                results_bias_DID_5_p) %>%
  mutate(effect_size = as.factor(effect_size))

DID_bias <- pt


pt <- pt %>% pivot_longer(
  cols = c(
    'bias_beta_validation_model',
    "bias_beta_4_hr_model",
    "bias_beta_12_hr_model",
    "bias_beta_10_p_missing",
    "bias_beta_30_p_missing",
    "bias_beta_50_p_missing",
    "bias_beta_70_p_missing"
  )
)

pt <- pt %>% mutate(
  name = case_when(
    name == "bias_beta_validation_model" ~ 'No exposure misclassification',
    name == "bias_beta_4_hr_model" ~ "Exposure misclassification: 8 hr exposure instead of 4 hr",
    name == "bias_beta_12_hr_model" ~ "Exposure misclassification: 8 hr exposure instead of 12 hr",
    name == "bias_beta_10_p_missing" ~ "10% exposure data missing",
    name == "bias_beta_30_p_missing" ~ "30% exposure data missing",
    name == "bias_beta_50_p_missing" ~ '50% exposure data missing',
    name == "bias_beta_70_p_missing" ~ '70% exposure data missing'
  )
)


ordered_levels <- c(
  "No exposure misclassification",
  "Exposure misclassification: 8 hr exposure instead of 4 hr",
  "Exposure misclassification: 8 hr exposure instead of 12 hr",
  "10% exposure data missing",
  "30% exposure data missing",
  "50% exposure data missing",
  "70% exposure data missing"
)  # Replace with your actual levels in the order you want

pt$name <- factor(pt$name, levels = ordered_levels)

pt <- pt %>% mutate(
  effect_size = case_when(
    as.factor(effect_size) == 0.005 ~ ".5% increase",
    as.factor(effect_size) == 0.01 ~ "1% increase",
    as.factor(effect_size) == 0.05 ~ "5% increase"
  )
)


p <-
  ggplot(data = pt, aes(
    x = name,
    y = value,
    fill = as.factor(effect_size)
  )) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "black", size = 1) +
  geom_boxplot() +
  theme_minimal() +
  #theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust=1)) +
  theme(text = element_text(size = 27)) +
    scale_x_discrete(
      labels = c(
        "No exposure misclassification" = "No exposure\n misclassification",
        "Exposure misclassification: 8 hr exposure instead of 4 hr" = "Exposure misclassification:\n 8 hr exposure\n instead of 4 hr",
        "Exposure misclassification: 8 hr exposure instead of 12 hr" = "Exposure misclassification:\n 8 hr exposure\n instead of 12 hr",
        "10% exposure data missing" = "10% exposure\n data missing",
        "30% exposure data missing" = "30% exposure\n data missing",
        "50% exposure data missing" = "50% exposure\n data missing",
        "70% exposure data missing" = "70% exposure\n data missing"
      )
  ) +
  theme(legend.title = element_text(size = 22)) +
  xlab("") + ylab("Percent bias") +
  labs(fill = "Simulated increase in risk\nof hospitalization with 8+ hour\npower outage exposure") +
  theme(axis.title.y = element_text(size = 22)) 
  

ggsave(
  here("power_outage_medicare", "figures", "DID_bias_new.pdf"),
  plot = p,
  width = 35,
  height = 8,
  dpi = 300
)


# make results plots
# CC

pt2 <- bind_rows(results_bias_CC_05_p,
                results_bias_CC_1_p,
                results_bias_CC_5_p) %>%
  mutate(effect_size = as.factor(effect_size))

CC_bias <- pt2


pt2 <- pt2 %>% pivot_longer(
  cols = c(
    'bias_beta_validation_model',
    "bias_beta_4_hr_model",
    "bias_beta_12_hr_model",
    "bias_beta_10_p_missing",
    "bias_beta_30_p_missing",
    "bias_beta_50_p_missing",
    "bias_beta_70_p_missing"
  )
)

pt2 <- pt2 %>% mutate(
  name = case_when(
    name == "bias_beta_validation_model" ~ 'No exposure misclassification',
    name == "bias_beta_4_hr_model" ~ "Exposure misclassification: 8 hr exposure instead of 4 hr",
    name == "bias_beta_12_hr_model" ~ "Exposure misclassification: 8 hr exposure instead of 12 hr",
    name == "bias_beta_10_p_missing" ~ "10% exposure data missing",
    name == "bias_beta_30_p_missing" ~ "30% exposure data missing",
    name == "bias_beta_50_p_missing" ~ '50% exposure data missing',
    name == "bias_beta_70_p_missing" ~ '70% exposure data missing'
  )
)



ordered_levels <- c(
  "No exposure misclassification",
  "Exposure misclassification: 8 hr exposure instead of 4 hr",
  "Exposure misclassification: 8 hr exposure instead of 12 hr",
  "10% exposure data missing",
  "30% exposure data missing",
  "50% exposure data missing",
  "70% exposure data missing"
)  # Replace with your actual levels in the order you want

pt2$name <- factor(pt2$name, levels = ordered_levels)

pt2 <- pt2 %>% mutate(
  effect_size = case_when(
    as.factor(effect_size) == 0.005 ~ ".5% increase",
    as.factor(effect_size) == 0.01 ~ "1% increase",
    as.factor(effect_size) == 0.05 ~ "5% increase"
  )
)


p2 <-
  ggplot(data = pt2, aes(
    x = name,
    y = value,
    fill = as.factor(effect_size)
  )) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "black", size = 1) +
  geom_boxplot() +
  theme_minimal() +
  #theme(axis.text.x = element_text(angle = 45, vjust = 0.5, hjust=1)) +
  theme(text = element_text(size = 27)) +
  scale_x_discrete(
    labels = c(
      "No exposure misclassification" = "No exposure\n misclassification",
      "Exposure misclassification: 8 hr exposure instead of 4 hr" = "Exposure misclassification:\n 8 hr exposure\n instead of 4 hr",
      "Exposure misclassification: 8 hr exposure instead of 12 hr" = "Exposure misclassification:\n 8 hr exposure\n instead of 12 hr",
      "10% exposure data missing" = "10% exposure\n data missing",
      "30% exposure data missing" = "30% exposure\n data missing",
      "50% exposure data missing" = "50% exposure\n data missing",
      "70% exposure data missing" = "70% exposure\n data missing"
    )
  ) +
  xlab("") + ylab("Percent bias") +
  theme(legend.title = element_text(size = 22)) +
  labs(fill = "Simulated increase in risk\nof hospitalization with 8+ hour\npower outage exposure") +
  theme(axis.title.y = element_text(size = 22)) 

ggsave(
  here(
    "power_outage_medicare",
    "figures",
    "case_crossover_bias_new.pdf"
  ),
  plot = p2,
  width = 35,
  height = 8,
  dpi = 300
)


# Coverage plots ----------------------------------------------------------

# get all CIs
CI_cols <- grep("lower_CI|upper_CI", colnames(results), value = TRUE)
CIs <- results[, CI_cols]

# get effect specific CIs
effect_05_columns <- grep("model_05p", colnames(CIs), value = TRUE)
CIs_05p <- CIs[, effect_05_columns]

effect_1_columns <- grep("model_1p", colnames(CIs), value = TRUE)
CIs_1p <- CIs[, effect_1_columns]

effect_5_columns <- grep("model_5p", colnames(CIs), value = TRUE)
CIs_5p <- CIs[, effect_5_columns]

# calculate coverage within those data frames

# define a function to calculate coverage
calculate_coverage <- function(lower_CI, upper_CI, reference_value) {
  ifelse(lower_CI <= reference_value & upper_CI >= reference_value, 1, 0)
}

# apply it
coverage_05p <- CIs_05p %>%
  mutate(
    coverage_validation_DID = calculate_coverage(
      lower_CI_validation_model_05p,
      upper_CI_validation_model_05p,
      reference_value = 0.005
    ),
    coverage_validation_CC = calculate_coverage(
      lower_CI_validation_model_05p_cc,
      upper_CI_validation_model_05p_cc,
      reference_value = 0.005
    ),
    coverage_4_hr_DID = calculate_coverage(
      lower_CI_4_hr_outcome_model_05p,
      upper_CI_4_hr_outcome_model_05p,
      reference_value = 0.005
    ),
    coverage_4_hr_CC = calculate_coverage(
      lower_CI_4_hr_outcome_model_05p_cc,
      upper_CI_4_hr_outcome_model_05p_cc,
      reference_value = 0.005
    ),
    coverage_12_hr_DID = calculate_coverage(
      lower_CI_12_hr_outcome_model_05p,
      upper_CI_12_hr_outcome_model_05p,
      reference_value = 0.005
    ),
    coverage_12_hr_CC = calculate_coverage(
      lower_CI_12_hr_outcome_model_05p_cc,
      upper_CI_12_hr_outcome_model_05p_cc,
      reference_value = 0.005
    ),
    coverage_10_p_missing_DID = calculate_coverage(
      lower_CI_10_p_missing_model_05p,
      upper_CI_10_p_missing_model_05p,
      reference_value = 0.005
    ),
    coverage_10_p_missing_CC = calculate_coverage(
      lower_CI_10_p_missing_model_05p_cc,
      upper_CI_10_p_missing_model_05p_cc,
      reference_value = 0.005
    ),
    coverage_30_p_missing_DID = calculate_coverage(
      lower_CI_30_p_missing_model_05p,
      upper_CI_30_p_missing_model_05p,
      reference_value = 0.005
    ),
    coverage_30_p_missing_CC = calculate_coverage(
      lower_CI_30_p_missing_model_05p_cc,
      upper_CI_30_p_missing_model_05p_cc,
      reference_value = 0.005
    ),
    coverage_50_p_missing_DID = calculate_coverage(
      lower_CI_50_p_missing_model_05p,
      upper_CI_50_p_missing_model_05p,
      reference_value = 0.005
    ),
    coverage_50_p_missing_CC = calculate_coverage(
      lower_CI_50_p_missing_model_05p_cc,
      upper_CI_50_p_missing_model_05p_cc,
      reference_value = 0.005
    ),
    coverage_70_p_missing_DID = calculate_coverage(
      lower_CI_70_p_missing_model_05p,
      upper_CI_70_p_missing_model_05p,
      reference_value = 0.005
    ),
    coverage_70_p_missing_CC = calculate_coverage(
      lower_CI_70_p_missing_model_05p_cc,
      upper_CI_70_p_missing_model_05p_cc,
      reference_value = 0.005
    )
  )

coverage_1p <- CIs_1p %>%
  mutate(
    coverage_validation_DID = calculate_coverage(
      lower_CI_validation_model_1p,
      upper_CI_validation_model_1p,
      reference_value = 0.01
    ),
    coverage_validation_CC = calculate_coverage(
      lower_CI_validation_model_1p_cc,
      upper_CI_validation_model_1p_cc,
      reference_value = 0.01
    ),
    coverage_4_hr_DID = calculate_coverage(
      lower_CI_4_hr_outcome_model_1p,
      upper_CI_4_hr_outcome_model_1p,
      reference_value = 0.01
    ),
    coverage_4_hr_CC = calculate_coverage(
      lower_CI_4_hr_outcome_model_1p_cc,
      upper_CI_4_hr_outcome_model_1p_cc,
      reference_value = 0.01
    ),
    coverage_12_hr_DID = calculate_coverage(
      lower_CI_12_hr_outcome_model_1p,
      upper_CI_12_hr_outcome_model_1p,
      reference_value = 0.01
    ),
    coverage_12_hr_CC = calculate_coverage(
      lower_CI_12_hr_outcome_model_1p_cc,
      upper_CI_12_hr_outcome_model_1p_cc,
      reference_value = 0.01
    ),
    coverage_10_p_missing_DID = calculate_coverage(
      lower_CI_10_p_missing_model_1p,
      upper_CI_10_p_missing_model_1p,
      reference_value = 0.01
    ),
    coverage_10_p_missing_CC = calculate_coverage(
      lower_CI_10_p_missing_model_1p_cc,
      upper_CI_10_p_missing_model_1p_cc,
      reference_value = 0.01
    ),
    coverage_30_p_missing_DID = calculate_coverage(
      lower_CI_30_p_missing_model_1p,
      upper_CI_30_p_missing_model_1p,
      reference_value = 0.01
    ),
    coverage_30_p_missing_CC = calculate_coverage(
      lower_CI_30_p_missing_model_1p_cc,
      upper_CI_30_p_missing_model_1p_cc,
      reference_value = 0.01
    ),
    coverage_50_p_missing_DID = calculate_coverage(
      lower_CI_50_p_missing_model_1p,
      upper_CI_50_p_missing_model_1p,
      reference_value = 0.01
    ),
    coverage_50_p_missing_CC = calculate_coverage(
      lower_CI_50_p_missing_model_1p_cc,
      upper_CI_50_p_missing_model_1p_cc,
      reference_value = 0.01
    ),
    coverage_70_p_missing_DID = calculate_coverage(
      lower_CI_70_p_missing_model_1p,
      upper_CI_70_p_missing_model_1p,
      reference_value = 0.01
    ),
    coverage_70_p_missing_CC = calculate_coverage(
      lower_CI_70_p_missing_model_1p_cc,
      upper_CI_70_p_missing_model_1p_cc,
      reference_value = 0.01
    )
  )

coverage_5p <- CIs_5p %>%
  mutate(
    coverage_validation_DID = calculate_coverage(
      lower_CI_validation_model_5p,
      upper_CI_validation_model_5p,
      reference_value = 0.05
    ),
    coverage_validation_CC = calculate_coverage(
      lower_CI_validation_model_5p_cc,
      upper_CI_validation_model_5p_cc,
      reference_value = 0.05
    ),
    coverage_4_hr_DID = calculate_coverage(
      lower_CI_4_hr_outcome_model_5p,
      upper_CI_4_hr_outcome_model_5p,
      reference_value = 0.05
    ),
    coverage_4_hr_CC = calculate_coverage(
      lower_CI_4_hr_outcome_model_5p_cc,
      upper_CI_4_hr_outcome_model_5p_cc,
      reference_value = 0.05
    ),
    coverage_12_hr_DID = calculate_coverage(
      lower_CI_12_hr_outcome_model_5p,
      upper_CI_12_hr_outcome_model_5p,
      reference_value = 0.05
    ),
    coverage_12_hr_CC = calculate_coverage(
      lower_CI_12_hr_outcome_model_5p_cc,
      upper_CI_12_hr_outcome_model_5p_cc,
      reference_value = 0.05
    ),
    coverage_10_p_missing_DID = calculate_coverage(
      lower_CI_10_p_missing_model_5p,
      upper_CI_10_p_missing_model_5p,
      reference_value = 0.05
    ),
    coverage_10_p_missing_CC = calculate_coverage(
      lower_CI_10_p_missing_model_5p_cc,
      upper_CI_10_p_missing_model_5p_cc,
      reference_value = 0.05
    ),
    coverage_30_p_missing_DID = calculate_coverage(
      lower_CI_30_p_missing_model_5p,
      upper_CI_30_p_missing_model_5p,
      reference_value = 0.05
    ),
    coverage_30_p_missing_CC = calculate_coverage(
      lower_CI_30_p_missing_model_5p_cc,
      upper_CI_30_p_missing_model_5p_cc,
      reference_value = 0.05
    ),
    coverage_50_p_missing_DID = calculate_coverage(
      lower_CI_50_p_missing_model_5p,
      upper_CI_50_p_missing_model_5p,
      reference_value = 0.05
    ),
    coverage_50_p_missing_CC = calculate_coverage(
      lower_CI_50_p_missing_model_5p_cc,
      upper_CI_50_p_missing_model_5p_cc,
      reference_value = 0.05
    ),
    coverage_70_p_missing_DID = calculate_coverage(
      lower_CI_70_p_missing_model_5p,
      upper_CI_70_p_missing_model_5p,
      reference_value = 0.05
    ),
    coverage_70_p_missing_CC = calculate_coverage(
      lower_CI_70_p_missing_model_5p_cc,
      upper_CI_70_p_missing_model_5p_cc,
      reference_value = 0.05
    )
  )

coverage_05p <- coverage_05p %>%
  select(contains('coverage')) %>%
mutate(effect_size = '0.005')

coverage_1p <- coverage_1p %>%
  select(contains('coverage')) %>%
mutate(effect_size = '0.01')

coverage_5p <- coverage_5p %>%
  select(contains('coverage')) %>% 
  mutate(effect_size = '0.05')


all_cov <- bind_rows(coverage_5p, coverage_05p, coverage_1p)

all_cov <- all_cov %>%
  group_by(effect_size) %>%
  summarize(across(contains("coverage"), mean, na.rm = T), .groups = "drop")

all_cov <- all_cov %>% mutate(across(contains("coverage"), ~ . * 100))

DID_cov <- all_cov %>% select(contains("DID"), effect_size)
CC_cov <- all_cov %>% select(contains('CC'), effect_size)


# Make plots --------------------------------------------------------------

DID_cov <- DID_cov %>%
  pivot_longer(cols = coverage_validation_DID:coverage_70_p_missing_DID)

DID_cov <- DID_cov %>% mutate(
  name = case_when(
    name == "coverage_validation_DID" ~ 'No exposure misclassification',
    name == "coverage_4_hr_DID" ~ "Exposure misclassification: 8 hr exposure instead of 4 hr",
    name == "coverage_12_hr_DID" ~ "Exposure misclassification: 8 hr exposure instead of 12 hr",
    name == "coverage_10_p_missing_DID" ~ "10% exposure data missing",
    name == "coverage_30_p_missing_DID" ~ "30% exposure data missing",
    name == "coverage_50_p_missing_DID" ~ '50% exposure data missing',
    name == "coverage_70_p_missing_DID" ~ '70% exposure data missing'
  )
)

ordered_levels <- c(
  "No exposure misclassification",
  "Exposure misclassification: 8 hr exposure instead of 4 hr",
  "Exposure misclassification: 8 hr exposure instead of 12 hr",
  "10% exposure data missing",
  "30% exposure data missing",
  "50% exposure data missing",
  "70% exposure data missing"
)  # Replace with your actual levels in the order you want

DID_cov$name <- factor(DID_cov$name, levels = ordered_levels)

DID_cov <- DID_cov %>% mutate(
  effect_size = case_when(
    as.factor(effect_size) == 0.005 ~ ".5% increase",
    as.factor(effect_size) == 0.01 ~ "1% increase",
    as.factor(effect_size) == 0.05 ~ "5% increase"
  )
)

pt <- DID_cov %>% ggplot() +
  geom_hline(yintercept = 95, linetype = "dashed", color = "black", size = 0.3) +
  theme(text = element_text(size = 18)) +
  geom_point(shape = 23,
             size = 7,
             position = position_dodge(width = 0.75)) +
  aes(x = name, y = value, fill = effect_size) + theme_minimal() +
  xlab("") + ylab("Percent coverage") +
  scale_x_discrete(
    labels = c(
      "No exposure misclassification" = "No exposure\n misclassification",
      "Exposure misclassification: 8 hr exposure instead of 4 hr" = "Exposure misclassification:\n 8 hr exposure\n instead of 4 hr",
      "Exposure misclassification: 8 hr exposure instead of 12 hr" = "Exposure misclassification:\n 8 hr exposure\n instead of 12 hr",
      "10% exposure data missing" = "10% exposure\n data missing",
      "30% exposure data missing" = "30% exposure\n data missing",
      "50% exposure data missing" = "50% exposure\n data missing",
      "70% exposure data missing" = "70% exposure\n data missing"
    )
  ) +
theme(legend.title = element_text(size = 15)) +
  labs(fill = "Simulated increase in risk\nof hospitalization with 8+ hour\npower outage exposure") +
  scale_y_continuous(breaks = seq(floor(min(DID_cov$value)), 100, by = 10))+  # Custom y-axis ticks 
  theme(axis.title.y = element_text(size = 15)) +
  theme(axis.text.x = element_text(size = 15)) +
  theme(legend.text = element_text(size = 15))


ggsave(
  here("power_outage_medicare", "figures", "DID_coverage.pdf"),
  plot = pt,
  width = 23,
  height = 4,
  dpi = 300
)

# for CC scenario

CC_cov <- CC_cov %>%
  pivot_longer(cols = coverage_validation_CC:coverage_70_p_missing_CC)

CC_cov <- CC_cov %>% mutate(
  name = case_when(
    name == "coverage_validation_CC" ~ 'No exposure misclassification',
    name == "coverage_4_hr_CC" ~ "Exposure misclassification: 8 hr exposure instead of 4 hr",
    name == "coverage_12_hr_CC" ~ "Exposure misclassification: 8 hr exposure instead of 12 hr",
    name == "coverage_10_p_missing_CC" ~ "10% exposure data missing",
    name == "coverage_30_p_missing_CC" ~ "30% exposure data missing",
    name == "coverage_50_p_missing_CC" ~ '50% exposure data missing',
    name == "coverage_70_p_missing_CC" ~ '70% exposure data missing'
  )
)

ordered_levels <- c(
  "No exposure misclassification",
  "Exposure misclassification: 8 hr exposure instead of 4 hr",
  "Exposure misclassification: 8 hr exposure instead of 12 hr",
  "10% exposure data missing",
  "30% exposure data missing",
  "50% exposure data missing",
  "70% exposure data missing"
)   # Replace with your actual levels in the order you want

CC_cov$name <- factor(CC_cov$name, levels = ordered_levels)

CC_cov <- CC_cov %>% mutate(
  effect_size = case_when(
    as.factor(effect_size) == 0.005 ~ ".5% increase",
    as.factor(effect_size) == 0.01 ~ "1% increase",
    as.factor(effect_size) == 0.05 ~ "5% increase"
  )
)

pt2 <- CC_cov %>% ggplot() +
  geom_hline(yintercept = 95, linetype = "dashed", color = "black", size = 0.3) +
  theme(text = element_text(size = 18)) +
  geom_point(shape = 23,
             size = 4,
             position = position_dodge(width = 0.75)) +
  aes(x = name, y = value, fill = effect_size) + theme_minimal() +
  xlab("") + ylab("Percent coverage") +
  theme(text = element_text(size = 15)) +
  geom_point(shape = 23,
             size = 7,
             position = position_dodge(width = 0.75)) +
  aes(x = name, y = value, fill = effect_size) + theme_minimal() +
  xlab("") + ylab("Percent coverage") +
  scale_x_discrete(
    labels = c(
      "No exposure misclassification" = "No exposure\n misclassification",
      "Exposure misclassification: 8 hr exposure instead of 4 hr" = "Exposure misclassification:\n 8 hr exposure\n instead of 4 hr",
      "Exposure misclassification: 8 hr exposure instead of 12 hr" = "Exposure misclassification:\n 8 hr exposure\n instead of 12 hr",
      "10% exposure data missing" = "10% exposure\n data missing",
      "30% exposure data missing" = "30% exposure\n data missing",
      "50% exposure data missing" = "50% exposure\n data missing",
      "70% exposure data missing" = "70% exposure\n data missing"
    )
  )+
  theme(legend.title = element_text(size = 15)) +
  labs(fill = "Simulated increase in risk\nof hospitalization with 8+ hour\npower outage exposure") +
  scale_y_continuous(breaks = seq(floor(min(DID_cov$value)), 100, by = 10)) +
  theme(axis.title.y = element_text(size = 15)) +
  theme(axis.text.x = element_text(size = 15)) +
  theme(legend.text = element_text(size = 15))# Custom y-axis ticks

ggsave(
  here("power_outage_medicare", "figures", "CC_coverage.pdf"),
  plot = pt2,
  width = 23,
  height = 4,
  dpi = 300
)


# Tables ------------------------------------------------------------------

bias_DID <- DID_bias %>%
  select(bias_beta_validation_model:bias_beta_70_p_missing,
         effect_size) %>%
  group_by(effect_size) %>%
  summarize(val = mean(bias_beta_validation_model),
            mc_4h = mean(bias_beta_4_hr_model),
            mc_12h = mean(bias_beta_12_hr_model),
            missing_10_p = mean(bias_beta_10_p_missing),
            missing_30_p = mean(bias_beta_30_p_missing),
            missing_50_p = mean(bias_beta_50_p_missing),
            missing_70_p = mean(bias_beta_70_p_missing)) %>%
  mutate(across(val:missing_70_p, round))

cov_DID <- all_cov %>% select(
  coverage_validation_DID,
  coverage_4_hr_DID,
  coverage_12_hr_DID,
  coverage_10_p_missing_DID,
  coverage_30_p_missing_DID,
  coverage_50_p_missing_DID,
  coverage_70_p_missing_DID,
  effect_size) %>% 
  mutate(across(coverage_validation_DID:coverage_70_p_missing_DID, round))


DID_table <- bias_DID %>% left_join(cov_DID) 

# combine values for kable

DID_table <- DID_table %>% 
  mutate(`Validation models` = paste0(val, ' (', coverage_validation_DID, ')'),
         `Exposure misclassification: 8 hr exposure instead of 4 hr` = paste0(mc_4h, ' (', coverage_4_hr_DID, ')'),
         `Exposure misclassification: 8 hr exposure instead of 12 hr` = paste0(mc_12h, ' (', coverage_12_hr_DID, ')'),
         `10 percent missingness` = paste0(missing_10_p, ' (', coverage_10_p_missing_DID, ')'),
         `30 percent missingness` = paste0(missing_30_p, ' (', coverage_30_p_missing_DID, ')'),
         `50 percent missingness` = paste0(missing_50_p, ' (', coverage_50_p_missing_DID, ')'),
         `70 percent missingness` = paste0(missing_70_p, ' (', coverage_70_p_missing_DID, ')')) %>%
  select(`Effect size` = effect_size, "Validation models":"70 percent missingness")

DID_table %>% knitr::kable()



bias_CC <- CC_bias %>%
  select(bias_beta_validation_model:bias_beta_70_p_missing,
         effect_size) %>%
  group_by(effect_size) %>%
  summarize(val = mean(bias_beta_validation_model),
            mc_4h = mean(bias_beta_4_hr_model),
            mc_12h = mean(bias_beta_12_hr_model),
            missing_10_p = mean(bias_beta_10_p_missing),
            missing_30_p = mean(bias_beta_30_p_missing),
            missing_50_p = mean(bias_beta_50_p_missing),
            missing_70_p = mean(bias_beta_70_p_missing)) %>%
  mutate(across(val:missing_70_p, round))

cov_CC <- all_cov %>% select(
  coverage_validation_CC,
  coverage_4_hr_CC,
  coverage_12_hr_CC,
  coverage_10_p_missing_CC,
  coverage_30_p_missing_CC,
  coverage_50_p_missing_CC,
  coverage_70_p_missing_CC,
  effect_size) %>% 
  mutate(across(coverage_validation_CC:coverage_70_p_missing_CC, round))


CC_table <- bias_CC %>% left_join(cov_CC) 

# combine values for kable

CC_table <- CC_table %>% 
  mutate(`Validation models` = paste0(val, ' (', coverage_validation_CC, ')'),
         `Exposure misclassification: 8 hr exposure instead of 4 hr` = paste0(mc_4h, ' (', coverage_4_hr_CC, ')'),
         `Exposure misclassification: 8 hr exposure instead of 12 hr` = paste0(mc_12h, ' (', coverage_12_hr_CC, ')'),
         `10 percent missingness` = paste0(missing_10_p, ' (', coverage_10_p_missing_CC, ')'),
         `30 percent missingness` = paste0(missing_30_p, ' (', coverage_30_p_missing_CC, ')'),
         `50 percent missingness` = paste0(missing_50_p, ' (', coverage_50_p_missing_CC, ')'),
         `70 percent missingness` = paste0(missing_70_p, ' (', coverage_70_p_missing_CC, ')')) %>%
  select(`Effect size` = effect_size, "Validation models":"70 percent missingness")

CC_table %>% knitr::kable()


