# Plot results from DID and CC missingness analyses for manuscript, as well as
# make tables and coverage figures

# Libraries ---------------------------------------------------------------

pacman::p_load(here, tidyverse, data.table)

# Read --------------------------------------------------------------------

did <- read_rds(here(
  'results',
  'simulation_model_output',
  'results_all_missingness_DID.RDS'
)) |> setDT()

# extract info from model labels
did[, `:=`(
  percent_affected = sub(".*_(\\d+\\.?\\d*)p_.*", "\\1", outcome),
  percent_missing = sub(".*_missing_(\\d+)_.*", "\\1", outcome),
  effect_size = sub(".*_(\\d+\\.?\\d*p)$", "\\1", outcome)
)]

# fix none missing case
did[, `:=`(
  percent_affected = ifelse(
    grepl("outcome_exposed_8_hrs_0.005_none_missing", outcome),
    0,
    percent_affected
  ),
  percent_missing = ifelse(
    grepl("outcome_exposed_8_hrs_0.005_none_missing", outcome),
    0,
    percent_missing
  )
)]

# order factors correctly
did$percent_affected <-
  factor(did$percent_affected, levels = c("80", "50", "20", '0'))
did$percent_missing <-
  factor(did$percent_missing, levels = c("20", "50", "80", '0'))

# map effect_size to numeric values
did[, effect := case_when(
  effect_size == '5p' ~ 0.05,
  effect_size == '1p' ~ 0.01,
  effect_size == '0.5p' ~ 0.005
)]

# calculate bias 
did[, percent_bias := ((estimate - effect)/effect) * 100]

# add effect size labels 
did <- did %>% mutate(
  effect_size = case_when(
    effect_size == '0.5p' ~ ".5% increase",
    effect_size == '1p' ~ "1% increase",
    effect_size == '5p' ~ "5% increase"
  )
)

# plot main figure 1
p1 <- did %>%
  as.data.frame() %>%
  filter(percent_affected != 0) %>%
  filter(percent_missing != 0) %>%
  rename(`% counties affected by missingness` = percent_missing,
         `% data missing in counties affected` = percent_affected) %>%
  ggplot() +
  geom_hline(
    yintercept = 0,
    linetype = "dashed",
    color = "black",
    size = 1
  ) +
  geom_boxplot(aes(x = as.factor(effect_size), y = percent_bias, fill = as.factor(effect_size)), alpha = 0.8) +
  facet_grid(rows = vars(`% data missing in counties affected`),
             cols = vars(`% counties affected by missingness`),
             labeller = labeller(`% counties affected by missingness` = label_both)) +
  scale_y_continuous(
    breaks = c(-100, 0, 100),
    limits = c(-100, 100),
    sec.axis = sec_axis(~ ., name = "% data missing in counties affected")
  ) +
  xlab("Effect size") + ylab("% bias") +
  labs(fill = "Simulated increase in risk\nof hospitalization with 8+ hour\npower outage exposure") +
  ggtitle(
    "Bias in simulations representing missing data, using difference-in-differences study design"
  ) +
  theme_minimal() + 
  theme(
    #axis.text.x = element_blank(),
    #axis.ticks.x = element_blank(),
    axis.ticks.y.right = element_blank(),
    axis.text.y.right = element_blank()) + 
  theme(plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),  # Center the title and add margin below
        plot.margin = margin(t = 20, r = 20, b = 20, l = 20),  # Add margin around the plot
       # legend.position = "bottom",  # Position the legend at the bottom
        legend.margin = margin(t = 30),  # Add margin above the legend
        legend.box.margin = margin(l = 30),  # Add margin inside the legend box
        axis.title.x = element_text(margin = margin(t = 20)),  # Add margin above x-axis title
        axis.title.y = element_text(margin = margin(r = 20)),  # Add margin to the right of y-axis title
        axis.title.y.right = element_text(margin = margin(l = 20)),
       panel.spacing = unit(0.5, "lines"),  # Adjust spacing between panels
       panel.border = element_rect(color = "black", fill = NA, size = 1)  # Add border around each panel
  ) +  # Add margin to the left of secondary y-axis title 
theme(text = element_text(size = 30)) +
  theme(legend.title = element_text(size = 27)) 

ggsave(
  here("figures", "DID_bias_nov_6_2024.pdf"),
  plot = p1,
  width = 30,
  height = 15,
  dpi = 300
)



# Coverage and tables -----------------------------------------------------

coverage <- 
  did %>%
  mutate(covered = ifelse(effect < upper_ci & effect > lower_ci, 1, 0))

coverage <-
  coverage %>%
  group_by(percent_missing, percent_affected, effect_size) %>%
  summarize(mean_percent_bias = mean(percent_bias),
            coverage = mean(covered)*100)

c1 <- coverage %>%
  filter(percent_affected != 0) %>%
  filter(percent_missing != 0) %>%
  rename(`% counties affected by missingness` = percent_missing,
         `% data missing in counties affected` = percent_affected) %>%
 
  ggplot() +
  geom_hline(
    yintercept = 95,
    linetype = "dashed",
    color = "black",
    size = 1
  ) +
  geom_point(
    aes(x = as.factor(effect_size), y = coverage, fill = as.factor(effect_size)),
    shape = 23,
    size = 11,
    position = position_dodge(width = 0.75)
  ) +
  facet_grid(rows = vars(`% data missing in counties affected`),
             cols = vars(`% counties affected by missingness`),
             labeller = labeller(`% counties affected by missingness` = label_both)) +
  theme_minimal() +
  labs(fill = "Simulated increase in risk\nof hospitalization with 8+ hour\npower outage exposure") +
  scale_y_continuous(
    breaks = seq(0, 100, by = 20),
    sec.axis = sec_axis(~ ., name = "% data missing in counties affected")
  ) +
  
  theme(legend.title = element_text(size = 27)) +
  xlab('Effect size') + ylab("Coverage: \n% of confidence intervals containing simulated effect") +
  theme(
    #axis.text.x = element_blank(),
    # Remove x-axis text
    #axis.ticks.x = element_blank(),
    # Remove x-axis ticks
    axis.ticks.y.right = element_blank(),
    axis.text.y.right = element_blank()
  ) +
  theme(text = element_text(size = 35)) +
  theme(plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),  # Center the title and add margin below
        plot.margin = margin(t = 40, r = 40, b = 40, l = 40),  # Add margin around the plot
        # legend.position = "bottom",  # Position the legend at the bottom
        legend.margin = margin(t = 30),  # Add margin above the legend
        legend.box.margin = margin(l = 30),  # Add margin inside the legend box
        axis.title.x = element_text(margin = margin(t = 20)),  # Add margin above x-axis title
        axis.title.y = element_text(margin = margin(r = 20)),  # Add margin to the right of y-axis title
        axis.title.y.right = element_text(margin = margin(l = 20))  # Add margin to the left of secondary y-axis title
  ) +
  ggtitle(
    "Coverage in simulations representing missing data, using difference-in-differences study design"
  ) +
  theme(
    panel.spacing = unit(1, "lines"),  # Increase space between facets
    panel.border = element_rect(color = "black", fill = NA, size = 1),  # Add a box around facets
    panel.grid.major = element_line(color = "darkgray", size = 0.5),  # Make major grid lines darker
    panel.grid.minor = element_line(color = "darkgray", size = 0.25)  # Make minor grid lines darker
  )

ggsave(
  here("figures", "DID_coverage_nov_6_2024.pdf"),
  plot = c1,
  width = 32,
  height = 16,
  dpi = 300
)


o <- coverage %>% filter(percent_missing == 80) %>%
  pivot_wider(names_from = c(percent_missing, percent_affected), values_from = c(mean_percent_bias, coverage))

# CC results --------------------------------------------------------------

# Read --------------------------------------------------------------------

cc <- read_rds(here(
  'results',
  'simulation_model_output',
  'results_all_missingness_CC.RDS'
)) |> setDT() |> 
  filter(!is.na(estimate))

# extract info from model labels
cc[, `:=`(
  percent_affected = sub(".*_(\\d+\\.?\\d*)p_.*", "\\1", outcome),
  percent_missing = sub(".*_missing_(\\d+)_.*", "\\1", outcome),
  effect_size = sub(".*_(\\d+\\.?\\d*p)$", "\\1", outcome)
)]

# fix none missing case
cc[, `:=`(
  percent_affected = ifelse(
    grepl("outcome_exposed_8_hrs_0.005_none_missing", outcome),
    0,
    percent_affected
  ),
  percent_missing = ifelse(
    grepl("outcome_exposed_8_hrs_0.005_none_missing", outcome),
    0,
    percent_missing
  )
)]

# Ensure percent_affected and percent_missing are factors with the desired order
cc$percent_affected <- factor(cc$percent_affected, levels = c("80", "50", "20"))
cc$percent_missing <- factor(cc$percent_missing, levels = c("20", "50", "80"))

# map effect_size to numeric values
cc[, effect := case_when(
  effect_size == '5p' ~ 0.05,
  effect_size == '1p' ~ 0.01,
  effect_size == '0.5p' ~ 0.005
)]

# calculate bias 
cc[, percent_bias := ((estimate - effect)/effect) * 100]

cc <- cc %>% mutate(
  effect_size = case_when(
    effect_size == '0.5p' ~ ".5% increase",
    effect_size == '1p' ~ "1% increase",
    effect_size == '5p' ~ "5% increase"
  )
)



# plot main figure 1
p2 <- cc %>%
  as.data.frame() %>%
  filter(percent_affected != 0) %>%
  filter(percent_missing != 0) %>%
  rename(`% counties affected by missingness` = percent_missing,
         `% data missing in counties affected` = percent_affected) %>%
  ggplot() +
  geom_hline(
    yintercept = 0,
    linetype = "dashed",
    color = "black",
    size = 1
  ) +
  geom_boxplot(aes(x = as.factor(effect_size), y = percent_bias, fill = as.factor(effect_size)), alpha = 0.8) +
  facet_grid(rows = vars(`% data missing in counties affected`),
             cols = vars(`% counties affected by missingness`),
             labeller = labeller(`% counties affected by missingness` = label_both)) +
  scale_y_continuous(
    breaks = c(-300, -100, 0, 100, 300),
    sec.axis = sec_axis(~ ., name = "% data missing in counties affected")
  ) +
  xlab("Effect size") + ylab("% bias") +
  labs(fill = "Simulated increase in risk\nof hospitalization with 8+ hour\npower outage exposure") +
  ggtitle(
    "Bias in simulations representing missing data, using a case-crossover study design") +
  theme_minimal() + 
  theme(
   # axis.text.x = element_blank(),
    #axis.ticks.x = element_blank(),
    axis.ticks.y.right = element_blank(),
    axis.text.y.right = element_blank()) + 
  theme(plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),  # Center the title and add margin below
        plot.margin = margin(t = 20, r = 20, b = 20, l = 20),  # Add margin around the plot
        # legend.position = "bottom",  # Position the legend at the bottom
        legend.margin = margin(t = 30),  # Add margin above the legend
        legend.box.margin = margin(l = 30),  # Add margin inside the legend box
        axis.title.x = element_text(margin = margin(t = 20)),  # Add margin above x-axis title
        axis.title.y = element_text(margin = margin(r = 20)),  # Add margin to the right of y-axis title
        axis.title.y.right = element_text(margin = margin(l = 20)),
        panel.spacing = unit(0.5, "lines"),  # Adjust spacing between panels
        panel.border = element_rect(color = "black", fill = NA, size = 1)) +  # Add margin to the left of secondary y-axis title 
  theme(text = element_text(size = 30)) +
  theme(legend.title = element_text(size = 27)) 


ggsave(
  here("figures", "CC_bias_oct_23_2024.pdf"),
  plot = p2,
  width = 30,
  height = 15,
  dpi = 300
)



coverage <- 
  cc %>%
  mutate(covered = ifelse(effect < upper_ci & effect > lower_ci, 1, 0))

coverage <-
  coverage %>%
  group_by(percent_missing, percent_affected, effect_size) %>%
  summarize(mean_percent_bias = mean(percent_bias),
            coverage = mean(covered)*100)

c1 <- coverage %>%
  filter(percent_affected != 0) %>%
  filter(percent_missing != 0) %>%
  rename(`% counties affected by missingness` = percent_missing,
         `% data missing in counties affected` = percent_affected) %>%
  
  ggplot() +
  geom_hline(
    yintercept = 0.95,
    linetype = "dashed",
    color = "black",
    size = 1
  ) +
  geom_point(
    aes(x = as.factor(effect_size), y = coverage, fill = as.factor(effect_size)),
    shape = 23,
    size = 11,
    position = position_dodge(width = 0.75)
  ) +
  facet_grid(rows = vars(`% data missing in counties affected`),
             cols = vars(`% counties affected by missingness`),
             labeller = labeller(`% counties affected by missingness` = label_both)) +
  theme_minimal() +
  labs(fill = "Simulated increase in risk\nof hospitalization with 8+ hour\npower outage exposure") +
  scale_y_continuous(
    breaks = seq(0, 100, by = 20),
    sec.axis = sec_axis(~ ., name = "% data missing in counties affected")
  ) +
  
  theme(legend.title = element_text(size = 27)) +
  xlab('Effect size') + ylab("Coverage: \n% of confidence intervals containing simulated effect") +
  theme(
    #axis.text.x = element_blank(),
    # Remove x-axis text
    #axis.ticks.x = element_blank(),
    # Remove x-axis ticks
    axis.ticks.y.right = element_blank(),
    axis.text.y.right = element_blank()
  ) +
  theme(text = element_text(size = 35)) +
  theme(plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),  # Center the title and add margin below
        plot.margin = margin(t = 40, r = 40, b = 40, l = 40),  # Add margin around the plot
        # legend.position = "bottom",  # Position the legend at the bottom
        legend.margin = margin(t = 30),  # Add margin above the legend
        legend.box.margin = margin(l = 30),  # Add margin inside the legend box
        axis.title.x = element_text(margin = margin(t = 20)),  # Add margin above x-axis title
        axis.title.y = element_text(margin = margin(r = 20)),  # Add margin to the right of y-axis title
        axis.title.y.right = element_text(margin = margin(l = 20))  # Add margin to the left of secondary y-axis title
  ) +
  ggtitle(
    "Coverage in simulations representing missing data, using case-crossover study design"
  ) 


ggsave(
  here("figures", "CC_coverage_oct_23_2024.pdf"),
  plot = c1,
  width = 32,
  height = 16,
  dpi = 300
)

o <- coverage %>% filter(percent_missing == 80) %>%
  pivot_wider(names_from = c(percent_missing, percent_affected), values_from = c(mean_percent_bias, coverage))


