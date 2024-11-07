# Create figure explaining power outage definition 

pacman::p_load(tidyverse, here)


hrs <- seq(1:24)
customers_served <- 1000
customers_out <- c(3, 2, 4, 524, 1, 10, 15, 145, 200, 250, 600, 557, 550, 557, 
                   599, 600, 601, 400, 300, 4, 34, 1, 35, 1)

po_data <- data.frame(hrs, customers_out)

cust_out <- 
  po_data %>% 
  ggplot() + 
  geom_point(aes(x = hrs, y = customers_out), size = 3) +
  geom_line(aes(x = hrs, y = customers_out)) +
  scale_x_continuous(breaks = seq(from = 1, to = 24, by = 3)) +
  theme_minimal() +
  xlab('Hours in the day') +
  ylab('Customers without power\nin county i') +
  geom_hline(yintercept = 100, linetype = 'dashed') +
  theme(text = element_text(size = 24)) 


ggsave(
  cust_out,
  filename = here(
    'figures',
    'power_outage_definition_fig',
    'customers_out.pdf'
  ),
  width = 5,
  height = 5
)

po_data <- 
  po_data %>% 
  mutate(power_out = ifelse(customers_out > 100, 1, 0))

po_on <-
  po_data %>%
  ggplot() +
  geom_point(aes(x = hrs, y = power_out), size = 3) +
  scale_x_continuous(breaks = seq(from = 1, to = 24, by = 3)) +
  theme_minimal() +
  scale_y_continuous(breaks = c(0, 1)) +
  xlab('Hours in the day') +
  ylab('Power outage on in \ncounty i, Y/N') +
  theme(text = element_text(size = 24)) 


ggsave(
  po_on,
  filename = here(
    'figures',
    'power_outage_definition_fig',
    'power_outage_on.pdf'
  ),
  width = 5,
  height = 5
)
