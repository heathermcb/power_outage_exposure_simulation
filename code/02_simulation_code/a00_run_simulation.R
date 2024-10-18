# Create exposure data

library(here)

print('Expanding city-utility time series')

source(
  here(
    'code',
    '02_simulation_code',
    'code_for_creating_exposure_data',
    'a01_expand_city_utility_time_series.R'
  )
)

print('Power outage data expanded')

source(
  here(
    'code',
    '02_simulation_code',
    'code_for_creating_exposure_data',
    'a02_pull_distributions.R'
  )
)

print('Distributions pulled')

source(
  here(
    'code',
    '02_simulation_code',
    'code_for_creating_exposure_data',
    'a03_assemble_exposure_dataset.R'
  )
)

print('Exposure data assembled and saved')
