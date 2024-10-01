library(here)
iiijj1 <- Sys.time()
source(
  here(
    "power_outage_medicare",
    "power_outage_US_2017_2020_data_cleaning",
    "code_feb_23",
    "b01_read_and_clean.R"
  ),
  echo = TRUE
)
rm()
gc()
source(
  here(
    "power_outage_medicare",
    "power_outage_US_2017_2020_data_cleaning",
    "code_feb_23",
    "b02_expand.R"
  ),
  echo = TRUE
)
rm()
gc()
# source(
#   here(
#     "power_outage_medicare",
#     "power_outage_US_2017_2020_data_cleaning",
#     "code_feb_23",
#     "b03_hrly_locf.R"
#   ),
#   echo = TRUE
# )
# rm()
# gc()
# source(
#   here(
#     "power_outage_medicare",
#     "power_outage_US_2017_2020_data_cleaning",
#     "code_feb_23",
#     "b04_agg_to_county.R"
#   ),
#   echo = TRUE
# )
rm()
gc()
source(
  here(
    "power_outage_medicare",
    "power_outage_US_2017_2020_data_cleaning",
    "code_feb_23",
    "b03_attach_denoms.R"
  ),
  echo = TRUE
)
rm()
gc()

source(here(  "power_outage_medicare",
              "power_outage_US_2017_2020_data_cleaning",
              "code_feb_23", "b04_id_outages.R"), echo = TRUE)
rm()
gc()

iiijj2 <- Sys.time()
