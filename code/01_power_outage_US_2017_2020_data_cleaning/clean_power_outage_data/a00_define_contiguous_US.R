# This script creates files that have 
# a) a crosswalk of state fips codes, county fips codes, state abbreviations, 
# and state names for counties we wish to capture in the POUS dataset because 
# they are in the contiguous US 
# b) a list of counties and five-digit county fips codes that are in the 
# contiguous US
# c) a shapefile of county boundaries for those counties in the contiguous US 
# that we're aiming to include 

# Last updated: Oct 3rd, 2024
# Author: Heather 

pacman::p_load(sf, here, tidyverse, data.table)

# set shapefile for whole project 
# states we wish to include: just being very clear about this list 
cotus_state_fips_abbrev <- data.frame(
  state_fips = 
    c("01", "04", "05", "06", "08", "09", "10", "12", "13", "16", "17", "18", 
      "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", 
      "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", 
      "44", "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56"), 
  state_ab = 
    c("AL", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "ID", "IL", "IN", 
      "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", 
      "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", 
      "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"),
  state_name = 
    c("Alabama", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
      "Delaware", "Florida", "Georgia", "Idaho", "Illinois", "Indiana", "Iowa", 
      "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", 
      "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
      "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", 
      "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", 
      "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
      "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", 
      "West Virginia", "Wisconsin", "Wyoming"),
  stringsAsFactors = FALSE
)

write_rds(cotus_state_fips_abbrev, here("data", 'cotus_state_fips_abbrev.RDS'))

# get and filter shapefile to that set of counties 
county_shp <- tigris::counties(year = 2020) # get
county_shp <- county_shp |> # save relevant info only
  mutate(five_digit_fips = paste0(STATEFP, COUNTYFP)) |>
  select(five_digit_fips, 
         state_fips = STATEFP, 
         county_fips = COUNTYFP,
         county_name = NAME) 

county_shp <- county_shp |>
  filter(state_fips %in% cotus_state_fips_abbrev$state_fips)

# reproject to albers
epsg_code <- 5070
county_shp <- st_transform(county_shp, crs = epsg_code)

# write 
write_rds(county_shp, here("data", "cotus_county_shp_w_fips.RDS"))

# also write backbone file with just fips and no geometry
county_list <- county_shp %>%
  st_drop_geometry() %>%
  distinct()

write_rds(county_list, here("data", "cotus_county_list_of_fips.RDS"))

# write backbone with all fips and all dates in 2018
dates_2018 <- seq(as.Date("2018-01-01"), as.Date("2018-12-31"), by = "day")

panel_fips_2018 <- 
  CJ(five_digit_fips = county_list$five_digit_fips, date = dates_2018)

write_rds(panel_fips_2018, here("data", "panel_for_2018.RDS"))

