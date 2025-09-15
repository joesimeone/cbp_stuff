library(tidyverse)
library(httr2)
library(glue)
library(here)


## ---------------------------------------------------------------------------=
# Load hot-humid climate regions ----
## ---------------------------------------------------------------------------=

clim_path <-
  "C:/Users/js5466/OneDrive - Drexel University/r_master/new_projects/climate_regions/raw_data"

clim_reg <-
  read_csv(
    glue(
      '{clim_path}/climate_zones.csv'
    )
  ) %>%
  janitor::clean_names()

## Narrow to hot humid mixed humid climate region
hd_md_reg <-
  clim_reg %>%
  mutate(
    climate_region_filt = if_else(
      ba_climate_zone %in% c('Hot-Dry', 'Mixed-Dry"'),
      'IN',
      'OUT'
    )
  ) %>%
  filter(climate_region_filt == 'IN')

## Did that work? Expect States: Texas, CA, NM, AZ
unique(
  hd_md_reg$state
)

source(
  here('code', 'secrets_shh.R')
)


## --------------------------------------------------------------------------=
# Create function to build API Queries -----
## --------------------------------------------------------------------------=

create_cbp_requests <-
  function(
    fields,
    state_fips, ## What state do we want data from
    co_fips,
    year ## What year do we want data from
  ) {
    base_url <-
      glue("https://api.census.gov/data/{year}/cbp")

    api_query <-
      request(base_url) %>%
      req_url_query(
        get = fields,
        `for` = glue("county:{co_fips}"),
        `in` = glue("state:{state_fips}"),
        key = census_api_key
      ) %>%
      req_throttle(rate = 5) %>%
      req_retry(max_tries = 3, backoff = ~10) %>%
      req_timeout(30)

    return(api_query)
  }


## ---------------------------------------------------------------------------=
# Load raw url query inputs ----
## ---------------------------------------------------------------------------=

## So, we want to query the census API and return CBP data for the
## counties in hd_md_reg. These guys serve as raw inputs into the
## httr2 function

## The variables to be included in the eventual Co. buisness pattern DF
cbp_09_10_vars <-
  c(
    "COUNTY",
    "CSA",
    "EMP",
    "EMP_F",
    "EMP_N",
    "EMP_N_F",
    "EMPSZES",
    "EMPSZES_TTL",
    "ESTAB",
    "ESTAB_F",
    "FOOTID_GEO",
    "FOOTID_NAICS",
    "GEO_ID",
    "GEO_TTL",
    "GEOTYPE",
    "LFO",
    "LFO_TTL",
    "MD",
    "MSA",
    "NAICS2007",
    "NAICS2007_TTL",
    "PAYANN",
    "PAYANN_F",
    "PAYANN_N",
    "PAYANN_N_F",
    "PAYQTR1",
    "PAYQTR1_F",
    "PAYQTR1_N",
    "PAYQTR1_N_F",
    "ST",
    "YEAR"
  )

fields_09_10 <-
  paste(cbp_09_10_vars, collapse = ",")

study_years <-
  c(paste(2002:2010))

hd_md_split <-
  hd_md_reg %>%
  distinct(state_fips, county_fips) %>%
  split(.$state_fips)

api_arguments <-
  map(hd_md_split, ~ expand_grid(study_years, .x)) %>%
  list_rbind()


api_calls_09_10 <-
  filter(api_arguments, study_years %in% c('2009', '2010'))

## ---------------------------------------------------------------------------=
# Build API Calls -----
## ---------------------------------------------------------------------------=

cbp_api_09_10_calls <- pmap(
  api_calls_09_10,
  function(study_years, state_fips, county_fips) {
    create_cbp_requests(
      fields = fields_09_10,
      year = study_years,
      state_fips = state_fips,
      co_fips = county_fips
    )
  }
)

tictoc::tic()
cbp_data_09_10 <-
  map(
    cbp_api_09_10_calls[c(1:5)],
    ~ .x %>%
      req_perform()
  )
tictoc::toc()

cbp_09_10_raw <-
  map(
    cbp_data_09_10,
    ~ resp_body_json(.x, simplifyVector = TRUE) %>%
      as.data.frame(stringsAsFactors = TRUE)
  )
