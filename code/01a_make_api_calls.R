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
    naics_year, ## What year is in the NAICS variable name
    state_fips, ## What state do we want data from
    co_fips, ## Which county
    year ## What year do we want data from
  ) {
    ## Minimum number of variables that is common across 2010 - 2002
    cbp_common_vars <-
      c(
        "COUNTY",
        "ST",
        "GEO_TTL",
        "YEAR",
        "PAYANN",
        "EMP",
        "EMPSZES_TTL",
        "ESTAB"
      )

    ## Coerce these variables to a string that API likes
    fields <-
      paste(cbp_common_vars, collapse = ",")

    ## Add flexible NAICS error term that changes depending on year of query call
    fields_fin <-
      glue("{fields},NAICS{naics_year},NAICS{naics_year}_TTL")

    ## This url keeps census secrets
    base_url <-
      glue("https://api.census.gov/data/{year}/cbp")

    ## This shapes the query | 1 call for each hot-humid county
    api_query <-
      request(base_url) %>%
      req_url_query(
        get = fields_fin,
        `for` = glue("county:{co_fips}"),
        `in` = glue("state:{state_fips}"),
        key = my_census_key
      ) %>%
      req_throttle(rate = 5) %>% ## Trying to be a good API citizen
      req_retry(max_tries = 3, backoff = ~10) %>%
      req_timeout(60)

    return(api_query)
  }


## ---------------------------------------------------------------------------=
# Load raw url query inputs ----
## ---------------------------------------------------------------------------=

## So, we want to automate our calls to census cbp api. We can do this by
## putting function arguments into a tibble and using pmap in the call

study_years <-
  c(paste(2002:2010))

hd_md_split <-
  hd_md_reg %>%
  distinct(state_fips, county_fips) %>%
  split(.$state_fips)

api_arguments <-
  map(hd_md_split, ~ expand_grid(study_years, .x)) %>%
  list_rbind() %>%
  mutate(
    naics_era = case_when(
      study_years %in% c('2009', '2010', '2008') ~ '2007',
      study_years == '2002' ~ '1997',
      TRUE ~ '2002'
    )
  )


## ---------------------------------------------------------------------------=
# Build API Calls -----
## ---------------------------------------------------------------------------=

tictoc::tic()

pwalk(
  api_args_unprocessed,
  function(study_years, state_fips, county_fips, naics_era) {
    cli::cli_alert(glue(
      'Creating Query for {state_fips}{county_fips} in {study_years}'
    ))

    Sys.sleep(0.5)
    api_query <-
      create_cbp_requests(
        naics_year = naics_era,
        year = study_years,
        state_fips = state_fips,
        co_fips = county_fips
      )

    cli::cli_alert('Peforming Query --> Coercing to dataframe')

    cbp_dat <-
      api_query %>%
      req_perform() %>%
      resp_body_json(simplifyVector = TRUE) %>%
      as.data.frame(stringsAsFactors = FALSE)

    cli::cli_alert("Performing basic cleaning pre export")

    cbp_cl <-
      cbp_dat %>%
      janitor::row_to_names(row_number = 1) %>%
      mutate(
        naics = str_remove(.data[[glue("NAICS{naics_era}")]], "\\d{4}$"),
        naics_ttl = str_remove(
          .data[[glue("NAICS{naics_era}_TTL")]],
          "\\d{4}$"
        ),
        naics_yr = naics_era
      )

    cli::cli_alert(glue(
      'Writing cleaned file for {state_fips}{county_fips} in {study_years}'
    ))

    write_csv(
      cbp_cl,
      glue('data/cbp_dat_{state_fips}{county_fips}_{study_years}.csv')
    )
  }
)
tictoc::toc()


## ---------------------------------------------------------------------------=
# Addressing Texas Weirdness -----
## ---------------------------------------------------------------------------=
## Post processing, we're left with 13 problematic calls. Let's see why

processed <-
  list.files(
    here('data')
  )

st_co_yr <-
  str_extract(processed, "\\d+_\\d{4}") %>%
  as_tibble() %>%
  rename(file_id = value)

api_args_unprocessed <-
  api_arguments %>%
  mutate(file_id = glue('{state_fips}{county_fips}_{study_years}')) %>%
  filter(!file_id %in% st_co_yr$file_id) %>%
  select(-file_id)

tx_annoyances <-
  pmap(
    api_args_unprocessed,
    function(study_years, state_fips, county_fips, naics_era) {
      cli::cli_alert(glue(
        'Creating Query for {state_fips}{county_fips} in {study_years}'
      ))

      Sys.sleep(0.5)
      api_query <-
        create_cbp_requests(
          naics_year = naics_era,
          year = study_years,
          state_fips = state_fips,
          co_fips = county_fips
        )
    }
  )

## This url keeps census secrets
help <-
  glue("https://api.census.gov/data/2010/cbp")


## Minimum number of variables that is common across 2010 - 2002
cbp_common_vars <-
  c(
    "COUNTY",
    "ST",
    "GEO_TTL",
    "YEAR",
    "PAYANN",
    "EMP",
    "EMPSZES_TTL",
    "ESTAB"
  )

## Coerce these variables to a string that API likes
fields <-
  paste(cbp_common_vars, collapse = ",")

## Add flexible NAICS error term that changes depending on year of query call
fields_fin <-
  glue("{fields}")


api_query <- request("https://api.census.gov/data/2003/cbp") %>%
  req_url_query(
    get = fields_fin,
    `for` = "county:*",
    `in` = "state:48",
    key = my_census_key
  ) %>%
  req_throttle(rate = 5) %>% # optional throttling
  req_retry(max_tries = 3, backoff = ~10) %>%
  req_timeout(60)


cbp_dat <-
  api_query %>%
  req_perform() %>%
  resp_body_json(simplifyVector = TRUE) %>%
  as.data.frame(stringsAsFactors = FALSE) %>%
  janitor::row_to_names(row_number = 1)

write_csv(api_args_unprocessed, 'tx_problem_obs.csv')
