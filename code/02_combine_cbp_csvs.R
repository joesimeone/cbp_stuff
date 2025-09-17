library(here)
library(tidyverse)
library(janitor)


## ---------------------------------------------------------------------------=
# Read files from cbp API -----
## ---------------------------------------------------------------------------=
cali_og_path <- "C:/git/ca_ore/data/"

## From previous version iteration of project. Good for checks & filters
cali_og_dat <-
  read_csv(
    glue::glue('{cali_og_path}/full_adj2025_ca_2002_2010_wpolicy_cleaned.csv')
  )

## Load up all the files we pulled from api (There are LOTS)
co_payroll_files <-
  list.files(
    here(
      'data'
    ),
    full.names = TRUE
  )

## From manual review (I know, I know), A few of the industry codes are a little
## different from Cali data, but payroll results appear to be the same. We're
## going to tweak this here to make the import smaller

industry_filters <-
  cali_og_dat %>%
  distinct(naics) %>%
  mutate(
    naics = as.character(naics),
    naics = case_when(
      naics == '31' ~ '31-33',
      naics == '44' ~ '44-45',
      naics == '48' ~ '48-49',
      TRUE ~ naics
    )
  ) %>%
  pull()


## Hard coded classes to prevent issues on list_rbind()
co_payroll_dat <-
  map(
    co_payroll_files,
    ~ read_csv(
      .x,
      show_col_types = FALSE,
      col_types = cols(
        "COUNTY" = col_character(),
        "ST" = col_character(),
        "GEO_TTL" = col_character(),
        "YEAR" = col_double(),
        "PAYANN" = col_double(),
        "EMP" = col_double(),
        "EMPSZES_TTL" = col_character(),
        "ESTAB" = col_double(),
        "state" = col_character(),
        "county" = col_character(),
        "naics" = col_character(),
        "naics_ttl" = col_character(),
        "naics_yr" = col_double()
      )
    ) %>%
      select(-matches('NAICS', ignore.case = FALSE)) %>%
      filter(naics %in% industry_filters)
  ) %>%
  list_rbind()

## 2002 - 2010 Intercensal Estimates
pop_est <-
  read_csv(
    "co-est00int-tot.csv",
    col_types = cols(
      'COUNTY' = col_character(),
      'STATE' = col_character()
    )
  )


## ---------------------------------------------------------------------------=
# Little test to make sure my logic works -----
## ---------------------------------------------------------------------------=

## Eye-balling it, I think that my method works to reproduce

ca_tst <-
  co_payroll_dat %>%
  filter(state == '06') %>%
  mutate(
    GEOID10 = glue::glue('{state}{county}')
  ) %>%
  group_by(county, YEAR, naics) %>%
  filter(ESTAB == max(ESTAB)) %>%
  ungroup() %>%
  distinct(GEOID10, YEAR, naics, .keep_all = TRUE) %>% ## Resolves duplicates where ESTAB is the same and both get pulled in
  arrange(state, county, YEAR, naics)

hot_dry_ca <-
  ca_tst %>%
  mutate(
    GEOID10 = glue::glue('{state}{county}')
  ) %>%
  distinct(GEOID10) %>%
  pull()

## From Tina's original data frame. Comparing values should yield the same payroll where we can compare
ca_tina <-
  cali_og_dat %>%
  mutate(
    geoid = paste('0', GEOID10, sep = ""),
    naics = as.character(naics),
    naics = case_when(
      naics == '31' ~ '31-33',
      naics == '44' ~ '44-45',
      naics == '48' ~ '48-49',
      TRUE ~ naics
    )
  ) %>%
  filter(geoid %in% hot_dry_ca) %>%
  distinct(
    GEOID10,
    naics,
    year,
    .keep_all = TRUE
  ) %>%
  arrange(GEOID10, year, naics) %>%
  mutate(
    GEOID10 = paste('0', GEOID10, sep = "")
  )


## --------------------------------------------------------------------------=
# Last Touches -----
## --------------------------------------------------------------------------=

hot_humid_payrolls <-
  co_payroll_dat %>%
  mutate(
    GEOID10 = glue::glue('{state}{county}')
  ) %>%
  group_by(GEOID10, YEAR, naics) %>%
  filter(ESTAB == max(ESTAB)) %>%
  ungroup() %>%
  distinct(GEOID10, YEAR, naics, .keep_all = TRUE) %>% ## Resolves duplicates where ESTAB is the same and both get pulled in
  arrange(GEOID10, YEAR, naics) %>%
  janitor::clean_names()


## Formatting census estimates | Want panel data, so we take column estimates and
## swing them wide. ## Also need to do a bunch of nonsense to make join work
pop_est_fin <-
  pop_est %>%
  janitor::clean_names() %>%
  mutate(
    st_nchar = nchar(state),
    co_nchar = nchar(county)
  ) %>%
  mutate(
    st_fix = if_else(st_nchar == 1, paste('0', state, sep = ""), state),
    co_fix = case_when(
      co_nchar == 1 ~ paste('00', county, sep = ''),
      co_nchar == 2 ~ paste('0', county, sep = ''),
      TRUE ~ county
    ),
    geoid_fix = glue::glue('{st_fix}{co_fix}')
  ) %>%
  select(
    geoid_fix,
    contains(c('estimate'))
  ) %>%
  pivot_longer(
    cols = contains(c('estimate')),
    names_to = 'eras',
    values_to = 'pop_estimates'
  ) %>%
  mutate(
    year = str_extract(eras, "\\d+"),
    year = as.numeric(year)
  ) %>%
  select(-eras)

hot_humid_payrolls_fin <-
  hot_humid_payrolls %>%
  left_join(pop_est_fin, by = c('geoid10' = 'geoid_fix', 'year')) %>%
  select(-county_2)


write_csv(
  hot_humid_payrolls_fin,
  here('fin_data', 'hot_dry_mixed_dry_county_payrolls.csv')
)
