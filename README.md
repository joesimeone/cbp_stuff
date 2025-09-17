# Purpose

Code to pull down labor productivity (annual payroll data) in hot-humid climate region counties from the 
[county buisness pattern (cbp)](https://www.census.gov/data/developers/data-sets/cbp-zbp/cbp-api.html) api. Script
produces county level measures of annual payroll (and some other stuff) for select industries between 2002 - 2010. 
See [climate regions csv](https://github.com/joesimeone/cbp_stuff/blob/main/climate_zones.csv) for county assignments. See 
[DOE guidance](https://www1.eere.energy.gov/buildings/publications/pdfs/building_america/ba_climateguide_7_1.pdf) for how assignments
were done. 

# Workflow

It's pretty quick and dirty, but it goes:

1. [**01_make_api_calls.R**](https://github.com/joesimeone/cbp_stuff/blob/main/code/01a_make_api_calls.R): Uses the httr2 package 
and purrr create a series of API calls for each year-county. Because I only figured out how to query 1 county at a time, the output
here produces A LOT of uniform csvs (1200). There might be a better way, but I didn't get there.
1. [**02_combine_cbp_csvs.R**](https://github.com/joesimeone/cbp_stuff/blob/main/code/02_combine_cbp_csvs.R):
Cleans those csv files up, filtering for the industry codes that we care about, and joining them with
   [intercensal population estimates](https://www.census.gov/data/datasets/time-series/demo/popest/intercensal-2000-2010-counties.html).


   # Industries in dataset

| naics | naics_ttl                                         |
|------:|---------------------------------------------------|
| 11    | Forestry, fishing, hunting, and agriculture support |
| 21    | Mining                                            |
| 22    | Utilities                                         |
| 23    | Construction                                      |
| 31-33 | Manufacturing                                     |
| 42    | Wholesale trade                                   |
| 44-45 | Retail trade                                      |
| 48-49 | Transportation & warehousing                      |
| 51    | Information                                       |
| 52    | Finance & insurance                                |

   

   # County Buisness Patterns API resources

   1. [2010 data example](https://www.census.gov/data/developers/data-sets/cbp-zbp/cbp-api.2010.html#list-tab-711980547): Click
      here to see available variables and example api calls. **Note that field names do change year over year, so some care is
      required to not make the API guides angry as you shift over time.
    2. [census api user guide](https://www.census.gov/data/developers/guidance/api-user-guide.html): Here's the census API user guide.
   There's much more stuff than just buisness pattern data!

Questions or confusion, feel free to hit me up at joesimeone9@gmail.com or js5466@drexel.edu
      
