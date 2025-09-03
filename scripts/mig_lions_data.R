# Description: This script creates the lions data set of immigration related cases to upload to deportationdataproject.org

library(dplyr)
library(tidyverse)
library(tidylog)
library(arrow)
library(purrr)
library(fs)

# Setup directories

input_dir <- "outputs"
output_dir <- "outputs/mig_LIONS"

# For local tests
input_dir <- "~/Library/CloudStorage/Box-Box/deportationdata/_processing/intermediate/EOUSA/library/lions_data"
output_dir <- "~/Dropbox/DDP/Tests"
fs::dir_create(output_dir)

# 0.1 Get immigration related cases

mig_cases <- arrow::read_feather(paste0(output_dir, "/immigration_cases.feather"))

# 1. Gs_participant  -------

## 1.0 Function to read in feather files, create UNIQUEID, select only specified variables AND SUBSET for immigration related cases and after 1996

read_filter_subset <- function(file_path, district_col, caseid_col, vars) {
  
  df <- arrow::read_feather(file_path) # Reading dataset
  
  # Ensure the specified columns exist
  if (!all(c(district_col, caseid_col) %in% colnames(df))) {
    stop("Specified columns do not exist in the data frame")
  }
  
  df  <- df |> 
    mutate(UNIQUEID = paste0(.data[[district_col]], .data[[caseid_col]])) |> # Creating uniqueid
    select(UNIQUEID, everything()) |> 
    mutate(SYS_INIT_DATE = as.Date(SYS_INIT_DATE, format = "%d-%b-%Y")) |> # Transforming date to correct format
    filter(!is.na(SYS_INIT_DATE),
           SYS_INIT_DATE >= as.Date("1996-01-01")) |> # Filter for cases after 1996
    filter(UNIQUEID %in% pull(mig_cases,UNIQUEID)) |> # Filter for migration related cases
    select(all_of(vars)) # Select variables
  
  return(df)
}

# 1.1 List all feather files that start with "gs_participant_"

participant_paths <- list.files(input_dir, pattern = "gs_participant_.*\\.feather$", full.names = TRUE)
# participant_paths_test <- participant_paths[1:20] # For testing, only first two files

# 1.2 Variables to subset
gs_participant_vars <- c("UNIQUEID", 
                         "DISTRICT",
                         "CASEID",
                         "ID", 
                         "AGENCY",
                         "SYS_INIT_DATE",
                         "TYPE",
                         "GENDER",
                         "COUNTRY",
                         "HOME_CITY",
                         "HOME_COUNTY",
                         "CRIM_HIST",
                         "HOME_ZIPCODE")

# 1.3 Map reading, filtering and subsetting function over all participants tables

dfs <- map(participant_paths, ~ read_filter_subset(.x, district_col = "DISTRICT", caseid_col = "CASEID", vars = gs_participant_vars))

# combine into one tibble
mig_lions_data <- bind_rows(dfs, .id = "source_file")

## 1.4 Renaming

mig_lions_data <- 
  mig_lions_data |> 
  rename(
    PARTICIPANT_TYPE = TYPE
  ) |> 
  select(!source_file) # Removing source file column

# 2. Gs_case -------

## 2.0 Function to read in feather files, create UNIQUEID and select only specified variables

read_and_subset <- function(file_path, district_col, caseid_col, vars) {
  
  df <- arrow::read_feather(file_path) # Reading dataset
  
  # Ensure the specified columns exist
  if (!all(c(district_col, caseid_col) %in% colnames(df))) {
    stop("Specified columns do not exist in the data frame")
  }
  
  df  <- df |> # Creating uniqueid
    mutate(UNIQUEID = paste0(.data[[district_col]], .data[[caseid_col]])) |> 
    select(UNIQUEID, everything())
  
  df <- df |> select(all_of(vars)) # Select variables
  
  return(df)
}
## 2.1 Run function to read, create UNIQUEID and subset

# Variables to subset
gs_case_vars <- c("UNIQUEID", 
                  "STATUS", 
                  "LEAD_CHARGE",
                  "UNIT",
                  "TYPE",
                  "BRANCH",
                  "OFFENSE_FROM",
                  "OFFENSE_TO",
                  "CLOSE_DATE")

# Read and subset
gs_case <- read_and_subset(paste0(input_dir, "/gs_case.feather"), "DISTRICT", "ID", gs_case_vars)

## 2.2 Renaming for join

gs_case <- 
  gs_case |> 
  rename(
    CASE_TYPE = TYPE,
    CASE_STATUS = STATUS
  )

## 2.3 Join to mig_lions_data

mig_lions_data <- 
  mig_lions_data |> 
  left_join(gs_case , by = "UNIQUEID")

# 3. Gs_sentence -------
## 3.1 Run function to read, create UNIQUEID and subset

### Variables to subset
gs_sentence <- c("UNIQUEID",
                  "FINE",
                  "INCAR_DAYS",
                  "INCAR_MONTHS",
                  "INCAR_TYPE",
                  "INCAR_YEARS")

### Read and subset

gs_sentence <- read_and_subset(paste0(input_dir, "/gs_sentence.feather"), "DISTRICT", "CASEID", gs_sentence)

## 3.2 Join to mig_lions_data

mig_lions_data <- 
  mig_lions_data |> 
  left_join(gs_sentence , by = "UNIQUEID")


# 4. Get gs_case_prog_cat  -------
## 4.1 Run function to read, create UNIQUEID and subset

# Variables to subset

gs_case_prog_cat <- c("UNIQUEID", 
                  "PROG_CAT")

# Read and subset
gs_case_prog_cat <- read_and_subset(paste0(input_dir, "/gs_case_prog_cat.feather"), "DISTRICT", "ID", gs_case_prog_cat)

## 4.2 Join to mig_lions_data

mig_lions_data <- 
  mig_lions_data |> 
  left_join(gs_case_prog_cat , by = "UNIQUEID")

# 5. Get list of charges  -------

mig_lions_data <- 
  mig_lions_data |> 
  left_join(mig_cases , by = "UNIQUEID") # Adding charge list and number of participants per case
  
# 6. Get gs_count  -------
## 6.1 Run function to read, create UNIQUEID and subset

# Variables to subset

gs_count <- c("UNIQUEID",
                      "CRTHISID",
                      "INSTID",
                      "PENT_PROV")

# Read and subset
gs_count <- read_and_subset(paste0(input_dir, "/gs_count.feather"), "DISTRICT", "CASEID", gs_count)

## 6.2 Join to mig_lions_data

mig_lions_data <- 
  mig_lions_data |> 
  left_join(gs_count , by = "UNIQUEID")

# 7. Get gs_court_hist -------
## 7.1 Run function to read, create UNIQUEID and subset

# Variables to subset
# 
# gs_court_hist <- c("UNIQUEID",
#                    "ID",
#                    "DISPOSITION",
#                    "DISP_REASON1",
#                    "DISP_REASON2",
#                    "DISP_REASON3",
#                    "DISP_DATE")
# 
# # Read and subset
# gs_court_hist <- read_and_subset(paste0(input_dir, "/gs_court_hist.feather"), "DISTRICT", "CASEID", gs_court_hist)
# 
# ## 7.2 Join to mig_lions_data
# 
# mig_lions_data <- 
#   mig_lions_data |> 
#   left_join(gs_court_hist , by = "UNIQUEID")

# 5. Reviewing missingess  -------

missing_summary <- mig_lions_data |> 
  summarise(across(
    everything(),
    ~ mean(is.na(.)) * 100
  )) |> 
  tidyr::pivot_longer(
    everything(),
    names_to = "variable",
    values_to = "percent_missing"
  ) |> 
  print(n = Inf)

# 6. Save final dataset -----

write_csv(mig_lions_data, paste0(output_dir, "/mig_lions_data.csv"))

# TODO: 
## Finish JUDGE_NAME, JUDGE_TYPE, NONMONETARY, CUSTODY_LOC, DETEN_REASON
## Verify merges worked correctly. There seems to be an observation missmatch. Also check rows at the participant vs case level. 
## Include cleaning from table_gs for various variables for easier analysis (e.g., judge name and type instead of the code)
## Include data checking code

# file_path <- paste0(input_dir, "/gs_court_hist.feather")
# df <- arrow::read_feather(file_path) # Reading dataset



