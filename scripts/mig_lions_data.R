# Description: This script creates the lions data set of immigration related cases to upload to deportationdataproject.org

library(dplyr)
library(arrow)
library(purrr)
library(rlang)

# Setup directories

setwd("~/Library/CloudStorage/Box-Box/deportationdata")

input_dir <- "_processing/intermediate/EOUSA/library/lions_data"

output_dir <- "data/EOUSA"

# 0.1 Get immigration related cases

mig_cases <- arrow::read_feather(paste0(input_dir, "/filtered/immigration_cases.feather"))

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


# 3. Get GS_CASE_PROG_CAT  -------
## 3.1 Run function to read, create UNIQUEID and subset

# Variables to subset

gs_case_prog_cat <- c("UNIQUEID", 
                  "PROG_CAT")

# Read and subset
gs_case_prog_cat <- read_and_subset(paste0(input_dir, "/gs_case_prog_cat.feather"), "DISTRICT", "ID", gs_case_prog_cat)

## 3.2 Join to mig_lions_data

mig_lions_data <- 
  mig_lions_data |> 
  left_join(gs_case_prog_cat , by = "UNIQUEID")

# 4. Get CASELIST  -------

mig_lions_data <- 
  mig_lions_data |> 
  left_join(mig_cases , by = "UNIQUEID") # Adding charge list and number of participants per case
  

# Reviewing missingess  -------

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

# Save final dataset -----

write_csv(mig_lions_data, paste0(output_dir, "/mig_lions_data.csv"))




# 5. Get GS_CASE_PROG_CAT  -------
## 5.1 Run function to read, create UNIQUEID and subset
df <- arrow::read_feather(paste0(input_dir, "/gs_part_count.feather")) # Reading dataset
df2 <- arrow::read_feather(paste0(input_dir, "/gs_participant_AK.feather")) # Reading dataset


# UNSURE HOW TO IDENTIFY INDIVIDUALS, participants and part_count have different individual ids (e.g. 1996R00119)

# Variables to subset

gs_part_count <- c("UNIQUEID", 
                      "PROG_CAT")

# Read and subset
gs_part_count <- read_and_subset(paste0(input_dir, "/gs_part_count.feather"), "DISTRICT", "ID", gs_part_count)

## 5.2 Join to mig_lions_data

mig_lions_data <- 
  mig_lions_data |> 
  left_join(gs_case_prog_cat , by = "UNIQUEID")




