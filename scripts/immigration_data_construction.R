# Description: This script creates the lions data set of immigration related cases to upload to deportationdataproject.org

library(dplyr)
library(tidyverse)
library(tidylog)
library(arrow)
library(purrr)
library(fs)
library(validate) # For data checking code

# Setup directories

input_dir <- "outputs"
output_dir <- "outputs/mig_LIONS"

# For local tests

input_dir <- "~/Library/CloudStorage/Box-Box/deportationdata/_processing/intermediate/EOUSA/library/lions_data"
output_dir <- "~/Dropbox/DDP/Tests"
fs::dir_create(output_dir)

# 0.0 Helpers

## Data checking helper - stop if any rules are broken

confront_stop <- function(data, rules) { # rules = vector of rules create with validate::validator()
  
  validation <- confront(mig_lions_data, rules) # 
  s  <- summary(validation) # Keep the summary of the validation
  
  if (any(s$fails > 0 | s$error > 0)) { # If any rules are broken and/or have errors, the code will stop and print a message that might be helpful for debugging
    broken <- subset(s, fails > 0 | error > 0)
    
    msg <- paste0(
      "Validation failed on ", nrow(broken), " rule(s):\n",
      paste0(
        " - ", broken$name, ": ", # Includes the name of the rulke - make sure to include a name when creating the rules
        broken$expression, " failed, ", # Shows the rule that was broken
        broken$error, " errors while validating" # Shows if there were any errors while validating
        , collapse = "\n")
    )
    
    stop(msg, call. = FALSE)
  }}

## Clean missings 

clean_missings <- function(data) { 
  data |> 
    mutate(across(everything(), ~ {
      x <- .
      # Convert to character to test NAs safely, then restore class if needed
      # 1) Trim whitespace; 2) blank -> NA; 3) common tokens -> NA
      if (is.character(x)) {
        x <- str_trim(x)
        x[x == ""] <- NA_character_
        x[tolower(x) %in% c("na","n/a","null",".")] <- NA_character_
      }
      x
    }))
}

# 0.1 Get immigration related cases

mig_cases <- arrow::read_feather(paste0(output_dir, "/immigration_cases.feather"))

# 1. Gs_participant  -------

## 1.0 Function to read in feather files, create UNIQUEID, select only specified variables AND SUBSET for immigration related cases and after 1996

read_filter_subset <- function(file_path, district_col, caseid_col, vars) {
  
  df <- arrow::read_feather(file_path) # Reading dataset
  
  # Ensure the specified columns to subset exist
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
#participant_paths <- participant_paths[1:5] # For testing, only first five files

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

# 1.3 Map reading, filtering and subseting function over all participants tables

dfs <- map(participant_paths, ~ read_filter_subset(.x, district_col = "DISTRICT", caseid_col = "CASEID", vars = gs_participant_vars))

# combine into one tibble
mig_lions_data <- bind_rows(dfs, .id = "source_file")

## 1.4 Renaming

mig_lions_data <- 
  mig_lions_data |> 
  rename(
    PARTICIPANT_TYPE = TYPE
  ) |> 
  mutate(N_PARTICIPANTS = n_distinct(ID), .by = UNIQUEID) |> # Number of participants per case
  select(!source_file) # Removing source file column

## 1.5 Cleaning missingess

mig_lions_data <- clean_missings(mig_lions_data)

## Checking the binding
 # Set of rules to validate
 rules <- validator(
    not_all_districts = n_distinct(DISTRICT) == 93, # There should be 93 districts - If not, bind may have failed
    UNIQUEID_missing = mean(is.na(UNIQUEID)) * 100 == 0, # UNIQUEID should not be missing
    DISTRICT_missing = mean(is.na(DISTRICT)) * 100 == 0, # DISTRICT should not be missing
    CASEID_missing = mean(is.na(CASEID)) * 100 == 0, # CASEID should not be missing
    ID_missing = mean(is.na(ID)) * 100 == 0, # ID should not be missing
    SYS_INIT_DATE_missing = mean(is.na(SYS_INIT_DATE)) * 100 == 0, # UNIQUEID should not be missing
    PARTICIPANT_TYPE_missing = mean(is.na(PARTICIPANT_TYPE)) * 100 == 0, # PARTICIPANT_TYPE should not be missing
    GENDER_all_missing = mean(is.na(GENDER)) * 100 < 100, # GENDER should not be all missing
    COUNTRY_all_missing = mean(is.na(COUNTRY)) * 100 < 100, # COUNTRY should not be all missing
    HOME_CITY_all_missing = mean(is.na(HOME_CITY)) * 100 < 100, # HOME_CITY should not be all missing
    HOME_COUNTY_all_missing = mean(is.na(HOME_COUNTY)) * 100 < 100, # HOME_COUNTY should not be all missing
    CRIM_HIST_all_missing = mean(is.na(CRIM_HIST)) * 100 < 100, # CRIM_HIST should not be all missing
    HOME_ZIPCODE_all_missing = mean(is.na(HOME_ZIPCODE)) * 100 < 100, # HOME_ZIPCODE should not be all missing
    date_discrepancy = in_range(mig_lions_data$SYS_INIT_DATE, min = as.Date("1996-01-01"), max = today()),
    # No records before 1996 and not in the future to avoid potential date typos or errors
    non_unique_rows = is_unique(UNIQUEID, ID), # UNIQUEID and ID should uniquely identify each row
    n_participants_discrepancy = N_PARTICIPANTS == do_by(ID, by = UNIQUEID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants 
  )
 
 # Run helper function to check and stop if any rules are broken
 confront_stop(mig_lions_data, rules)

 # TODO: KEEP ONLY DEFENDENTS
 
 
 # Clean up
 rm(list = setdiff(ls(), c("mig_lions_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings"))) # Keeping only what's needed
 gc()
 
# 2. Gs_case -------

## 2.0 Function to read in feather files, create UNIQUEID and select only specified variables

read_and_subset <- function(file_path, district_col, caseid_col, vars) {
  
  df <- 
    arrow::read_feather(file_path) # Reading dataset
  
  df <- 
    df |> 
    mutate(
      UNIQUEID = str_c({{ district_col }}, {{ caseid_col }}), # Creating uniqueid 
      .before = everything() # Order
    )

  df <- 
    df |> select(all_of(vars)) # Select variables defined in vars vector
  
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
gs_case <- read_and_subset(paste0(input_dir, "/gs_case.feather"), DISTRICT, ID, gs_case_vars)

## 2.2 Renaming for join

gs_case <- 
  gs_case |> 
  rename(
    CASE_TYPE = TYPE,
    CASE_STATUS = STATUS
  )

## 2.3 Safely join to mig_lions_data

# Errors if no UNIQUEID in g_case appears in mig_lions_data
n_matched <- nrow(semi_join(mig_lions_data, gs_case, by = "UNIQUEID")) # Identify whether there will be matches first
if (n_matched == 0) stop("No rows matched on UNIQUEID.") # Will show an error if nothing matches

# Check observations before join
n_before <- nrow(mig_lions_data)

# If it's safe to proceed
mig_lions_data <- 
  left_join(mig_lions_data, gs_case, by = "UNIQUEID") # Matching

# Check observations before join
n_after  <- nrow(mig_lions_data) 

# To confirm that we are getting the expected number of rows (Given this is a case-level dataset we should not be getting more rows)
if (n_before != n_after) stop("Number of rows changed after join - Review merge") # Our number of observations pre and post join should be the same - Error if not

## Checking the merge

# Cleaning missingess

mig_lions_data <- clean_missings(mig_lions_data)

# Set of rules to validate
rules <- validator(
  CASE_STATUS_missing = mean(is.na(CASE_STATUS)) * 100 == 0, # CASE_STATUS should not be missing
  LEAD_CHARGE_missing = mean(is.na(LEAD_CHARGE)) * 100 == 0, # LEAD_CHARGE should not be missing
  BRANCH_missing = mean(is.na(BRANCH)) * 100 == 0, # BRANCH should not be missing
  UNIT_all_missing = mean(is.na(UNIT)) * 100 < 100, # UNIT should not be all missing
  CASE_TYPE_all_missing = mean(is.na(CASE_TYPE)) * 100 < 100, # CASE_TYPE should not be all missing
  OFFENSE_FROM_all_missing = mean(is.na(OFFENSE_FROM )) * 100 < 100, # OFFENSE_FROM should not be all missing
  CLOSE_DATE_all_missing = mean(is.na(CLOSE_DATE )) * 100 < 100, # CLOSE_DATE should not be all missing
  date_discrepancy = in_range(mig_lions_data$CLOSE_DATE, min = as.Date("1996-01-01"), max = today()),
  # No records before 1996 and not in the future to avoid potential date typos or errors
  non_unique_rows = is_unique(UNIQUEID, ID), # UNIQUEID and ID should uniquely identify each row
  n_participants_discrepancy = N_PARTICIPANTS == do_by(ID, by = UNIQUEID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants 
)

# Run helper function to check and stop if any rules are broken
confront_stop(mig_lions_data, rules)

# Clean up
rm(list = setdiff(ls(), c("mig_lions_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset"))) # Keeping only what's needed
gc()


# 3. Gs_sentence -------
## 3.1 Run function to read, create UNIQUEID and subset

### Variables to subset
gs_sentence <- c("UNIQUEID",
                 "ID" = "PARTID",
                 "FINE",
                 "INCAR_TYPE",
                 "INCAR_DAYS",
                 "INCAR_MONTHS",
                 "INCAR_YEARS")

### Read and subset

gs_sentence <- read_and_subset(paste0(input_dir, "/gs_sentence.feather"), DISTRICT, CASEID, gs_sentence)

## 3.2 Safely join to mig_lions_data

# Errors if no UNIQUEID in g_case appears in mig_lions_data
n_matched <- nrow(semi_join(mig_lions_data, gs_sentence, by = c("UNIQUEID", "ID"))) # Identify whether there will be matches first
if (n_matched == 0) stop("No rows matched on UNIQUEID.") # Will show an error if nothing matches

# Check observations before join
n_before <- nrow(mig_lions_data)

# If it's safe to proceed
mig_lions_data <- 
  left_join(mig_lions_data, gs_sentence,  by = c("UNIQUEID", "ID")) # Matching at participant level

# Check observations before join
n_after  <- nrow(mig_lions_data) 

# To confirm that we are getting the expected number of rows (Given this is a case-level dataset we should not be getting more rows)
if (n_before != n_after) stop("Number of rows changed after join - Review merge") # Our number of observations pre and post join should be the same - Error if not

## Checking the merge

# Cleaning missingess
mig_lions_data <- clean_missings(mig_lions_data)

# Set of rules to validate
rules <- validator(
  FINE_all_missing = mean(is.na(FINE)) * 100 < 100, # FINE should not be all missing
  INCAR_DAYS_all_missing = mean(is.na(INCAR_DAYS)) * 100 < 100, # CASE_TYPE should not be all missing
  INCAR_MONTHS_all_missing = mean(is.na(INCAR_MONTHS )) * 100 < 100, # INCAR_MONTHS should not be all missing
  INCAR_YEARS_all_missing = mean(is.na(INCAR_YEARS )) * 100 < 100, # INCAR_YEARS should not be all missing
  INCAR_TYPE_all_missing = mean(is.na(INCAR_TYPE )) * 100 < 100, # INCAR_TYPE should not be all missing
  non_unique_rows = is_unique(UNIQUEID, ID), # UNIQUEID and ID should uniquely identify each row
  n_participants_discrepancy = N_PARTICIPANTS == do_by(ID, by = UNIQUEID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants 
)

# Run helper function to check and stop if any rules are broken
confront_stop(mig_lions_data, rules)

# Clean up
rm(list = setdiff(ls(), c("mig_lions_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset"))) # Keeping only what's needed
gc()

# 4. Get gs_case_prog_cat  -------
## 4.1 Run function to read, create UNIQUEID and subset

# Variables to subset

gs_case_prog_cat <- c("UNIQUEID",
                      "ID",
                      "PROG_CAT")

# Read and subset
gs_case_prog_cat <- read_and_subset(paste0(input_dir, "/gs_case_prog_cat.feather"), DISTRICT, CASEID, gs_case_prog_cat)

## 4.2 Safely join to mig_lions_data

# Errors if no UNIQUEID in g_case appears in mig_lions_data
n_matched <- nrow(semi_join(mig_lions_data, gs_case_prog_cat, by = c("UNIQUEID", "ID"))) # Identify whether there will be matches first
if (n_matched == 0) stop("No rows matched on UNIQUEID.") # Will show an error if nothing matches - TODO: Confirm the level of analysis of this dataset, many IDs do not match. We are losing a lot of information and it might be from a merge issue. We could conca all programs and join by case, instead of case-id

# Check observations before join
n_before <- nrow(mig_lions_data)

# If it's safe to proceed
mig_lions_data <- 
  left_join(mig_lions_data, gs_case_prog_cat,  by = c("UNIQUEID", "ID")) # Matching at participant level

# Check observations before join
n_after  <- nrow(mig_lions_data) 

# To confirm that we are getting the expected number of rows (Given this is a case-level dataset we should not be getting more rows)
if (n_before != n_after) stop("Number of rows changed after join - Review merge") # Our number of observations pre and post join should be the same - Error if not

## Checking the merge

# Cleaning missingess
mig_lions_data <- clean_missings(mig_lions_data)

# 5. Get list of charges  -------
## Safely join with immigration charges vector

# Errors if no UNIQUEID in g_case appears in mig_lions_data
n_matched <- nrow(semi_join(mig_lions_data, mig_cases, by = "UNIQUEID")) # Identify whether there will be matches first
if (n_matched == 0) stop("No rows matched on UNIQUEID.") # Will show an error if nothing matches 

# Check observations before join
n_before <- nrow(mig_lions_data)

# If it's safe to proceed
mig_lions_data <- 
  left_join(mig_lions_data, mig_cases,  by = "UNIQUEID") # Matching at participant level

# Check observations before join
n_after  <- nrow(mig_lions_data) 

# To confirm that we are getting the expected number of rows (Given this is a case-level dataset we should not be getting more rows)
if (n_before != n_after) stop("Number of rows changed after join - Review merge") # Our number of observations pre and post join should be the same - Error if not

# Clean up
rm(list = setdiff(ls(), c("mig_lions_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset"))) # Keeping only what's needed
gc()


# TODO: STOPPED REVIEW HERE
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

gs_court_hist <- c("UNIQUEID",
                   "ID",
                   "DISPOSITION",
                   "DISP_REASON1",
                   "DISP_REASON2",
                   "DISP_REASON3",
                   "DISP_DATE")

# Read and subset
gs_court_hist <- read_and_subset(paste0(input_dir, "/gs_court_hist.feather"), "DISTRICT", "CASEID", gs_court_hist)

# Collapse by UNIQUEID and ID to have one row per participant - Concatenate disposition and reasons if multiple per UNIQUEID-ID (Confirm why????)

gs_court_hist <- gs_court_hist |> 
  group_by(UNIQUEID) |> 
  summarise(
    DISPOSITION   = paste(unique(na.omit(DISPOSITION)), collapse = ", "),
    DISP_REASON1  = paste(unique(na.omit(DISP_REASON1)), collapse = ", "),
    DISP_REASON2  = paste(unique(na.omit(DISP_REASON2)), collapse = ", "),
    DISP_REASON3  = paste(unique(na.omit(DISP_REASON3)), collapse = ", "),
    DISP_DATE  = paste(unique(na.omit(DISP_DATE)), collapse = ", "),
    .groups = "drop"
  )

# ## 7.2 Join to mig_lions_data

mig_lions_data <-
  mig_lions_data |>
  left_join(gs_court_hist , by = "UNIQUEID")

# 8. Get gs_court_judge -------
## 8.1 Run function to read, create UNIQUEID and subset

# Variables to subset

gs_court_judge <- c("UNIQUEID",
                  "ID",
                  "CRTHISID",
                  "JUDGEID")

# Read and subset

gs_court_judge <- read_and_subset(paste0(input_dir, "/gs_court_judge.feather"), "DISTRICT", "CASEID", gs_court_judge)

dup <- gs_court_judge |> 
  group_by(UNIQUEID, CRTHISID, ID) |> 
  summarise(n_unique = n_distinct(JUDGEID), .groups = "drop") |> 
  filter(n_unique > 1) 

## 8.2 Join to mig_lions_data

mig_lions_data <-
  mig_lions_data |>
  left_join(gs_court_judge , by = c("UNIQUEID", "CRTHISID", "ID"))

## 8.3 Get judge name and type from table_gs

# Read table_gs_judge: lookup table with judge code, names and type code
table_gs_judge <- read_feather(paste0(input_dir, "/table_gs_judge.feather")) |> 
  mutate(UNIQUE_JUDGEID = paste0(District,ID), # Create unique judge ID (District+ID)
         JUDGE_NAME = paste0(FirstName, " ", LastName), # Create JUDGE_NAME from First and Last Name
         JUDGE_TYPE = paste0(District, Type)) |> # Creating unique type code because they differ across districts
  select(UNIQUE_JUDGEID, JUDGE_NAME, JUDGE_TYPE) 
  
# Read table_gs_judge_type: lookup table with judge type code, type labels
table_gs_judge_type <- read_feather(paste0(input_dir, "/table_gs_judge_type.feather")) |> 
  mutate(JUDGE_TYPE = paste0(District, Code)) |> # Creating unique type code because they differ across districts
  select(JUDGE_TYPE, JUDGE_TYPE_LABEL = Description)

# Join to get JUDGE_NAME and JUDGE_TYPE
table_gs_judge <- table_gs_judge |> 
  left_join(table_gs_judge_type, by = "JUDGE_TYPE") 

# Join to main dataset
mig_lions_data <- mig_lions_data |> 
  mutate(UNIQUE_JUDGEID = paste0(DISTRICT, JUDGEID)) |> # Create unique judge ID (District+ID)
  left_join(table_gs_judge, by = "UNIQUE_JUDGEID") |> # Join to get JUDGE_NAME and JUDGE_TYPE
  select(-UNIQUE_JUDGEID) # Remove temporary variable

# 9. Get gs_defend_stat -------
## 9.1 Run function to read, create UNIQUEID and subset

# Variables to subset

gs_defend_stat <- c("UNIQUEID",
                    "PARTID",
                    "CUSTODY_LOC",
                    "DETEN_REASON")

# Read and subset

gs_defend_stat <- read_and_subset(paste0(input_dir, "/gs_defend_stat.feather"), "DISTRICT", "CASEID", gs_defend_stat)

# dup <- gs_court_judge |> 
#   group_by(UNIQUEID, CRTHISID, ID) |> 
#   summarise(n_unique = n_distinct(JUDGEID), .groups = "drop") |> 
#   filter(n_unique > 1) 

## 9.2 Join to mig_lions_data

mig_lions_data <-
  mig_lions_data |>
  left_join(gs_defend_stat , by = "UNIQUEID")


# 10. Reviewing missingness  -------


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

# 10. Save final dataset -----

arrow::write_feather(mig_lions_data, paste0(output_dir, "/immigration_defendant_level.feather"))

# TODO: 
## 
## Verify merges worked correctly. There seems to be an observation missmatch. Also check rows at the participant vs case level. 
## Include cleaning from table_gs for various variables for easier analysis (e.g., judge name and type instead of the code)
## Include data checking code


#pointblank::scan_data(palmerpenguins::penguins)



