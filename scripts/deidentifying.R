# Description: This script deidentifies the EOUSA data by removing identifiable information (PII) - The redacted variables were chosen through pii analysis on Aug 2025 

library(dplyr)
library(tidyverse)
library(tidylog)
library(purrr)
library(tibble)
library(arrow)

# 0. Setup directories

input_dir <- "outputs"

output_dir <- "outputs"

# 1. Redacting function

redact_pii <- function(input_path, output_path, vars) {
    arrow::read_feather(input_path) |> # Read .feather file
    mutate(
      across(
        any_of(vars),                     # To avoid error if a var is missing
        ~ if_else(is.na(.), NA_character_, "*") # Redact to *
      )
    ) |>
    arrow::write_feather(output_path)
}

# 2. Mapping table

mapping <- tibble(
  file  = c("gs_case_doj_div.feather", # .Include full names (n < 15), addresses, A-Numbers, phone numbers and numbers with SSN structure (XXX-XX-XXXX) instead of DOJ_NUMBER
            "gs_court_hist.feather", # Includes full names or case names (n < 15), addresses, A-numbers, phone numbers, and (potentially) SSN instead of COURT_NUMBER
            # "table_gs_oppose_attorn.feather", # Phone numbers instead of ZipCode
            "gs_archive_case.feather", # Includes full names for some of the “last lead attorney” (AUSA_LAST_NAME)
            "gs_defend_stat.feather", # Includes full names, addresses and phone numbers of bond providers in BOND_PROVIDER
            "gs_relief.feather", # Some rows in AMOUNT are flagged as phone numbers
            "gs_staff.feather", # Includes full names, some emails, and the specific office location of staff members (active or not)
            # "table_gs_judge.feather", # Includes full names and courtroom location (Cities and/or addresses) of judges
            "gs_case.feather", # DCMNS_NUMBER includes numbers flagged as SSN/ITIN, but do not follow the same structure
            "gs_sentence.feather" # Flagged phone numbers instead of RELATED_FLU_USAO. 
            ), 
  vars  = list(
    c("DOJ_NUMBER"),
    c("COURT_NUMBER"),
    # c("ZipCode"),
    c("AUSA_LAST_NAME"),
    c("BOND_PROVIDER"),
    c("AMOUNT"),
    c("LAST_NAME","FIRST_NAME","OFFICE_LOC"),
    # c("LastName","FirstName","CourtRoom"),
    c("DCMNS_NUMBER"),
    c("RELATED_FLU_USAO")
  )
)

# 3. Include paths in table

mapping <- mapping |>
  mutate(
    input  = file.path(input_dir, file),
    output = file.path(output_dir, file)
  )

# 4. Process each tibble row

purrr::pwalk(
  .l = list(mapping$input, mapping$output, mapping$vars),
  .f = redact_pii
)

# 5. Additional flow for gs_participant_* tables

 ## 5.1 Get gs_participant_* paths 

  participant_inputs <- list.files(input_dir, pattern = "gs_participant_.*\\.feather$", full.names = TRUE) # 
  #participant_inputs <- participant_inputs[1:2] # For testing, only first two files
  
  participant_outputs <- file.path(
    output_dir,
    basename(participant_inputs)
  )
  
  ## 5.2 Variables to subset
  gs_participant_vars <- c("EMPLOYER_DESC", # Has some specific descriptions that can be identifiable
                           "CATS_ASSET_ID", # The following include full names (n < 5) and specific company names, A-Numbers, phone numbers, A-numbers and (clearly) SSN instead
                           "OFF_CITY",
                           "HOME_ZIPCODE",
                           "HOME_CITY",
                           "HOME_COUNTY")

  ## 5.3 Map reading, redacting and saving function
  
  # Run over all files
  purrr::walk2(
    participant_inputs,
    participant_outputs,
    ~ redact_pii(.x, .y, gs_participant_vars)
  )
  
