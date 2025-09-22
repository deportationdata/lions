# Description: This script creates the lions data set of immigration related cases to upload to deportationdataproject.org

library(dplyr)
library(tidyverse)
library(tidylog)
library(arrow)
library(purrr)
library(fs)
install.packages("validate")
install.packages("pointblank")
library(validate) # For data checking code

# Setup directories

input_dir <- "outputs"
output_dir <- "outputs/mig_LIONS"

# For local tests

# input_dir <- "~/Library/CloudStorage/Box-Box/deportationdata/_processing/intermediate/EOUSA/library/lions_data"
# output_dir <- "~/Dropbox/DDP/Tests"
fs::dir_create(output_dir)

# 0.0 Helpers

## Data checking helper - stop if any rules are broken

confront_stop <- function(data, rules) { # rules = vector of rules created with validate::validator()

  validation <- confront(immigration_data, rules)
  s <- summary(validation) # Keep the summary of the validation

  if (any(s$fails > 0 | s$error > 0)) { # If any rules are broken and/or have errors, the code will stop and print a message that might be helpful for debugging
    broken <- subset(s, fails > 0 | error > 0)

    msg <- paste0(
      "Validation failed on ", nrow(broken), " rule(s):\n",
      paste0(
        " - ", broken$name, ": ", # Includes the name of the rule - make sure to include a name when creating the rules
        broken$expression, " failed, ", # Shows the rule that was broken
        broken$error, " errors while validating" # Shows if there were any errors while validating
        ,
        collapse = "\n"
      )
    )

    stop(msg, call. = FALSE)
  }
}

## Clean missings

clean_missings <- function(x) {
  # To clean character variables (vectors)
  # 1) Trim whitespace; 2) blank -> NA; 3) common tokens -> NA
  if (is.character(x)) {
    x <- str_trim(x)
    x[x == ""] <- NA_character_
    x[tolower(x) %in% c("na", "n/a", "null", ".")] <- NA_character_
  }
  x
}

## Safely join datasets

safe_join <- function(df1, df2, by_vars, clean_keys = TRUE) {
  # Verify that the by_vars vector is specified correctly
  if (!is.null(names(by_vars)) && any(nzchar(names(by_vars)))) {
    left_keys <- names(by_vars)
    right_keys <- unname(by_vars)
  } else {
    left_keys <- by_vars
    right_keys <- by_vars
  }

  # Verify whether the key variables (for by_vars) exist in both datasets
  missing_left <- setdiff(left_keys, names(df1))
  missing_right <- setdiff(right_keys, names(df2))
  if (length(missing_left)) {
    stop("Missing join columns in df1: ", paste(missing_left, collapse = ", "))
  }
  if (length(missing_right)) {
    stop("Missing join columns in df2: ", paste(missing_right, collapse = ", "))
  }

  # Clean key variables to improve matching
  if (clean_keys) {
    for (k in left_keys) if (is.character(df1[[k]])) df1[[k]] <- str_squish(df1[[k]]) # Trimming spaces
    for (k in right_keys) if (is.character(df2[[k]])) df2[[k]] <- str_squish(df2[[k]])
  }

  # Identify whether the key's type mismatches- if different, coerce to character
  for (i in seq_along(left_keys)) {
    cl1 <- class(df1[[left_keys[i]]])[1]
    cl2 <- class(df2[[right_keys[i]]])[1]
    if (!identical(cl1, cl2)) {
      message(sprintf(
        "Coercing key '%s' (df1: %s) and '%s' (df2: %s) to character for join.", # To identify whether there was a type change
        left_keys[i], cl1, right_keys[i], cl2
      ))
      df1[[left_keys[i]]] <- as.character(df1[[left_keys[i]]])
      df2[[right_keys[i]]] <- as.character(df2[[right_keys[i]]])
    }
  }

  # Verify first whether there are matches between the datasets
  n_matched <- nrow(semi_join(df1, df2, by = by_vars))
  if (n_matched == 0) {
    # Show a few unmatched examples to debug
    example <- df1 |>
      anti_join(df2, by = by_vars) |>
      select(all_of(left_keys)) |>
      distinct() |>
      head(10)
    stop(
      "No rows matched on join keys. First few unmatched keys from df1:\n",
      paste(capture.output(print(example)), collapse = "\n")
    )
  }

  # Check duplicates in df2 keys to avoid row inflation
  dup_df2 <- df2 |>
    count(across(all_of(right_keys)), name = "n") |>
    filter(n > 1)
  if (nrow(dup_df2) > 0) {
    message(sprintf("Warning: df2 has %s duplicated key combination(s). This may duplicate rows in df1.", nrow(dup_df2)))
  }

  # Join
  n_before <- nrow(df1)
  out <- left_join(df1, df2, by = by_vars)

  # Verify whether there was a change in number of rows
  if (nrow(out) != n_before) {
    stop(sprintf(
      "Number of rows changed after join (%s -> %s). Review duplicate keys in df2.",
      n_before, nrow(out)
    ))
  }

  # Flag if ALL newly added columns are NA - There's probably something wrong
  new_cols <- setdiff(names(out), names(df1))
  if (length(new_cols)) {
    miss <- sapply(out[new_cols], function(x) mean(is.na(x)))
    all_na <- all(miss == 1)
    msg <- paste0(
      "New columns NA-fraction:\n",
      paste(sprintf("  %s: %.1f%%", names(miss), 100 * miss), collapse = "\n")
    )
    message(msg)
    if (all_na) {
      # Show a small sample of unmatched keys to help diagnose
      example <- df1 |>
        anti_join(df2, by = by_vars) |>
        select(all_of(left_keys)) |>
        distinct() |>
        head(10)
      message("All newly joined columns are 100% NA. Example unmatched keys from df1:")
      message(paste(capture.output(print(example)), collapse = "\n"))
    }
  } else {
    message("Note: No new columns were added by the join (column names overlapped).")
  }

  out
}

## Check column types

check_dates_pb <- function(df, cols = tidyselect::ends_with("_DATE"), accept_datetime = FALSE) {
  # resolve the columns to check
  date_cols <- df %>% select({{ cols }}) %>% names()
  if (!length(date_cols)) return(invisible(TRUE))
  
  types_tbl <- pointblank::scan_data(df) %>%
    select(col_name, col_type)
  
  # allowed types
  ok_types <- if (accept_datetime) c("date", "datetime") else "date"
  
  bad <- types_tbl %>%
    filter(col_name %in% date_cols, !(col_type %in% ok_types)) %>%
    pull(col_name)
  
  if (length(bad) == 1) {
    stop(paste0(bad, " is not a Date."))
  } else if (length(bad) > 1) {
    stop(paste0("These columns are not Dates: ", paste(bad, collapse = ", "), "."))
  }
  
  invisible(TRUE)
}


# 0.1 Get immigration related cases

mig_cases <- arrow::read_feather(paste0(output_dir, "/immigration_cases.feather"))

# 1. Gs_participant  -------

# 1.1 List all feather files that start with "gs_participant_"

participant_paths <- list.files(input_dir, pattern = "gs_participant_.*\\.feather$", full.names = TRUE)

# 1.2 Variables to subset
gs_participant_vars <- c(
  "PARTICIPANT_ID",
  "CASE_ID",
  "DISTRICT",
  "AGENCY",
  "SYS_INIT_DATE",
  "TYPE",
  "ROLE",
  "GENDER",
  "COUNTRY",
  "HOME_CITY",
  "HOME_COUNTY",
  "CRIM_HIST",
  "HOME_ZIPCODE"
)

## 1.3 Loop to read in feather files, create CASE_ID, select only specified variables AND SUBSET for immigration related cases and after 1996

dfs <- vector("list", length(participant_paths)) # Vector to collect all dataframes to bind later

for (i in seq_along(participant_paths)) {
  path <- participant_paths[i]
  message("Processing: ", basename(path))

  df <- arrow::read_feather(path)

  # Ensure required columns exist, else stop
  if (!all(c("DISTRICT", "CASEID", "ID") %in% colnames(df))) {
    stop("File ", basename(path), " is missing required columns (DISTRICT, CASEID, ID)")
  }

  df <- df |>
    mutate(
      CASE_ID = paste0(.data[["DISTRICT"]], .data[["CASEID"]]),  # Creating CASE_ID for case
      PARTICIPANT_ID = paste0(.data[["DISTRICT"]], .data[["CASEID"]], .data[["ID"]]) # Creating CASE_ID for participant
    ) |>
    select(PARTICIPANT_ID, CASE_ID, everything()) |>
    mutate(SYS_INIT_DATE = as.Date(SYS_INIT_DATE, format = "%d-%b-%Y")) |> # Converting to date format
    filter(!is.na(SYS_INIT_DATE), SYS_INIT_DATE >= as.Date("1996-01-01")) |> # Filter for cases after 1996
    filter(CASE_ID %in% pull(mig_cases, CASE_ID)) |> # Filter for migration related cases
    select(all_of(gs_participant_vars)) # Select variables

  dfs[[i]] <- df
}

# Combine into one dataframe
  
immigration_data <- bind_rows(dfs, .id = "source_file")

## 1.4 Renaming and filtering for defendants

immigration_data <-
  immigration_data |>
  rename(
    PARTICIPANT_TYPE = TYPE
  ) |>
  filter(str_detect(ROLE, "D")) |> # Right now including all types of criminal defendants
  filter(!ROLE %in% c("DJ", "VD")) |> # Dropping Juvenile defendants (DJ) for safety of minors data and VD that is not Defendant related
  mutate(N_PARTICIPANTS = n_distinct(PARTICIPANT_ID), .by = CASE_ID) |> # Number of participants per case
  select(!source_file) # Removing source file column

## 1.5 Cleaning missingess

immigration_data <- immigration_data |> 
  mutate(across(everything(), clean_missings))

## Checking the binding
# Set of rules to validate
rules <- validator(
  not_all_districts = n_distinct(DISTRICT) == 93, # There should be 93 districts - If not, bind may have failed
  PARTICIPANT_ID_missing = mean(is.na(PARTICIPANT_ID)) * 100 == 0, # PARTICIPANT_ID should not be missing
  CASE_ID_missing = mean(is.na(CASE_ID)) * 100 == 0, # CASE_ID should not be missing
  DISTRICT_missing = mean(is.na(DISTRICT)) * 100 == 0, # DISTRICT should not be missing
  SYS_INIT_DATE_missing = mean(is.na(SYS_INIT_DATE)) * 100 == 0, # CASE_ID should not be missing
  PARTICIPANT_TYPE_missing = mean(is.na(PARTICIPANT_TYPE)) * 100 == 0, # PARTICIPANT_TYPE should not be missing
  GENDER_all_missing = mean(is.na(GENDER)) * 100 < 100, # GENDER should not be all missing
  COUNTRY_all_missing = mean(is.na(COUNTRY)) * 100 < 100, # COUNTRY should not be all missing
  HOME_CITY_all_missing = mean(is.na(HOME_CITY)) * 100 < 100, # HOME_CITY should not be all missing
  HOME_COUNTY_all_missing = mean(is.na(HOME_COUNTY)) * 100 < 100, # HOME_COUNTY should not be all missing
  CRIM_HIST_all_missing = mean(is.na(CRIM_HIST)) * 100 < 100, # CRIM_HIST should not be all missing
  HOME_ZIPCODE_all_missing = mean(is.na(HOME_ZIPCODE)) * 100 < 100, # HOME_ZIPCODE should not be all missing
  date_discrepancy = in_range(immigration_data$SYS_INIT_DATE, min = as.Date("1996-01-01"), max = today()),
  # No records before 1996 and not in the future to avoid potential date typos or errors
  non_unique_rows = is_unique(PARTICIPANT_ID), # CASE_ID and PARTICIPANT_ID should uniquely identify each row
  n_participants_discrepancy = N_PARTICIPANTS == do_by(PARTICIPANT_ID, by = CASE_ID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants
)

# Run helper function to check and stop if any rules are broken
confront_stop(immigration_data, rules)

# Clean up
rm(list = setdiff(ls(), c("immigration_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "safe_join"))) # Keeping only what's needed
gc()

# 2. Gs_case -------

## 2.0 Function to read in feather files, create CASE_ID and select only specified variables

read_and_subset <- function(file_path, district_col, caseid_col, vars) {
  df <-
    arrow::read_feather(file_path) # Reading dataset

  df <-
    df |>
    mutate(
      CASE_ID = str_c({{ district_col }}, {{ caseid_col }}), # Creating CASE_ID
      .before = everything() # Order
    )

  df <-
    df |> select(all_of(vars)) # Select variables defined in vars vector

  return(df)
}

## 2.1 Run function to read, create CASE_ID and subset

# Variables to subset
gs_case_vars <- c(
  "CASE_ID",
  "STATUS",
  "LEAD_CHARGE",
  "UNIT",
  "TYPE",
  "BRANCH",
  "OFFENSE_FROM",
  "OFFENSE_TO",
  "CLOSE_DATE"
)

# Read and subset
gs_case <- read_and_subset(paste0(input_dir, "/gs_case.feather"), DISTRICT, ID, gs_case_vars)

## 2.2 Cleaning for join

gs_case <-
  gs_case |>
  rename(
    CASE_TYPE = TYPE,
    CASE_STATUS = STATUS
  ) |>
  mutate(
    # Title: leading number at start (e.g., "8" in "8 :1324(a)(2)")
    title = str_extract(LEAD_CHARGE, "^\\s*\\d+") |>
      str_remove("^0+") |> # remove leading zeros
      str_trim(),

    # Section raw: everything after the first colon that looks like a section
    # (digits/letters/parentheses), ignoring spaces
    section_raw = str_match(LEAD_CHARGE, ":\\s*([0-9A-Za-z()]+)")[, 2],

    # Trim any leading zeros in the section number (won't affect parentheses)
    section_raw = str_replace(section_raw, "^0+", ""),

    # If section already has parentheses, keep as-is.
    # If it doesn't (e.g., "1324a2"), convert the tail after the leading digits
    # so that letters/digit-runs become (a)(2), giving "1324(a)(2)".
    section_norm = if_else(
      !is.na(section_raw) & !str_detect(section_raw, "\\("),
      {
        # split into leading digits + the rest (e.g., "1324" + "a2")
        m <- str_match(section_raw, "^(\\d+)(.*)$")
        lead <- m[, 2]
        rest <- m[, 3]

        # wrap letters and digit groups in parentheses for the tail
        ifelse(
          is.na(rest) | rest == "",
          lead,
          paste0(
            lead,
            rest |>
              str_replace_all("([A-Za-z])", "(\\1)") |>
              str_replace_all("(\\d+)", "(\\1)")
          )
        )
      },
      section_raw
    ),
    LEAD_CHARGE = if_else( # Replacing charge codes with formatted statute codes
      !is.na(title) & !is.na(section_norm),
      paste0(title, " U.S.C. § ", section_norm),
      LEAD_CHARGE # To replace with * in the cases it was redacted
    )
  ) |>
  select(!c(title, section_raw, section_norm)) # Removing temporary variables

## 2.3 Safely join to immigration_data

join_by <- c("CASE_ID")

immigration_data <- safe_join(immigration_data, gs_case, join_by)

## Checking the merge

# Cleaning missingess

immigration_data <- immigration_data |> 
  mutate(across(everything(), clean_missings))

# Set of rules to validate
rules <- validator(
  CASE_STATUS_missing = mean(is.na(CASE_STATUS)) * 100 == 0, # CASE_STATUS should not be missing
  LEAD_CHARGE_missing = mean(is.na(LEAD_CHARGE)) * 100 == 0, # LEAD_CHARGE should not be missing
  BRANCH_missing = mean(is.na(BRANCH)) * 100 == 0, # BRANCH should not be missing
  UNIT_all_missing = mean(is.na(UNIT)) * 100 < 100, # UNIT should not be all missing
  CASE_TYPE_all_missing = mean(is.na(CASE_TYPE)) * 100 < 100, # CASE_TYPE should not be all missing
  OFFENSE_FROM_all_missing = mean(is.na(OFFENSE_FROM)) * 100 < 100, # OFFENSE_FROM should not be all missing
  CLOSE_DATE_all_missing = mean(is.na(CLOSE_DATE)) * 100 < 100, # CLOSE_DATE should not be all missing
  non_unique_rows = is_unique(CASE_ID, PARTICIPANT_ID), # CASE_ID and PARTICIPANT_ID should uniquely identify each row
  n_participants_discrepancy = N_PARTICIPANTS == do_by(PARTICIPANT_ID, by = CASE_ID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants
)

# Run helper function to check and stop if any rules are broken
confront_stop(immigration_data, rules)

# Clean up
rm(list = setdiff(ls(), c("immigration_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset", "safe_join"))) # Keeping only what's needed
gc()


# 3. Gs_sentence -------
## 3.1 Run function to read, create CASE_ID and subset

### Variables to subset
gs_sentence_vars <- c("CASE_ID",
  "ID" = "PARTID",
  "SENT_DATE",
  "FINE",
  "INCAR_TYPE",
  "INCAR_DAYS",
  "INCAR_MONTHS",
  "INCAR_YEARS"
)

### Read and subset

gs_sentence <-
  read_and_subset(paste0(input_dir, "/gs_sentence.feather"), DISTRICT, CASEID, gs_sentence_vars) |>
  mutate(PARTICIPANT_ID = paste0(CASE_ID, ID)) |> # Creating PARTICIPANT_ID for join
  select(-c(ID, CASE_ID)) # Dropping ID to avoid confusion

## 3.2 Safely join to immigration_data

join_by <- c("PARTICIPANT_ID")
immigration_data <- safe_join(immigration_data, gs_sentence, join_by)

## Checking the merge

# Cleaning missingess
immigration_data <- immigration_data |> 
  mutate(across(everything(), clean_missings))

# Set of rules to validate
rules <- validator(
  SENT_DATE_all_missing = mean(is.na(FINE)) * 100 < 100, # SENT_DATE should not be all missing
  FINE_all_missing = mean(is.na(FINE)) * 100 < 100, # FINE should not be all missing
  INCAR_DAYS_all_missing = mean(is.na(INCAR_DAYS)) * 100 < 100, # CASE_TYPE should not be all missing
  INCAR_MONTHS_all_missing = mean(is.na(INCAR_MONTHS)) * 100 < 100, # INCAR_MONTHS should not be all missing
  INCAR_YEARS_all_missing = mean(is.na(INCAR_YEARS)) * 100 < 100, # INCAR_YEARS should not be all missing
  INCAR_TYPE_all_missing = mean(is.na(INCAR_TYPE)) * 100 < 100, # INCAR_TYPE should not be all missing
  non_unique_rows = is_unique(CASE_ID, PARTICIPANT_ID), # CASE_ID and PARTICIPANT_ID should uniquely identify each row
  n_participants_discrepancy = N_PARTICIPANTS == do_by(PARTICIPANT_ID, by = CASE_ID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants
)

# Run helper function to check and stop if any rules are broken
confront_stop(immigration_data, rules)

# Clean up
rm(list = setdiff(ls(), c("immigration_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset", "safe_join"))) # Keeping only what's needed
gc()

# 4. Get gs_case_prog_cat  -------
## 4.1 Run function to read, create CASE_ID and subset

# Variables to subset

gs_case_prog_cat_vars <- c(
  "CASE_ID",
  "ID",
  "PROG_CAT"
)

# Read and subset
gs_case_prog_cat <-
  read_and_subset(paste0(input_dir, "/gs_case_prog_cat.feather"), DISTRICT, CASEID, gs_case_prog_cat_vars) |>
  mutate(PARTICIPANT_ID = paste0(CASE_ID, ID)) |> # Creating PARTICIPANT_ID for join
  select(-c(ID, CASE_ID)) # Dropping ID to avoid confusion

## 4.2 Safely join to immigration_data

join_by <- c("PARTICIPANT_ID")
immigration_data <-
  safe_join(immigration_data, gs_case_prog_cat, join_by)

## Checking the merge
# Cleaning missingess
immigration_data <- immigration_data |> 
  mutate(across(everything(), clean_missings))

# Set of rules to validate
rules <-
  validator(
    PROG_CAT_all_missing = mean(is.na(PROG_CAT)) * 100 < 100, # PROG_CAT should not be all missing
    non_unique_rows = is_unique(CASE_ID, PARTICIPANT_ID), # CASE_ID and PARTICIPANT_ID should uniquely identify each row
    n_participants_discrepancy = N_PARTICIPANTS == do_by(PARTICIPANT_ID, by = CASE_ID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants
  )

# Run helper function to check and stop if any rules are broken
confront_stop(immigration_data, rules)

# 5. Get list of charges  -------
## Safely join with immigration charges vector

mig_cases <-
  mig_cases |>
  select(CASE_ID, CHARGES_LIST)

join_by <- c("CASE_ID")
immigration_data <-
  safe_join(immigration_data, mig_cases, join_by)

# Clean up
rm(list = setdiff(ls(), c("immigration_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset", "safe_join"))) # Keeping only what's needed
gc()

# 6. Get gs_count  -------
## 6.1 Run function to read, create CASE_ID and subset

# Variables to subset
gs_count_vars <- c(
  "CASE_ID",
  "CRTHISID",
  "INSTID",
  "ID",
  "CATEGORY",
  "PENT_PROV"
)

# Read and subset
gs_count <- read_and_subset(paste0(input_dir, "/gs_count.feather"), DISTRICT, CASEID, gs_count_vars) |>
  mutate(PARTICIPANT_ID = paste0(CASE_ID, ID)) |> # Creating PARTICIPANT_ID for join
  filter(CASE_ID %in% pull(immigration_data, CASE_ID)) |> # Filter for migration related cases for speed
  select(-c(ID, CASE_ID)) # Dropping ID to avoid confusion

## 6.2 Cleaning PENT_PROV
gs_count <- gs_count |>
  mutate(
    # Title: leading number at start (e.g., "8" in "8 :1324(a)(2)")
    title = str_extract(PENT_PROV, "^\\s*\\d+") |>
      str_remove("^0+") |> # remove leading zeros
      str_trim(),

    # Section raw: everything after the first colon that looks like a section
    # (digits/letters/parentheses), ignoring spaces
    section_raw = str_match(PENT_PROV, ":\\s*([0-9A-Za-z()]+)")[, 2],

    # Trim any leading zeros in the section number (won't affect parentheses)
    section_raw = str_replace(section_raw, "^0+", ""),

    # If section already has parentheses, keep as-is.
    # If it doesn't (e.g., "1324a2"), convert the tail after the leading digits
    # so that letters/digit-runs become (a)(2), giving "1324(a)(2)".
    section_norm = if_else(
      !is.na(section_raw) & !str_detect(section_raw, "\\("),
      {
        # split into leading digits + the rest (e.g., "1324" + "a2")
        m <- str_match(section_raw, "^(\\d+)(.*)$")
        lead <- m[, 2]
        rest <- m[, 3]

        # wrap letters and digit groups in parentheses for the tail
        ifelse(
          is.na(rest) | rest == "",
          lead,
          paste0(
            lead,
            rest |>
              str_replace_all("([A-Za-z])", "(\\1)") |>
              str_replace_all("(\\d+)", "(\\1)")
          )
        )
      },
      section_raw
    ),
    PENT_PROV = if_else( # Replacing charge codes with formatted statute codes
      !is.na(title) & !is.na(section_norm),
      paste0(title, " U.S.C. § ", section_norm),
      PENT_PROV # To replace with * in the cases it was redacted
    )
  ) |>
  select(!c(title, section_raw, section_norm)) # Removing temporary variables

## 6.3 Collapsing to CASE_ID-ID (participant) level

gs_count <-
  gs_count |>
  group_by(PARTICIPANT_ID) |>
  summarise(
    N_CRTHIS = n_distinct(na.omit(CRTHISID)), # Count unique values (#)
    N_INST = n_distinct(na.omit(INSTID)), # Count unique values
    CATEGORY = paste(unique(na.omit(CATEGORY)), collapse = ", "), # Concatenate categories if multiple per CASE_ID-ID
    PENT_PROV = paste(unique(na.omit(PENT_PROV)), collapse = ", "), # Concatenate charges if multiple per CASE_ID-ID
    .groups = "drop"
  )

## 6.4 Safely join to immigration_data

join_by <- c("PARTICIPANT_ID")
immigration_data <- safe_join(immigration_data, gs_count, join_by)

## Checking the merge
# Cleaning missingess
immigration_data <- immigration_data |> 
  mutate(across(everything(), clean_missings))

# Set of rules to validate
rules <- validator(
  N_CRTHIS_all_missing = mean(is.na(N_CRTHIS)) * 100 < 100, # N_CRTHIS should not be all missing
  N_INST_all_missing = mean(is.na(N_INST)) * 100 < 100, # N_INST should not be all missing
  CATEGORY_all_missing = mean(is.na(CATEGORY)) * 100 < 100, # CATEGORY should not be all missing
  # PENT_PROV_all_missing = mean(is.na(PENT_PROV)) * 100 < 100, # PENT_PROV should not be all missing
  non_unique_rows = is_unique(CASE_ID, PARTICIPANT_ID), # CASE_ID and PARTICIPANT_ID should uniquely identify each row
  n_participants_discrepancy = N_PARTICIPANTS == do_by(PARTICIPANT_ID, by = CASE_ID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants
)

# Run helper function to check and stop if any rules are broken
confront_stop(immigration_data, rules)

## 6.5 Clean up

rm(list = setdiff(ls(), c("immigration_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset", "safe_join"))) # Keeping only what's needed
gc()


# 7. Get gs_court_hist -------
## 7.1 Run function to read, create CASE_ID and subset

# Variables to subset

gs_court_hist_vars <- c(
  "CASE_ID",
  "COURT",
  "COURT_NUMBER",
  "ID",
  "COURT_LOC" = "LOCATION",
  "DISPOSITION",
  "DISP_REASON1",
  "DISP_REASON2",
  "DISP_REASON3",
  "DISP_DATE"
)

# Read and subset
gs_court_hist <-
  read_and_subset(paste0(input_dir, "/gs_court_hist.feather"), DISTRICT, CASEID, gs_court_hist_vars) |>
  mutate(PARTICIPANT_ID = paste0(CASE_ID, ID)) |> # Creating PARTICIPANT_ID for join
  select(-c(ID, CASE_ID)) # Dropping ID to avoid confusion


## 7.2 Safely join to immigration_data

join_by <- c("PARTICIPANT_ID")
immigration_data <-
  safe_join(immigration_data, gs_court_hist, join_by)

## Checking the merge
# Cleaning missingess
immigration_data <- immigration_data |> 
  mutate(across(everything(), clean_missings))

# Set of rules to validate
rules <-
  validator(
    COURT_all_missing = mean(is.na(COURT)) * 100 < 100, # COURT should not be all missing
    COURT_NUMBER_all_missing = mean(is.na(COURT_NUMBER)) * 100 < 100, # COURT_NUMBER should not be all missing
    COURT_LOC_all_missing = mean(is.na(COURT_LOC)) * 100 < 100, # COURT_LOC should not be all missing
    DISPOSITION_all_missing = mean(is.na(DISPOSITION)) * 100 < 100, # DISPOSITION should not be all missing
    DISP_REASON1_all_missing = mean(is.na(DISP_REASON1)) * 100 < 100, # DISP_REASON1 should not be all missing
    # DISP_REASON2_all_missing = mean(is.na(DISP_REASON2)) * 100 < 100, # DISP_REASON2 can be all missing but keeping if a case in the future includes it
    # DISP_REASON3_all_missing = mean(is.na(DISP_REASON3)) * 100 < 100, # DISP_REASON3 can be all missing but keeping if a case in the future includes it
    DISP_DATE_all_missing = mean(is.na(DISP_DATE)) * 100 < 100, # DISP_DATE should not be all missing
    non_unique_rows = is_unique(CASE_ID, PARTICIPANT_ID), # CASE_ID and PARTICIPANT_ID should uniquely identify each row
    n_participants_discrepancy = N_PARTICIPANTS == do_by(PARTICIPANT_ID, by = CASE_ID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants
  )

# Run helper function to check and stop if any rules are broken
confront_stop(immigration_data, rules)

## 7.3 Clean up

rm(list = setdiff(ls(), c("immigration_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset", "safe_join"))) # Keeping only what's needed
gc()


# 8. Get gs_defend_stat -------
## 8.1 Run function to read, create CASE_ID and subset TODO - IDENTIFY LEVEL OF ANALYSIS TOO DEBUG MERGE BREAKAGE

### Variables to subset

gs_defend_stat_vars <- c(
  "CASE_ID",
  "PARTID",
  "STATUS_TYPE" = "TYPE",
  "CUSTODY_LOC",
  "DETEN_REASON",
  "START_DATE",
  "END_DATE",
  "TERM_REASON"
)

### Read and subset

gs_defend_stat <- read_and_subset(paste0(input_dir, "/gs_defend_stat.feather"), DISTRICT, CASEID, gs_defend_stat_vars) |>
  mutate(PARTICIPANT_ID = paste0(CASE_ID, PARTID)) |> # Creating PARTICIPANT_ID for join - in this dataset, according to the codebook, ID is the participant status
  filter(CASE_ID %in% pull(mig_cases, CASE_ID)) # Filter for migration related cases for speed

## 8.2 Pivot to create participant-level dataset

gs_defend_stat <-
  gs_defend_stat |>
  group_by(PARTICIPANT_ID) |>
  arrange(START_DATE, END_DATE, .by_group = TRUE) |>
  # keep only the first and last row per participant (if we keep all it's at least 80 variables, we'll keep for now the first and last status and an indicator of the amount of statuses)
  mutate(
    row = row_number(), # First = 1
    N_STATUS = max(row)
  ) |> # Will show the last row per participant
  filter(row == 1 | row == N_STATUS) |> # Keeping first and last row
  mutate(instance = case_when(
    row == 1 ~ "FIRST",
    row == N_STATUS ~ "LAST"
  )) |> # Label them to create variables after pivot
  ungroup() |>
  select(
    PARTICIPANT_ID, N_STATUS, instance, STATUS_TYPE, START_DATE, END_DATE,
    CUSTODY_LOC, DETEN_REASON, TERM_REASON
  ) |>
  pivot_wider(
    names_from  = instance,
    values_from = c(STATUS_TYPE, START_DATE, END_DATE, CUSTODY_LOC, DETEN_REASON, TERM_REASON),
    names_glue  = "{.value}_{instance}" # e.g. TYPE_first, START_DATE_last
  ) |>
  select(PARTICIPANT_ID, N_STATUS, ends_with("_FIRST"), ends_with("_LAST"))

## 8.3 Safely join to immigration_data

join_by <- c("PARTICIPANT_ID")
immigration_data <-
  safe_join(immigration_data, gs_defend_stat, join_by)

### Checking the merge
### Cleaning missingess
immigration_data <- immigration_data |> 
  mutate(across(everything(), clean_missings))

### Set of rules to validate
rules <-
  validator(
    N_STATUS_all_missing = mean(is.na(N_STATUS)) * 100 < 100, # N_STATUS should not be all missing
    STATUS_TYPE_FIRST_all_missing = mean(is.na(STATUS_TYPE_FIRST)) * 100 < 100, # STATUS_TYPE_FIRST should not be all missing
    START_DATE_FIRST_all_missing = mean(is.na(START_DATE_FIRST)) * 100 < 100, # START_DATE_FIRST should not be all missing
    END_DATE_FIRST_all_missing = mean(is.na(END_DATE_FIRST)) * 100 < 100, # END_DATE_FIRST should not be all missing
    CUSTODY_LOC_FIRST_all_missing = mean(is.na(CUSTODY_LOC_FIRST)) * 100 < 100, # CUSTODY_LOC_FIRST should not be all missing
    DETEN_REASON_FIRST_all_missing = mean(is.na(DETEN_REASON_FIRST)) * 100 < 100, # DETEN_REASON_FIRST should not be all missing
    TERM_REASON_FIRST_all_missing = mean(is.na(TERM_REASON_FIRST)) * 100 < 100, # TERM_REASON_FIRST should not be all missing
    STATUS_TYPE_LAST_all_missing = mean(is.na(STATUS_TYPE_LAST)) * 100 < 100, # STATUS_TYPE_LAST should not be all missing
    START_DATE_LAST_all_missing = mean(is.na(START_DATE_LAST)) * 100 < 100, # START_DATE_LAST should not be all missing
    END_DATE_LAST_all_missing = mean(is.na(END_DATE_LAST)) * 100 < 100, # END_DATE_LAST should not be all missing
    CUSTODY_LOC_LAST_all_missing = mean(is.na(CUSTODY_LOC_LAST)) * 100 < 100, # CUSTODY_LOC_LAST should not be all missing
    DETEN_REASON_LAST_all_missing = mean(is.na(DETEN_REASON_LAST)) * 100 < 100, # DETEN_REASON_LAST should not be all missing
    TERM_REASON_LAST_all_missing = mean(is.na(TERM_REASON_LAST)) * 100 < 100, # TERM_REASON_LAST should not be all missing
    non_unique_rows = is_unique(CASE_ID, PARTICIPANT_ID), # CASE_ID and PARTICIPANT_ID should uniquely identify each row
    n_participants_discrepancy = N_PARTICIPANTS == do_by(PARTICIPANT_ID, by = CASE_ID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants
  )

###  Run helper function to check and stop if any rules are broken
confront_stop(immigration_data, rules)

## 8.4 Clean up

rm(list = setdiff(ls(), c("immigration_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset", "safe_join"))) # Keeping only what's needed
gc()

# 9. Get gs_court_judge -------
## 9.1 Run function to read, create CASE_ID and subset

# Variables to subset

gs_court_judge_vars <- c(
  "CASE_ID",
  "ID",
  "CRTHISID",
  "JUDGEID",
  "DISTRICT",
  "START_DATE"
)

# Read and subset

gs_court_judge <- read_and_subset(paste0(input_dir, "/gs_court_judge.feather"), DISTRICT, CASEID, gs_court_judge_vars) |>
  mutate(
    PARTICIPANT_ID = paste0(CASE_ID, ID), # Creating PARTICIPANT_ID for join
    UNIQUE_JUDGEID = if_else(is.na(DISTRICT) | is.na(JUDGEID), # Create unique judge ID (District+ID)
      NA_character_, str_c(DISTRICT, JUDGEID)
    )
  )

## 9.2 Clean and get labels before merging
# Read and clean table_gs_judge: lookup table with judge name and type code

table_gs_judge <- read_feather(file.path(input_dir, "table_gs_judge.feather")) |>
  mutate(
    across(
      where(is.character), # Clean missings before concatenating
      ~ .x |>
        str_trim() |>
        na_if("NA") |>
        na_if("")
    ),
    UNIQUE_JUDGEID = if_else(is.na(District) | is.na(ID), # Create unique judge ID (District+ID)
      NA_character_, str_c(District, ID)
    ),
    JUDGE_NAME = if_else(is.na(FirstName) & is.na(LastName), # Create JUDGE_NAME from First and Last Name
      NA_character_, str_c(FirstName, LastName, sep = " ")
    ),
    JUDGE_TYPE = if_else(is.na(District) | is.na(Type), # Creating unique type code because they differ across districts
      NA_character_, str_c(District, Type)
    )
  ) |>
  select(UNIQUE_JUDGEID, JUDGE_NAME, JUDGE_TYPE)

# Read table_gs_judge_type: lookup table with judge type label

table_gs_judge_type <- read_feather(file.path(input_dir, "table_gs_judge_type.feather")) |>
  mutate(
    across(
      where(is.character), # Clean missings before concatenating
      ~ .x |>
        str_trim() |>
        na_if("NA") |>
        na_if("")
    ),
    JUDGE_TYPE = if_else(is.na(District) | is.na(Code), # Creating unique type code because they differ across districts
      NA_character_, str_c(District, Code)
    )
  ) |>
  select(JUDGE_TYPE_LABEL = Description, JUDGE_TYPE)

# Join two lookup tables to get JUDGE_NAME and JUDGE_TYPE_LABEL
join_by <- c("JUDGE_TYPE")
table_gs_judge <-
  safe_join(table_gs_judge, table_gs_judge_type, join_by) |>
  select(-JUDGE_TYPE) # No need to keep the code

# Join to gs_court_judge
join_by <- c("UNIQUE_JUDGEID")
gs_court_judge <-
  safe_join(gs_court_judge, table_gs_judge, join_by)

## 9.3 Transform at the participant level

gs_court_judge <-
  gs_court_judge |>
  group_by(PARTICIPANT_ID) |>
  arrange(START_DATE, .by_group = TRUE) |>
  summarise(
    N_JUDGES = n_distinct(UNIQUE_JUDGEID, na.rm = TRUE), # Count unique judges (#)
    JUDGE_LIST = paste(unique(na.omit(JUDGE_NAME)), collapse = ", "), # Concatenate JUDGE_NAMES if multiple per
    JUDGE_NAME_FIRST = JUDGE_NAME[1], # First judge name (There are participants with more than 40 judges, we will include furteher info for the first judge and leave all names concatenaded in JUDGE_LIST)
    JUDGE_TYPE_FIRST = JUDGE_TYPE_LABEL[1], # First judge type
    .groups = "drop"
  )

## 9.4 Safely join to immigration_data

join_by <- c("PARTICIPANT_ID")
immigration_data <- safe_join(immigration_data, gs_court_judge, join_by)

### Checking the merge
### Cleaning missingess
immigration_data <- immigration_data |> 
  mutate(across(everything(), clean_missings))

### Set of rules to validate
rules <-
  validator(
    N_JUDGES_all_missing = mean(is.na(N_JUDGES)) * 100 < 100, # N_JUDGES should not be all missing
    JUDGE_LIST_all_missing = mean(is.na(JUDGE_LIST)) * 100 < 100, # JUDGE_LIST should not be all missing
    JUDGE_NAME_FIRST_all_missing = mean(is.na(JUDGE_NAME_FIRST)) * 100 < 100, # JUDGE_NAME_FIRST should not be all missing
    JUDGE_TYPE_FIRST_all_missing = mean(is.na(JUDGE_TYPE_FIRST)) * 100 < 100, # JUDGE_TYPE_FIRST should not be all missing
    non_unique_rows = is_unique(CASE_ID, PARTICIPANT_ID), # CASE_ID and PARTICIPANT_ID should uniquely identify each row
    n_participants_discrepancy = N_PARTICIPANTS == do_by(PARTICIPANT_ID, by = CASE_ID, fun = function(x) length(unique(x))) # Verify dataset is at participant-level - No more rows than participants
  )

###  Run helper function to check and stop if any rules are broken
confront_stop(immigration_data, rules)

## 9.5 Clean up

rm(list = setdiff(ls(), c("immigration_data", "mig_cases", "input_dir", "output_dir", "confront_stop", "clean_missings", "read_and_subset", "safe_join"))) # Keeping only what's needed
gc()

# 10. Reviewing missingness  -------

missing_summary <- immigration_data |>
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

# 12. Save final dataset -----

arrow::write_feather(immigration_data, paste0(output_dir, "/immigration_defendant_level.feather"))
