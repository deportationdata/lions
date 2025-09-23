library(tidyverse)
library(readr)
library(lubridate)
library(arrow)
library(tidylog)
library(stringr)
library(purrr)
library(dplyr)
library(tibble)
library(fs) # To suppress messages
library(tools) # for file_path_sans_ext()

# 1. Helpers

## Function to parse a chunk of text corresponding to a table layout
parse_table_chunk <- function(chunk, disk) { # Each disk folder (currently 28) has a README.txt file and other gs_*.txt files (27 is an exception***)
  header <- chunk[1]
  table <- str_extract(header, "^GS_[A-Z0-9_]+")

  # –– main regexp ––#
  # 1  column name      e.g. STATE_COMP_RECVD
  # 2  NOT NULL (optional)
  # 3  type-token        e.g. NUMBER(12,2) | FLOAT(*) | DATE | VARCHAR2(30)
  # 4  begin position
  # 5  end   position
  rgx <- "^\\s*([A-Z0-9_]+)\\s+(NOT NULL)?\\s*([A-Z0-9_]+(?:\\([^)]*\\))?)?\\s*\\((\\d+):(\\d+)\\)"

  tibble(raw = chunk[-1]) |> # Skipping header line
    filter(str_detect(raw, "\\(\\d+:\\d+\\)")) |> # lines that *have* positions
    mutate(m = str_match(raw, rgx)) |>
    filter(!is.na(m[, 2])) |> # keep only successfully-matched rows - drops lines that didn’t match the regex (NA rows)
    transmute(
      disk,
      table,
      col_names = m[, 2],
      nullable = is.na(m[, 3]), # TRUE  ⇢ column *can* be NULL
      raw_type = m[, 4],
      begin = as.integer(m[, 5]),
      end = as.integer(m[, 6])
    ) |>
    mutate(
      col_type = case_when(
        str_detect(raw_type, "^(VARCHAR2|CHAR|CLOB|BLOB|LONG)") ~ "c",
        str_detect(raw_type, "^NUMBER\\([^,]+,[^,]+\\)") ~ "d", # NUMBER(p,s)  → double
        str_detect(raw_type, "^NUMBER") ~ "n", # numeric / general
        str_detect(raw_type, "^(INT|INTEGER)$") ~ "i",
        str_detect(raw_type, "^FLOAT") ~ "d",
        str_detect(raw_type, "^DATE$") ~ "D",
        str_detect(raw_type, "^TIMESTAMP") ~ "T",
        TRUE ~ "?" # anything else → guess
      )
    )
}

## Function to check column types after reading
check_col_types <- function(df, types_str, date_cols = character()) {
  # expected codes per column, aligned to df's columns
  exp_code <- strsplit(types_str, "", fixed = TRUE)[[1]]
  if (length(exp_code) != ncol(df)) {
    stop(
      "Column count mismatch: layout has ", length(exp_code),
      " columns but data has ", ncol(df), "."
    )
  }
  names(exp_code) <- names(df)

  # Map codes to expected R classes; dates override to "Date"
  code_to_class <- c(
    c = "character",
    n = "numeric",
    i = "integer",
    d = "numeric", # treat double as numeric
    l = "logical",
    D = "Date",
    T = "POSIXct"
  )
  # Start from codes, then force expected "Date" for date columns
  exp_class <- unname(code_to_class[exp_code])
  names(exp_class) <- names(df)
  exp_class[names(df) %in% date_cols] <- "Date"

  # Actual primary class per column (normalize a few common cases)
  act_class <- vapply(df, function(x) {
    if (is.factor(x)) {
      "character"
    } else if (inherits(x, "integer64")) {
      "numeric"
    } else {
      class(x)[1]
    }
  }, character(1))

  # Allow integers where "numeric" is expected
  bad <- names(df)[mapply(function(exp, act) {
    if (is.na(exp)) {
      TRUE
    } # unknown expected code -> fail
    else if (exp == "numeric") {
      !(act %in% c("numeric", "integer"))
    } else {
      exp != act
    }
  }, exp_class, act_class)]

  if (length(bad) == 1) {
    stop(paste0(bad, " is not the correct type."))
  } else if (length(bad) > 1) {
    stop(paste0(
      "These columns are not the correct type: ",
      paste(bad, collapse = ", "), "."
    ))
  } else {
    message("All column types are the expected type.")
  }

  invisible(TRUE)
}

## Function to clean missingness of read files

clean_missings <- function(x) {
  # To clean character variables (vectors)
  if (is.character(x)) {
    # Some tables were creating errors - normalize encoding to valid UTF-8 (replace bad bytes with "?")
    x <- iconv(x, from = "", to = "UTF-8", sub = "?")

    # Trim whitespace safely
    x <- stringr::str_trim(x)

    # Empty -> NA
    x[x == ""] <- NA_character_

    # Transform common tokens (case-insensitive) to NA
    lx <- tolower(x) #    After iconv to UTF-8, tolower() won't error.
    x[lx %in% c("na", "n/a", "null", ".")] <- NA_character_
  }
  x
}

## Function to extract the file names from a README block
extract_files <- \(txt, disk) {
  tibble(line = txt) |>
    # filter to lines that have .txt in them
    filter(str_detect(line, "\\.txt$")) |>
    transmute(
      disk = disk,
      # extract filename from the line's text with .txt at end
      file = str_trim(str_extract(line, "[^\\s]+\\.txt$"))
    )
}

## Function to derive table name from file name
derive_table <- \(f) {
  paste0(
    "GS_",
    f |>
      str_remove("^gs_") |> # strip prefix
      str_remove("\\.txt$") |> # drop .txt
      str_remove("_[A-Z]{2,4}$") |> # remove district suffix
      str_to_upper()
  )
}

## Function to read in all of the README files in any DISC** folder
readmes <-
  dir(
    "inputs",
    pattern = "README\\.txt$",
    recursive = TRUE, # Looks in subdirectories too (because we have previously zipped folders)
    full.names = TRUE # Full file paths instead of just filenames
  ) |>
  # Set the name of the vector to make mapping easier
  # Set to the name of the directory
  set_names(\(p) basename(dirname(p))) |> # For each file path p, dirname(p) returns the directory path containing the file
  map(readLines, warn = FALSE)

## Getting the mapping between files and tables; some tables are in more than one file
files_mapping <-
  imap_dfr(readmes, extract_files) |>
  mutate(table = map_chr(file, derive_table))

# Getting the layout for each table which is used in the fwf read in command
layouts <-
  map2_dfr(readmes, names(readmes), \(lines, disk) {
    # start of layout declaration is the table name starting wiht GS_
    starts <- which(str_detect(lines, "^GS_[A-Z0-9_]+\\s+-\\s+\\d+"))
    ends <- c(starts[-1] - 1L, length(lines))

    map2_dfr(starts, ends, \(i, j) parse_table_chunk(lines[i:j], disk))
  }) |>
  full_join(files_mapping) |>
  mutate(is_lookup_table = is.na(col_names))

# Removing lookup tables which have a different format
layouts_data <- layouts |> filter(!is_lookup_table) # Dropping disk 28

# Getting layouts for lookup tables only
layouts_lookup <-
  layouts |>
  # global_LIONS is not a lookup table nor a data table; ignore completely
  filter(is_lookup_table & file != "global_LIONS.txt") |>
  select(disk, table, file)

# Create a list of file paths for each disk and file
layout_by_file <- layouts_data |>
  group_by(file_path = file.path("inputs/unzipped", disk, file)) |>
  summarise(
    fwf = list(fwf_positions(begin, end, col_names)),
    col_types = paste0(replace(col_type, col_type %in% c("D"), "c"), collapse = ""), # Compact readr type string - If these are Dates, we will take them as character and process later (they have Oracle-style DD-MON-YYYY format)
    date_cols = list(col_names[col_type == "D"]), # Identify date columns for post-processing
    .groups = "drop"
  )

# Creating table to walk
layout_tbl <- split(layout_by_file, layout_by_file$file_path)
# layout_tbl <- layout_tbl[1:2]# Test

# Creating directory to save feather files

output_dir <- "outputs"

fs::dir_create(output_dir)

# Loop through each table and read the corresponding .txt file, then save it as a feather file

walk2(layout_tbl, names(layout_tbl), function(tbl, path) {
  layout <- tbl[["fwf"]][[1]]
  types_str <- tbl[["col_types"]][[1]]
  date_cols <- tbl[["date_cols"]][[1]] # Bring vector of date columns

  if (file_exists(path)) {
    tryCatch(
      {
        # Extract the base name to save it
        base_name <- file_path_sans_ext(basename(path))
        feather_path <- file.path(output_dir, paste0(base_name, ".feather"))

        # Reading .txt table
        df <-
          read_fwf(path,
            col_positions = layout,
            col_types = types_str
          )

        # Clean missingness in character
        df <- tryCatch(
          {
            df |> mutate(across(everything(), clean_missings))
          },
          error = function(e) {
            message("↩︎ Skipping clean_missings (leaving df unchanged): ", conditionMessage(e))
            df # Return original df so the pipeline continues
          }
        )

        # Convert date columns to Date type
        if (length(date_cols)) {
          df <- df |>
            mutate(across(
              all_of(date_cols),
              ~ as.Date(., format = "%d-%b-%Y")
            ))
        }
        # Check column types
        check_col_types(df, types_str, date_cols)

        # Save as feather file
        write_feather(df, feather_path)

        message("✔ Saved: ", feather_path)
      },
      error = function(e) {
        stop("✘ Error processing: ", path, " — ", e$message)
      }
    )
  } else {
    message("⚠ File not found: ", path)
  }
})
