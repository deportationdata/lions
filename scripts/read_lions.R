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
library(tools)  # for file_path_sans_ext()

parse_table_chunk <- function(chunk, disk) { # Each disk folder (currently 28) has a README.txt file and other gs_*.txt files (27 is an exception***)
  header <- chunk[1]
  table <- str_extract(header, "^GS_[A-Z0-9_]+")

  #–– main regexp ––#
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


# grab the file names from a README block
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

# read in all of the README files in any DISC** folder
readmes <-
  dir(
    "inputs",
    pattern = "README\\.txt$",
    recursive = TRUE, # looks in subdirectories too
    full.names = TRUE # full file paths instead of just filenames
  ) |>
  # set the name of the vector to make mapping easier
  # set to the name of the directory
  set_names(\(p) basename(dirname(p))) |> # For each file path p, dirname(p) returns the directory path containing the file
  map(readLines, warn = FALSE)

# get the mapping between files and tables; some tables are in more than one file
files_mapping <-
  imap_dfr(readmes, extract_files) |>
  mutate(table = map_chr(file, derive_table))

# get the layout for each table which is used in the fwf read in command
layouts <-
  map2_dfr(readmes, names(readmes), \(lines, disk) {
    # start of layout declaration is the table name starting wiht GS_
    starts <- which(str_detect(lines, "^GS_[A-Z0-9_]+\\s+-\\s+\\d+"))
    ends <- c(starts[-1] - 1L, length(lines)) 

    map2_dfr(starts, ends, \(i, j) parse_table_chunk(lines[i:j], disk))
  }) |>
  full_join(files_mapping) |>
  mutate(is_lookup_table = is.na(col_names))

# remove lookup tables which have a different format
layouts_data <- layouts |> filter(!is_lookup_table) # Dropping disk 28

# get layouts for lookup tables only
layouts_lookup <-
  layouts |>
  # global_LIONS is not a lookup table nor a data table; ignore completely 
  filter(is_lookup_table & file != "global_LIONS.txt") |>
  select(disk, table, file)

# Create a list of file paths for each disk and file
layout_by_file <- layouts_data |>
  group_by(file_path = file.path("inputs", disk, file)) |>
  summarise(
    fwf = list(fwf_positions(begin, end, col_names)),
    .groups = "drop"
  )

# Creating table to walk

layout_tbl <- split(layout_by_file, layout_by_file$file_path)

# Creating directory to save feather files

fs::dir_create("outputs")

# Loop through each table and read the corresponding .txt file, then save it as a feather file

walk2(layout_tbl, names(layout_tbl), function(tbl, path) {
  layout <- tbl[["fwf"]][[1]]
  
  if (file_exists(path)) {
    tryCatch({
      
      # Extract the base name to save it
      base_name <- file_path_sans_ext(basename(path))
      feather_path <- file.path(output_dir, paste0(base_name, ".feather"))

      if (file.exists(feather_path)) { # Including this skip to face crashes while running the code - If it crashes, it will not try to read files that were already read
        message("Skipping (already exists): ", base_name)
        return(invisible(NULL))
      }
      
      df <- read_fwf(path, col_positions = layout, col_types = cols(.default = "c"))  # No warning
      
      # Replace * for NA 
      df <- df |> 
        mutate(across(everything(), ~case_when(
          #.x == "*" ~ "Redacted",  Variables with * values appear to be personal identifiable information (PII)
          .x == "" ~ NA_character_, # Spaces as NAs
          TRUE ~ .x
        ))) 
      
      # Save as feather file
      write_feather(df, feather_path)
      
      message("✔ Saved: ", feather_path)
    }, error = function(e) {
      message("✘ Error processing: ", path, " — ", e$message)
    })
  } else {
    message("⚠ File not found: ", path)
  }
})


