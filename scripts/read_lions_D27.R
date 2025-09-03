## Disk27 - This script reads the DISK27 files, which includes logs of the sizes of files

#  Read me from this Disk does not include positions but the formatting of the .log is clearer, so we'll read them by detecting the layout automatically from dashes

library(readr)
library(stringr)
library(purrr)
library(dplyr)
library(fs)
library(tibble)
library(tidylog)
library(tools) # for file_path_sans_ext()
library(arrow) # To handle feather


# Path to DISK27
disk27_path <- "inputs/unzipped/DISK27"

# For local tests:
#disk27_path <- "~/Library/CloudStorage/Box-Box/deportationdata/data/EOUSA/LIONS/DISK27"

# Find all .log files
disk27_files <- list.files(disk27_path, pattern = "\\.log$", full.names = TRUE)
disk27_filtered <- disk27_files[!str_detect(disk27_files, "_(compare_file_sizes|detailed_log)\\.log$")] # dropping files with a different format - we will read _detailed_log below

## TEST 
#file_path <- disk27_filtered[1] # For testing

# Function to detect fwf positions using dash line
detect_fwf_positions <- function(file_path) {
  # Read at least 5 lines to capture header + dashes
  header_lines <- readLines(file_path, n = 5)
  
  # Identifies where the dashes are
  dash_line_index <- grep("^-{2,}", header_lines)[1]
  # Gets the dashed line
  dashes_line <- header_lines[dash_line_index]
  # From dashed line, gets the line with the column names
  colnames_line <- header_lines[dash_line_index - 1]
  
  # Use gregexpr to find start positions of dashes
  matches <- gregexpr("-+", dashes_line)
  starts <- as.vector(unlist(matches))
  # Gets how many dashes there are to identify width
  widths <- as.vector(unlist(attr(matches[[1]], "match.length")))
  starts <- starts[starts != -1]
  
  # Extract and trim column names using the column width we detected
  col_names <- map2_chr(starts, widths, ~ str_trim(substr(colnames_line, .x, .x + .y - 1)))
  
  # Return fwf specification
  fwf_positions(start = starts, end = starts + widths - 1, col_names = col_names)
}

# Create layout table for DISK27
layout_disk27 <- map_dfr(disk27_filtered, function(file_path) {
  col_positions <- detect_fwf_positions(file_path)

  tibble(
    disk = "DISK27",
    table = str_remove(basename(file_path), "\\.log$"),
    col_names = col_positions$col_names,
    begin = col_positions$begin + 1, # Adjusting so fwf_positions won't get negative positions
    end = col_positions$end,
    file = basename(file_path),
    is_lookup_table = FALSE
  )
})

# Group into file-specific specs

layout_by_file <- tibble(file_path = disk27_filtered) |>
  mutate(fwf = purrr::map(file_path, detect_fwf_positions))

# Directory to save intermediate feather files
output_dir <-  "outputs"
#output_dir <- "~/Dropbox/DDP/Tests" # For tests
dir_create(output_dir)

# Read each file, parse it using the detected layout, and save as feather

purrr::walk2(layout_by_file$fwf, layout_by_file$file_path, function(layout, path) {
  if (!fs::file_exists(path)) {
    message("⚠ File not found: ", path)
    return(invisible(NULL))
  }
  
  tryCatch({
    base_name <- tools::file_path_sans_ext(basename(path))
    feather_path <- file.path(output_dir, paste0(base_name, ".feather"))
    
    # Read & normalize lines
    lines <- readLines(path, warn = FALSE)
    lines <- sub("\r$", "", lines)          
    
    # Find dashed separator
    sep_idx <- stringr::str_which(lines, "^-{2,}")[1]
    if (is.na(sep_idx) || sep_idx >= length(lines)) {
      message("✘ No dash separator or no data lines: ", path)
      return(invisible(NULL))
    }
    
    # Data lines (trim right, drop empties)
    data_lines <- lines[(sep_idx + 1):length(lines)]
    data_lines <- sub("\\s+$", "", data_lines)
    data_lines <- data_lines[nzchar(data_lines)]
    if (length(data_lines) == 0) {
      message("✘ Empty data section after separator: ", path)
      return(invisible(NULL))
    }
    
    # Parse using the single, precomputed spec
    df <- readr::read_fwf(
      I(data_lines),
      col_positions = layout,
      col_types = readr::cols(.default = "c")
    ) |>
      dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.x, "*")))
    
    # Sanity checks before writing
    if (nrow(df) == 0) {
      message("✘ Parsed 0 rows: ", path, " (spec misalignment likely)")
      return(invisible(NULL))
    }
    
    # Replace dplyr::if_all with a base-R check:
    all_empty <- all(vapply(df, function(x) all(is.na(x) | x == ""), logical(1)))
    if (all_empty) {
      message("✘ All columns empty: ", path, " (check spec)")
      return(invisible(NULL))
    }
    
    arrow::write_feather(df, feather_path)
    message("✔ Saved: ", feather_path)
  }, error = function(e) {
    message("✘ Error processing: ", path, " — ", e$message)
  })
})

# Loop through each table and read the corresponding .txt file, then save it as a feather file

# Test
# tbl <- layout_tbl
# path <- names(layout_tbl)[1]
# 
# walk2(layout_tbl, names(layout_tbl), function(tbl, path) {
#   
#   layout <- purrr::pluck(tbl, 1, "fwf", 1) # Get positions
#   
#   if (file_exists(path)) {
#     tryCatch({
#       
#       # Extract the base name to save it
#       base_name <- file_path_sans_ext(basename(path))
#       feather_path <- file.path(output_dir, paste0(base_name, ".feather"))
#       
#       # if (file.exists(feather_path)) { # Including this skip to face crashes while running the code - If it crashes, it will not try to read files that were already read
#       #  message("Skipping (already exists): ", base_name)
#       #  return(invisible(NULL))
#       # }
#       
#       # Read the file as lines
#       lines <- readLines(path)
#       
#       # Find the line index that contains only dashes (separator line)
#       sep_line <- str_which(lines, "^-{2,}")[1]
#       
#       # Extract header and data lines
#       header <- lines[sep_line - 1]
#       data_lines <- lines[(sep_line + 1):length(lines)]
#       
#       # Read the data
# 
#       df <- read_fwf(I(data_lines), col_positions = layout, col_types = cols(.default = "c")) # No warnings but won't process JANFY2025_compare_file_sizes.log and JANFY2025_detailed_log.log - These have different formats. JANFY2025_compare_file_sizes.log was not considered necessary so we'll only read JANFY2025_detailed_log.log
#       
#       # Replace * for NA
#       df <- df |> 
#         mutate(across(everything(), ~na_if(.x, "*")))
#       
#       write_feather(df, feather_path)
#       
#       message("✔ Saved: ", feather_path)
#     }, error = function(e) {
#       message("✘ Error processing: ", path, " — ", e$message)
#     })
#   } else {
#     message("⚠ File not found: ", path)
#   }
# })
# 
### Read JANFY2025_detailed_log that has a different format

# Read in the lines from the log file
detailed_log  <- str_subset(disk27_files, "_detailed_log") # Get path of the detailed log file
lines <- readLines(detailed_log) # Read the log

# Find indices of lines indicating table blocks
table_starts <- str_which(lines, "^Table name\\s+[–-]\\s+GS_") # Identify the line where the chunk for each table starts
table_ends <- str_which(lines, "^\\*{5,}$") # Identify the line where the chunk for each table ends - more than 5 asteriscs

## Read first the most straight forward cases (the tables that are not split by district have different formats)

# Now extract information for each block
list1 <- map_dfr(table_starts, function(i) {
  # Get the relevant lines by searching ahead
  end_idx <- table_ends[table_ends > i][1]
  if (is.na(end_idx)) return(tibble())  # Skip if no end found

  block <- lines[(i + 1):(end_idx - 1)] # Block of information - each block is a table

  imported_line <- block[str_detect(block, "Number of records imported:")]
  deleted_line  <- block[str_detect(block, "Marked and sealed civil records deleted:")]
  written_line  <- block[str_detect(block, "Number of records written to file:")]

  # Extract values
  table   <- str_match(lines[i], "Table name\\s+[–-]\\s+(GS_[A-Z0-9_]+)")[,2]
  imported <- as.integer(str_extract(imported_line, "-?\\d+"))
  deleted  <- as.integer(str_extract(deleted_line, "-?\\d+"))
  written  <- as.integer(str_extract(written_line, "-?\\d+"))

  tibble(table, imported, deleted, written)
})

list1 <- list1 |>
  mutate(
    district = NA_character_,  # Initialize district as NA to bind later
  )

## Now process district-splitted tables

# Process each block
list2 <- map_dfr(table_starts, function(start_idx) {
   end_idx <- table_ends[table_ends > start_idx][1]
  if (is.na(end_idx)) return(tibble())  # skip if no end

  block <- lines[(start_idx + 1):(end_idx - 1)]

  # Extract table name from the start line (make sure it works even with weird dash characters)
  parent_table <- str_match(
    lines[start_idx],
    "Table name\\s+(–|-|—)\\s+(GS_[A-Z0-9_]+)"
  )[,3]

  # Extract imported/deleted using safe match
  imported <- block %>%
    str_subset("Number of records imported:") %>%
    str_extract("[0-9,]+") %>%
    str_remove_all(",") %>%
    as.integer()

  deleted <- block %>%
    str_subset("Marked and sealed civil records deleted:") %>%
    str_extract("-?[0-9,]+") %>%
    str_remove_all(",") %>%
    as.integer()

  # Extract region-level records
  regional_lines <- str_subset(block, "Number of records written to file GS_")

  regional_matches <- str_match(
    regional_lines,
    "Number of records written to file (GS_[A-Z0-9_]+) \\(([A-Z]+)\\)\\s*:\\s*(-?[0-9,]+)"
  )

  # Filter out empty matches
  regional_matches <- regional_matches[!is.na(regional_matches[,1]), , drop = FALSE]

  # If none, return empty tibble
  if (nrow(regional_matches) == 0) return(tibble())

  # Build tibble
  tibble(
    table = regional_matches[, 2],
    district = regional_matches[, 3],
    imported = imported,
    deleted = deleted,
    written = regional_matches[, 4] %>%
      str_remove_all(",") %>%
      as.integer()
  )
})

base_name <- file_path_sans_ext(basename(detailed_log)) # Get name of file

final <- rbind(list1, list2) |>
 write_feather(file.path(output_dir, paste0(base_name, ".feather"))) # Saving detailed logas feather file

