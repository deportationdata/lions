## Disk28 - This script reads the DISK28 files, which are formatted differently than the other disks.

#  Read me from this Disk does not include positions but the formatting of the .txt is clearer, so we'll read them by detecting the layout automatically from dashes

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

# Output directory

setwd("~/Dropbox/deportationdata")

# Path to DISK28 directory
disk28_path <- "~/Dropbox/deportationdata/data/EOUSA/LIONS/DISK28"

# List all .txt files in DISK28
disk28_files <- list.files(disk28_path, pattern = "\\.txt$", full.names = TRUE)

# Set working directory and path to DISK28 files
setwd("~/Dropbox/deportationdata")
disk28_path <- "~/Dropbox/deportationdata/data/EOUSA/LIONS/DISK28"
disk28_files <- list.files(disk28_path, pattern = "\\.txt$", full.names = TRUE)

# Function: Detect fwf layout from full file lines
detect_fwf_layout <- function(lines) {
  dash_line_idx <- which(str_detect(lines, "^-{3,}.*"))[1]
  if (is.na(dash_line_idx) || dash_line_idx < 2) return(NULL)
  
  header_line <- lines[dash_line_idx - 1]
  dash_line <- lines[dash_line_idx]
  
  dash_matches <- str_match_all(dash_line, "-+")[[1]]
  starts <- str_locate_all(dash_line, "-+")[[1]][, 1]
  ends <- starts + str_length(dash_matches[, 1]) - 1
  
  col_names <- map2_chr(starts, ends, ~ str_trim(str_sub(header_line, .x, .y)))
  
  fwf_positions(start = starts, end = ends, col_names = col_names)
}

# Process each DISK28 .txt file
layout_disk28 <- map_dfr(disk28_files, function(file_path) {
  # Read first 100 lines as character vector
  lines <- readLines(file_path, n = 100)  # read enough lines
  
  # Try to detect column layout
  col_positions <- detect_fwf_layout(lines)
  
  if (is.null(col_positions)) return(tibble())  # Skip if layout not detected
  
  # Build layout tibble for each column
  tibble(
    disk = "DISK28",
    table = toupper(file_path_sans_ext(basename(file_path))),
    col_names = col_positions$col_names,
    begin = col_positions$begin,
    end = col_positions$end,
    file = basename(file_path),
    is_lookup_table = FALSE
  )
})

layout_disk28 <- layout_disk28 |>
  # global_LIONS is not a lookup table nor a data table; ignore completely 
  filter(file != "global_LIONS.txt") 
  
# Create a list of file paths for each disk and file
layout_by_file <- 
  layout_disk28 |>
  group_by(file_path = file.path("data/EOUSA/LIONS", disk, file)) |>
  summarise(
    fwf = list(fwf_positions(begin, end, col_names)),
    .groups = "drop"
  )

# Creating table to walk

layout_tbl <- split(layout_by_file, layout_by_file$file_path)

# Creating directory to save feather files

output_dir <- "_processing/intermediate/EOUSA/library/lions_data"
dir_create(output_dir, 
           recurse = TRUE)  # ensures it does not throw an error if the directory already exists

#test_layout_tbl <- head(layout_tbl, 5) # For testing purposes, take only the first 5 entries

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
      
      # Read the file as lines
      lines <- readLines(path)
      
      # Find the line index that contains only dashes (separator line)
      sep_line <- str_which(lines, "^-{2,}")[1]
      
      # Extract header and data lines
      header <- lines[sep_line - 1]
      data_lines <- lines[(sep_line + 1):length(lines)]
      
      # Read the data
      df <- read_fwf(I(data_lines), col_positions = layout, col_types = cols(.default = "c")) # The second line of each .txt file 
      
      # Replace * for NA
      df <- df |> 
        mutate(across(everything(), ~na_if(.x, "*")))
      
      write_feather(df, feather_path)
      
      message("✔ Saved: ", feather_path)
    }, error = function(e) {
      message("✘ Error processing: ", path, " — ", e$message)
    })
  } else {
    message("⚠ File not found: ", path)
  }
})


