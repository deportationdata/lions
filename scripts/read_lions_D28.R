## Disk28 - This script reads the DISK28 files, which are formatted differently than the other disks
#  Read me from this Disk does not include positions but the formatting of the .txt is clearer, so we'll read them by detecting the layout automatically from dashes

library(tidyverse)
library(readr)
library(arrow)
library(tidylog)
library(purrr)
library(dplyr)
library(tibble)
library(fs) # To suppress messages
library(tools)  # for file_path_sans_ext()

# safer on CI (vroom multithreading can segfault on odd inputs)
Sys.setenv(VROOM_NUM_THREADS = "1")

# Path to DISK28 directory
disk28_path <- "inputs/unzipped/DISK28"

# Creating directory to save feather files
output_dir <- "outputs"
dir_create(output_dir, 
           recurse = TRUE)  # ensures it does not throw an error if the directory already exists

# List .txt files
disk28_files <- list.files(disk28_path, pattern = "\\.txt$", full.names = TRUE) |>
  keep(~ basename(.x) != "global_LIONS.txt")


# Function: Detect fwf layout from full file lines
detect_fwf_layout <- function(lines) {
  dash_line_idx <- which(str_detect(lines, "^-{3,}.*"))[1]
  if (is.na(dash_line_idx) || dash_line_idx < 2) return(NULL)
  
  header_line <- lines[dash_line_idx - 1]
  dash_line <- lines[dash_line_idx]
  
  spans <- str_locate_all(dash_line, "-+")[[1]]
  if (is.null(spans) || nrow(spans) == 0) return(NULL)

  starts <- spans[, 1]
  ends   <- spans[, 2]
  
  col_names <- map2_chr(starts, ends, ~ str_trim(str_sub(header_line, .x, .y)))
  
  keep_cols <- nzchar(col_names)
  if (!any(keep_cols)) return(NULL)

  fwf_positions(start = starts[keep_cols], end = ends[keep_cols], col_names = col_names[keep_cols])
}

# Process each DISK28 .txt file skipping undetectable
layout_disk28 <- map_dfr(disk28_files, function(file_path) {
  # Read first 100 lines as character vector
  lines <- readLines(file_path, n = 200L, warn = FALSE)  # read enough lines
  
  # Try to detect column layout
  col_positions <- detect_fwf_layout(lines)
  
  if (is.null(col_positions)) {
    message("↷ No valid dashed header in: ", basename(file_path), " — skipping")
    return(tibble())
  }
  # Build layout tibble for each column
  tibble(
    disk = "DISK28",
    table = toupper(file_path_sans_ext(basename(file_path))),
    col_names = col_positions$col_names,
    begin = col_positions$begin,
    end = col_positions$end,
    file = basename(file_path)
  )
})

if (nrow(layout_disk28) == 0L) {
  message("No layouts detected in ", disk28_path, " — nothing to do.")
  quit(save = "no", status = 0)
}

# Create a list of file paths for each disk and file
layout_by_file <- 
  layout_disk28 |>
  group_by(file_path = file.path("inputs/unzipped", disk, file)) |>
  summarise(
    fwf = list(fwf_positions(begin, end, col_names)),
    .groups = "drop"
  )

# Creating table to walk

layout_tbl <- split(layout_by_file, layout_by_file$file_path)

#test_layout_tbl <- head(layout_tbl, 5) # For testing purposes, take only the first 5 entries

# Loop through each table and read the corresponding .txt file, then save it as a feather file

walk2(layout_tbl, names(layout_tbl), function(tbl, path) {
  layout <- tbl[["fwf"]][[1]]
  
  if (file_exists(path)) {
    tryCatch({
      
      # Extract the base name to save it
      base_name <- file_path_sans_ext(basename(path))
      feather_path <- file.path(output_dir, paste0(base_name, ".feather"))
      
      # if (file.exists(feather_path)) { # Including this skip to face crashes while running the code - If it crashes, it will not try to read files that were already read
      #   message("Skipping (already exists): ", base_name)
      #   return(invisible(NULL))
      # }
      
      # Avoid encoding errors by guessing the encoding of the file
      raw_bytes <- readr::read_lines_raw(path)
      enc_guess <- readr::guess_encoding(raw_bytes) |>
        arrange(desc(confidence)) |>
        slice(1) |>
        pull(encoding) |>                           # best encoding (or NA)
        purrr::pluck(1) %||% "UTF-8"                  # fallback to UTF-8
      
      # Read the file as lines using identified encoding
      con <- file(path, open = "r", encoding = enc_guess)
      lines <- readLines(con, warn = FALSE)      # all lines as character vector
      close(con)

      lines <- str_replace_all(lines, "\r$", "")
    
      # Find the line index that contains only dashes (separator line)
      sep_line <- stringr::str_which(lines, "^[\\-–—]{3,}.*$")[1]
      
      # Extract header and data lines
      header <- lines[sep_line - 1]
      data_lines <- lines[(sep_line + 1L):length(lines)]
      
      # Read the data avoiding encoding errors
      df <- readr::read_fwf(
        file          = I(data_lines),                 # <- character input
        col_positions = layout,
        col_types     = readr::cols(.default = "c"),
        locale        = readr::locale(encoding = enc_guess)
      )
      
      write_feather(df, feather_path)
      
      message("✔ Saved: ", feather_path)
    }, error = function(e) {
      message("✘ Error processing: ", path, " — ", e$message)
    })
  } else {
    message("⚠ File not found: ", path)
  }
})
