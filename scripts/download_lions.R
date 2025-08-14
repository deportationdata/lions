library(httr)       # HTTP requests (GET, headers, status handling)
library(rvest)      # HTML parsing helpers on top of xml2
library(tidyverse)  # dplyr/stringr/tibble/purrr etc.
library(lubridate)  # dates (make_date)
library(xml2)       # low-level HTML/XML + url_absolute()
library(purrr)     # For pluck
library(dplyr)
library(fs)        # For dir_create 

# Set up

UA <- "Mozilla/5.0 (compatible; R scraper; +https://example.org/)" # User-Agent to identify the scraper

## Identify the link of the latest release (Latest month)

get_latest_lions_month <- function(
    index_url = "https://www.justice.gov/usao/resources/foia-library/national-caseload-data"
) {
  # Fetch & parse html (with user-agent)
  
  resp <- httr::GET(index_url, httr::add_headers(`User-Agent` = UA)) # Sends GET request with user-agent to reduce the chances of getting blocked 
  httr::stop_for_status(resp) # Check if the request was successful, otherwise stop with an error
  html <- read_html(httr::content(resp, as = "text", encoding = "UTF-8")) # Reads the HTTP body as text, then parses it into an HTML document.
  
  # Collect links + text 
  
  a_nodes <- html_elements(html, "a") # Selects all <a> nodes (links) from the HTML document
  links <- tibble( # Builds a tibble from the anchor
    text = html_text(a_nodes, trim = TRUE), # text: the clickable text of each link (trimmed)
    href = html_attr(a_nodes, "href") # href: the raw link target
  ) |> 
    filter(!is.na(href), text != "") |>  # Filters out empty/NA links
    mutate(url = xml2::url_absolute(href, index_url)) # url converts each href to an absolute URL using the page’s base URL
  
  # Keep only "Month YYYY data files" items and parse month/year to identify the most recent
  month_regex <- "(January|February|March|April|May|June|July|August|September|October|November|December)"
   
  data_links <- links |> 
    filter(
      str_detect(text, regex(month_regex, ignore_case = TRUE)) & # Filter for links that contain a month name
        str_detect(text, regex("data files", ignore_case = TRUE)) # and the text "data files"
    ) |>
    mutate(
      month_name = str_match(text, regex(month_regex, ignore_case = TRUE))[,1] |> str_to_title(), # Extract the month name and convert it to title case
      year       = str_extract(text, "\\d{4}") |> as.integer(), # Extract the year as an integer
      month_num  = match(month_name, month.name), # Convert month name to month number (1-12)
      date_key   = make_date(year, month_num, 1) # Create a date key for sorting
    ) |>
    arrange(desc(date_key)) # Arranges to have the most recent month first
  
  list(
    all_months = data_links |> select(date_key, month_name, year, url, text),
    latest     = data_links |> slice(1) |> select(date_key, month_name, year, month_num, url) # Gets the url of the most recent month's data files
  )
}

# Function to get links to download disks
get_disk_links <- function(month_url) {
  # Fetch & parse html (with user-agent)
    resp <- httr::GET(month_url, httr::add_headers(`User-Agent` = UA))

    httr::stop_for_status(resp) # Check if the request was successful, otherwise stop with an error
    html <- read_html(httr::content(resp, as = "text", encoding = "UTF-8")) # Reads the HTTP body as text, then parses it into an HTML document.
    
    # Collect links + text 
    
    a_nodes <- html_elements(html, "a") # Selects all <a> nodes (links) from the HTML document
    links <- tibble( # Builds a tibble from the anchor
      text = html_text(a_nodes, trim = TRUE), # text: the clickable text of each link (trimmed)
      href = html_attr(a_nodes, "href") # href: the raw link target
    ) |> 
      filter(!is.na(href), text != "") |>  # Filters out empty/NA links
      mutate(url = xml2::url_absolute(href, month_url)) # url converts each href to an absolute URL using the page’s base URL
    
    # Keep only "DISK*" items 
  
    disks_links <- links |> 
      filter(
        str_detect(text, regex("Disk", ignore_case = TRUE)) & # Filter for links that contain "Disk"
        str_detect(text, regex("zip", ignore_case = TRUE)) # and the text "zip"
      ) |> 
      transmute(url) |>
      distinct(url) |>
      pull(url)
}

## Function to download from DISK urls

download_with_retry <- function(url, path, tries = 3) {
  for (i in seq_len(tries)) {
    r <- try(
      httr::GET(
        url,
        httr::add_headers(`User-Agent` = UA),
        httr::write_disk(path, overwrite = TRUE)
      ),
      silent = TRUE
    )
    ok <- !inherits(r, "try-error") && identical(httr::status_code(r), 200L) # Must have HTTP status 200
    if (ok) return(invisible(TRUE))
    Sys.sleep(i) # simple backoff - if the download failed, wait a bit before retrying. The wait grows with i (1s, 2s, 3s).
  }
  stop("Failed to download after retries: ", url)
}

# Function to get tag for the latest release GitHub: latest release TAG for this repo (or NA if none) ---

get_latest_github_release_tag <- function(repo = Sys.getenv("GITHUB_REPOSITORY"),
                                          token = Sys.getenv("GITHUB_TOKEN")) {
  if (identical(repo, "")) stop("GITHUB_REPOSITORY not set (expected 'owner/repo').")
  url <- paste0("https://api.github.com/repos/", repo, "/releases/latest") # Getting JSON of latest release
  hdr <- httr::add_headers(
    `User-Agent` = UA,
    Authorization = if (nzchar(token)) paste("Bearer", token) else NULL,
    Accept = "application/vnd.github+json"
  )
  resp <- httr::GET(url, hdr)
  if (httr::status_code(resp) == 404) return(NA_character_)  # no releases yet
  httr::stop_for_status(resp)
  json <- httr::content(resp, as = "parsed")
  # Prefer tag_name; fall back to name if needed
  tag <- json$tag_name %||% json$name %||% NA_character_ # Getting the tag name of the latest release (it's automatically called tag_name in release's json)
  as.character(tag)
}


## Running all functions

fs::dir_create("inputs")

# Identify latest month on DOJ
latest <- get_latest_lions_month() |> purrr::pluck("latest") # Run our first function
latest_url  <- latest |> dplyr::pull(url)
latest_name <- sprintf("%s %d", latest$month_name, latest$year)        # e.g., "April 2025"
latest_tag  <- sprintf("%04d-%02d", latest$year, latest$month_num)     # e.g., "2025-04"


message("DOJ latest: ", latest_name, " (tag ", latest_tag, ")") # Show the latest month
message("Month page: ", latest_url) # and its URL

# Getting the month tag to name the release with the month (this is pulled in the YAML later)
ghout <- Sys.getenv("GITHUB_ENV")
if (nzchar(ghout)) {
  cat(sprintf("LATEST_MONTH_TAG=%s\n",  latest_tag),  file = ghout, append = TRUE)
  cat(sprintf("LATEST_MONTH_NAME=%s\n", latest_name), file = ghout, append = TRUE)
}

# Compare with latest GitHub release tag
latest_release_tag <- get_latest_github_release_tag()
msg_release <- if (is.na(latest_release_tag) || !nzchar(latest_release_tag)) "<none>" else latest_release_tag
message("Latest GitHub release tag: ", msg_release)

# CHECKER: Decide whether we need to skip downloads or not

if (!is.na(latest_release_tag) && nzchar(latest_release_tag) && identical(latest_release_tag, latest_tag)) {
  message("Already released ", latest_tag, " — skipping download.")
  if (nzchar(ghout)) cat("SHOULD_RUN=false\n", file = ghout, append = TRUE)
  quit(save = "no", status = 0)  # Stop if there is no new release
} else {
  if (nzchar(ghout)) cat("SHOULD_RUN=true\n", file = ghout, append = TRUE) # Otherwise, proceed
}

# Download new month zips
zip_urls <- get_disk_links(latest_url) # Run our second function to get the DISK links
if (length(zip_urls) == 0) stop("No DISK zip links found at: ", latest_url)

zip_urls <- head(zip_urls, 1) # TEST LINE - only one link
  
dests <- file.path("inputs", basename(zip_urls))
  
walk2(zip_urls, dests, \(url, path) {
  message("→ Downloading ", basename(path))
  download_with_retry(url, path)
})
  
message("All downloads completed into ./inputs")
