library(httr)       # HTTP requests (GET, headers, status handling)
library(rvest)      # HTML parsing helpers on top of xml2
library(tidyverse)  # dplyr/stringr/tibble/purrr etc.
library(lubridate)  # dates (make_date)
library(xml2)       # low-level HTML/XML + url_absolute()
library(purrr)     # For pluck
library(dplyr)

## Identify the link of the latest release (Latest month)

get_latest_lions_month <- function(
    index_url = "https://www.justice.gov/usao/resources/foia-library/national-caseload-data"
) {
  # Fetch & parse html (with user-agent)
  
  resp <- httr::GET(index_url, httr::user_agent("R (rvest/httr)")) # Sends GET request with user-agent to reduce the chances of getting blocked 
  httr::stop_for_status(resp) # Check if the request was successful, otherwise stop with an error
  html <- read_html(httr::content(resp, as = "text", encoding = "UTF-8")) # Reads the HTTP body as text, then parses it into an HTML document.
  
  # Collect links + text (build tibble from vectors, not from ".")
  
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
    latest     = data_links |> slice(1) |> select(date_key, month_name, year, url) # Gets the url of the most recent month's data files
  )
}

# Usage
lions_links <- get_latest_lions_month()

## Get all the zip links from the latest month 

# Bringing the url of the latest month
index_url <- lions_links |>
  purrr::pluck("latest") |> # Getting the elements in "latest"
  dplyr::pull(url) # Pulling the url

# Function to get links to download disks
get_disk_links <- function(
      index_url = index_url
  ) {
    # Fetch & parse html (with user-agent)
    
    resp <- httr::GET(index_url, httr::user_agent("R (rvest/httr)")) # Sends GET request with user-agent to reduce the chances of getting blocked 
    httr::stop_for_status(resp) # Check if the request was successful, otherwise stop with an error
    html <- read_html(httr::content(resp, as = "text", encoding = "UTF-8")) # Reads the HTTP body as text, then parses it into an HTML document.
    
    # Collect links + text (build tibble from vectors, not from ".")
    
    a_nodes <- html_elements(html, "a") # Selects all <a> nodes (links) from the HTML document
    links <- tibble( # Builds a tibble from the anchor
      text = html_text(a_nodes, trim = TRUE), # text: the clickable text of each link (trimmed)
      href = html_attr(a_nodes, "href") # href: the raw link target
    ) |> 
      filter(!is.na(href), text != "") |>  # Filters out empty/NA links
      mutate(url = xml2::url_absolute(href, index_url)) # url converts each href to an absolute URL using the page’s base URL
    
    # Keep only "DISK*" items 
  
    disks_links <- links |> 
      filter(
        str_detect(text, regex("Disk", ignore_case = TRUE)) & # Filter for links that contain "Disk"
        str_detect(text, regex("zip", ignore_case = TRUE)) # and the text "zip"
      ) 
   
  }