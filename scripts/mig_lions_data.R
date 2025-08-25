# Description: This script creates the lions data set of immigration related cases to upload to deportationdataproject.org

library(dplyr)
library(arrow)

# Setup directories

setwd("~/Library/CloudStorage/Box-Box/deportationdata")

input_dir <- "_processing/intermediate/EOUSA/library/lions_data"

output_dir <- "_processing/intermediate/EOUSA/library/lions_data/filtered"

# 0. Get immigration related cases

mig_cases <- arrow::read_feather(paste0(input_dir, "/filtered/immigration_cases.feather"))

# 1. Get gs_participant

gs_participant <- arrow::read_feather(paste0(input_dir, "/gs_participant_AK.feather"))

## 1.1 Create unique ID (District + CASEID) 

gs_participant <- gs_participant |> 
  mutate(UNIQUEID = paste0(DISTRICT, CASEID)) |> 
  select(UNIQUEID, everything())

## 1.2 Filter for immigration related cases from immigration_cases.feather

gs_participant_mig <-
  gs_participant |> 
  filter(UNIQUEID %in% pull(mig_cases,UNIQUEID))        # keep rows that match any statute

## 1.3 Filter for cases after 1996 (Sys_init_date)

gs_participant_mig <- 
  gs_participant_mig |> 
  mutate(
    SYS_INIT_DATE = as.Date(SYS_INIT_DATE, format = "%d-%b-%Y")
  ) |> 
  filter(as.Date(SYS_INIT_DATE) >= as.Date("1996-01-01")) # After statues changes

## 1.4 Select needed variables 
 # Caseid, ID, Agency, District, Sys_init_date, Rcvd_date, Participant_Type, Gender, Country, Home_City, CTY, Crim_hist, ZipCode



## This should loop across all districts and bind to have one dataset (mig_lions_data)


# 2. Get gs_case
## 2.1 Create unique ID (District + CASEID)
## 2.2 Select needed variables
 # Case_status (Status), Unit, Case_type (type), branch, OFFENSE_FROM, OFFENSE_TO, CLOSE_DATE
## 2.3 Join to mig_lions_data

# 3. Get GS_CASE_PROG_CAT
## 2.1 Create unique ID (District + CASEID)
## 2.2 Select needed variables
# PROG_CAT
## 2.3 Join to mig_lions_data