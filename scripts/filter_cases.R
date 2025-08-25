# Description: This script reads in all feather files and outputs a vector of all UNIQUEIDs that have at least one immigration related charge 

library(readr)
library(dplyr)
library(stringr)
library(arrow)

# Setup directories

setwd("~/Library/CloudStorage/Box-Box/deportationdata")

input_dir <- "_processing/intermediate/EOUSA/library/lions_data"

output_dir <- "_processing/intermediate/EOUSA/library/lions_data/filtered"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# 1. List setups
## 1.1. List of Immigration Status - Update as needed. Keep the following format for correct filtering "8 U.S.C. § 1306(a)"  

statutes_df <- tribble(
  ~Statute,                               ~Description,
  #"18 U.S.C. § 1001",  "False statements (commonly used in immigration forms)", # Broad charge - not using it to filter for now 
  "18 U.S.C. § 1015",  "False statements in naturalization, citizenship applications", 
  #"18 U.S.C. § 1028",  "Fraud and related activity in connection with ID documents", # Broad charge - not using it to filter for now
  #"18 U.S.C. § 1028(a)",  "Aggravated identity theft (used in immigration-related document fraud)", # Broad charge - not using it to filter for now
  "18 U.S.C. § 1421",  "Accounts of court officers", 
  "18 U.S.C. § 1422",  "Fees in naturalization proceedings", 
  "18 U.S.C. § 1423",  "Misuse of evidence of citizenship or naturalization", 
  "18 U.S.C. § 1424",  "Personation or misuse of papers in naturalization proceedings", 
  "18 U.S.C. § 1425",  "Procurement of Citizenship or Naturalization Unlawfully", 
  "18 U.S.C. § 1426",  "Reproduction of naturalization or citizenship papers", 
  "18 U.S.C. § 1427",  "Sale of naturalization or citizenship papers", 
  "18 U.S.C. § 1428",  "Surrender of canceled naturalization certificate", 
  "18 U.S.C. § 1429",  "Penalties for neglect or refusal to answer subpena", 
  "18 U.S.C. § 1542",  "Passport Fraud", 
  "18 U.S.C. § 1546",  "Visa Fraud and False Statements", 
  "18 U.S.C. § 758",  "High-Speed Flight from an Immigration Checkpoint", 
  "18 U.S.C. § 911",  "False Claim of U.S. Citizenship", 
  "8 U.S.C. § 1101",  "Definitions", 
  "8 U.S.C. § 1154",  "Procedure for granting immigrant status (petitions by spouse or child)", 
  "8 U.S.C. § 1160",  "Special agricultural workers", 
  "8 U.S.C. § 1182",  "Inadmissible aliens", 
  "8 U.S.C. § 1185",  "Travel control of citizens and aliens", 
  "8 U.S.C. § 1251",  "Deportable aliens", 
  "8 U.S.C. § 1252",  "Judicial review of orders of removal", 
  "8 U.S.C. § 1253(a)",  "Failure to Depart", 
  "8 U.S.C. § 1253(b)",  "Willful Failure to Comply with Terms of Release", 
  "8 U.S.C. § 1255",  "Adjustment of status (eligibility for permanent residence)", 
  "8 U.S.C. § 1302",  "Registration of aliens (data requirements for foreign nationals)", 
  "8 U.S.C. § 1304",  "Maintenance of registry information (Forms for registration and fingerprinting)", 
  "8 U.S.C. § 1305",  "Notification of address change by noncitizen", 
  "8 U.S.C. § 1306(a)",  "Willful Failure to Apply for Registration or be Fingerprinted", 
  "8 U.S.C. § 1306(b)",  "Failure to Notify of a Change of Address", 
  "8 U.S.C. § 1306(c)",  "Fraudulent Statements in Registration", 
  "8 U.S.C. § 1306(d)",  "Counterfeiting Photographs or Prints in Registration", 
  "8 U.S.C. § 1321",  "Prevent Landing", 
  "8 U.S.C. § 1322",  "Bringing Health-Related Inadmissible Noncitizens", 
  "8 U.S.C. § 1323",  "Bringing by Carrier", 
  "8 U.S.C. § 1324(a)(1)(A)(i)",  "Smuggling", 
  "8 U.S.C. § 1324(a)(1)(A)(ii)",  "Transporting", 
  "8 U.S.C. § 1324(a)(1)(A)(iii)",  "Harboring", 
  "8 U.S.C. § 1324(a)(1)(A)(iv)",  "Inducing or Encouraging", 
  "8 U.S.C. § 1324(a)(2)",  "Bringing to the United States", 
  "8 U.S.C. § 1324(a)(3)",  "Knowingly Hiring Unauthorized Noncitizens", 
  "8 U.S.C. § 1325(a)",  "Illegal Entry", 
  "8 U.S.C. § 1325(c)",  "Marriage Fraud", 
  "8 U.S.C. § 1326",  "Illegal Reentry", 
  "8 U.S.C. § 1327",  "Aiding or Assisting Entry of Criminal-, Subversion-, or Terrorism-Related Inadmissible Noncitizens", 
  "8 U.S.C. § 1328",  "Importation, Holding, or Keeping of Noncitizen for prostitution or “any other immoral purpose”", 
  "8 U.S.C. § 1357",  "Powers of immigration officers (apprehension, arrest, inspection)", 
  "8 U.S.C. § 1425",  "Ineligibility to naturalization of deserters from the Armed Forces", 
  "8 U.S.C. § 1440(c)",  "Civil Denaturalization for Military Discharge", 
  "8 U.S.C. § 1451(a)",  "Civil Denaturalization for Illegal Procurement and Willful Misrepresentation", 
  "8 U.S.C. § 1451(c)",  "Civil Denaturalization for Membership in Certain Organizations"
)

  # Get vector of target statutes
statutes_vec <- unique(na.omit(statutes_df$Statute))

## 1.2. List of immigration units to further filter Title 18 - Pending confirmation to include this filter for broad charges (18 U.S.C. § 1001, 18 U.S.C. § 1028 and 18 U.S.C. § 1028A)

units <- tribble(
  ~Code, ~Unit,
  "IMMI", 	"Immigration Cases",
  "DSI", 	"Domestic Security and Immigration",
  "VIMG", 	"Immigration Unit",
  "VIMM", 	"Immigration",
  "I", 	"Immigration",
  "VCIE", 	"Violent Crimes & Immigation Enforcement",
  "1IMC", 	"Immigration",
  "VBS", 	"Civil - 2nd Cir. Immigration Appeal",
  "IM", 	"IMMIGRATION",
  "INS", 	"Misdemeanor Immigration Case"
)

# 2. Filtering gs_case using LEAD_CHARGE

## 2.0 Loading dataset

gs_count <- arrow::read_feather(paste0(input_dir, "/gs_count.feather"))

## 2.1 Creating unique ID (CASEID - District)

gs_count <- gs_count |> 
  mutate(UNIQUEID = paste0(DISTRICT, CASEID)) |> 
  select(UNIQUEID, everything())

## 2.2 Format CHARGE to match the statute format in the tibble - also easier for analysis

gs_count <- gs_count |>
  mutate(
    # Title: leading number at start (e.g., "8" in "8 :1324(a)(2)")
    title = str_extract(CHARGE, "^\\s*\\d+") |>
      str_remove("^0+") |>   # remove leading zeros
      str_trim(),
    
    # Section raw: everything after the first colon that looks like a section
    # (digits/letters/parentheses), ignoring spaces
    section_raw = str_match(CHARGE, ":\\s*([0-9A-Za-z()]+)")[, 2],
    
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
    
    statute_code = if_else( # Replacing charge codes with formatted statute codes 
      !is.na(title) & !is.na(section_norm),
      paste0(title, " U.S.C. § ", section_norm),
      CHARGE # To replace with * in the cases it was redacted
    )
  )

## 2.3 Filter rows matching any statute in the tibble

filtered_gs_count <-
  gs_count |> 
  filter(statute_code %in% statutes_vec) |>          # keep rows that match any statute
  distinct(UNIQUEID, .keep_all = TRUE) 

## 3.3 Create case-level variable with all charges 
# This dataset is at the participant level, so there are multiple rows per case. We want to collapse it so each row has a cell with all charges for that case (comma separated) so we can filter all cases with at least one immigration related charge, not only the lead charge.

collapsed <- gs_count |> 
  group_by(UNIQUEID) |> 
  summarise(
    CHARGES_LIST = paste(unique(statute_code), collapse = ", "),
    N_PARTICIPANTS = n(), # Create a variable to capture number of participants per case
    .groups = "drop"
  )

## 3.4 Subset the case-level dataset to get a list of only immigration-related uniqueids (UNIQUEIDs, list of charges and amount of participants for future use)

immigration_cases <- collapsed |> 
  filter(UNIQUEID %in% pull(filtered_gs_count, UNIQUEID)) 

# 4 Save list of immigration related caseids

arrow::write_feather(immigration_cases, paste0(output_dir, "/immigration_cases.feather"))

# Cleanup
rm(list = ls()); gc()
