# Load libraries
library(tidyverse)
library(magrittr)
library(dbplyr)

# Source in config and function objects
source('/app/01-functions.R')

# Establish connection to db
conn <- DBI::dbConnect(odbc::odbc(),
                       Driver = "Oracle Driver",
                       Server = Sys.getenv("server"),
                       Database = Sys.getenv("db"),
                       uid = Sys.getenv("user"),
                       pwd = Sys.getenv("pass"))

cdm_schema <- Sys.getenv("cdm_schema")
cdm_version <- Sys.getenv("cdm_version")

# Declare list of tables to characterize
table_list <- c("CONDITION", "DEATH", "DEATH_CAUSE", "DEMOGRAPHIC", "DIAGNOSIS",
                "DISPENSING", "ENCOUNTER", "ENROLLMENT", "MED_ADMIN", "OBS_CLIN",
                "OBS_GEN", "PCORNET_TRIAL", "PRESCRIBING", "PROCEDURES", "PRO_CM",
                "PROVIDER", "VITAL", "IMMUNIZATION")

# Create directory structure to store reports
dir.create('/app/summaries/CSV', recursive = TRUE)
dir.create('/app/summaries/HTML')

# Loop through list of tables and run data characterization
for (i in table_list) {
  generate_summary(conn, backend = "Oracle", version = version, schema = cdm_schema, table = i)
}
