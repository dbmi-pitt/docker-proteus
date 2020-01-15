# Load libraries
library(tidyverse)
library(magrittr)
library(dbplyr)
library(emayili)

attachments <- function(email, file) {
  email %>%
    attachment(file)
}

# Source in config and function objects
source('/app/01-functions.R')

# Establish connection to db
conn <- DBI::dbConnect(odbc::odbc(),
          Driver = "Oracle Driver",
          Server = Sys.getenv("server"),
	  Database = Sys.getenv("db"),
	  uid = Sys.getenv("user"),
          pwd = Sys.getenv("pass"))

version <- Sys.getenv("cdm_version")
cdm_schema <- Sys.getenv("cdm_schema")
ref_schema <- Sys.getenv("ref_schema")
ref_table <- Sys.getenv("ref_table")
USE_LOOKUP_TBL <- Sys.getenv("use_ref_tbl")

if (Sys.getenv("email") == "Y") {
  smtp <- server(host = Sys.getenv("email_host"),
                 port = as.numeric(Sys.getenv("email_port")),
                 username = Sys.getenv("email_user"),
                 password = Sys.getenv("email_pass"))
  
  start_email <- envelope() %>%
    from(Sys.getenv("email_from")) %>%
    to(unlist(strsplit(Sys.getenv("email_to"), split=" "))) %>%
    subject(as.character(glue::glue("[Proteus] DC tests initiated on {cdm_schema} version {version}")))
  smtp(start_email)
}

# Run unit tests and save results to unit_tests subdirectory
dir.create('/app/unit_tests/')
dir.create('/app/unit_tests/invalid_values/')

{if (version == "5.1") readr::read_csv('/app/inst/unit_tests_51.csv')
  else if (version == "5.1_STG") readr::read_csv('/app/inst/staging_tests.csv')
  else if (version == "5.1_HP") readr::read_csv('/app/inst/unit_tests_51_hp.csv')
  else if (version == "5.1_HP_STG") readr::read_csv('/app/inst/hp_staging_tests.csv') } %>%
  mutate(schema = cdm_schema, ref_schema = ref_schema, ref_table = ref_table, backend = "Oracle", version = version) %>%
  purrr::pmap_df(perform_unit_tests) %>%
  readr::write_csv(., paste0('/app/unit_tests/unit_tests_', format(Sys.time(), "%m%d%Y"), '.csv'))

if (Sys.getenv("email") == "Y") {
  finish_email <- envelope() %>%
  	from(Sys.getenv("email_from")) %>%
    to(unlist(strsplit(Sys.getenv("email_to"), split=" "))) %>%
  	subject(as.character(glue::glue("[Proteus] DC tests completed on {cdm_schema} version {version}")))
  
  attachment_list <- list.files('/app/unit_tests', recursive = TRUE, full.names = TRUE)
  
  for (i in 1:length(attachment_list)) {
    finish_email <- attachments(finish_email, attachment_list[i])
  }
  
  smtp(finish_email)
}
