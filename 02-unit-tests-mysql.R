# Load libraries
library(tidyverse)
library(dbplyr)
library(emayili)
attachments <- function(email, file) {
  email %>%
    attachment(file)
}

# Source in config and function objects
source('/app/01-functions.R')

# Establish connect to db
conn <- DBI::dbConnect(RMySQL::MySQL(),
											 server = Sys.getenv("server"),
											 db = Sys.getenv("db"),
											 user = Sys.getenv("user"),
											 password = Sys.getenv("pass"))
schema <- NULL
ref_table <- Sys.getenv("ref_table")
db <- Sys.getenv("db")
USE_LOOKUP_TBL <- Sys.getenv("use_ref_tbl")
version <- Sys.getenv("cdm_version")


if (Sys.getenv("email") == "Y") {
  smtp <- server(host = Sys.getenv("email_host"),
                 port = as.numeric(Sys.getenv("email_port")),
                 username = Sys.getenv("email_user"),
                 password = Sys.getenv("email_pass"))

  start_email <- envelope() %>%
    from(Sys.getenv("email_from")) %>%
    to(unlist(strsplit(Sys.getenv("email_to"), split=" "))) %>%
    subject(as.character(glue::glue("[Proteus] DC tests initiated on {db} version {version}")))
  smtp(start_email, verbose = TRUE)
}

# Run unit tests and save results to unit_tests subdirectory
dir.create('/app/unit_tests')
dir.create('/app/unit_tests/invalid_values/')

{if (version == "3.1") readr::read_csv('/app/inst/unit_tests_31.csv')
  else if (version == "4.1") readr::read_csv('/app/inst/unit_tests_41.csv')
  else if (version == "5.1") readr::read_csv('/app/inst/unit_tests_51.csv') } %>%
  mutate(schema = NULL, ref_table = ref_table, backend = "mysql", version = version) %>%
  purrr::pmap_df(perform_unit_tests) %>%
  readr::write_csv(., paste0('/app/unit_tests/unit_tests_', format(Sys.time(), "%m%d%Y"), '.csv'))

if (Sys.getenv("email") == "Y") {
  finish_email <- envelope() %>%
    from(Sys.getenv("email_from")) %>%
    to(unlist(strsplit(Sys.getenv("email_to"), split=" "))) %>%
    subject(as.character(glue::glue("[Proteus] DC tests completed on {db} version {version}")))

  attachment_list <- list.files('/app/unit_tests', recursive = TRUE, full.names = TRUE)

  for (i in 1:length(attachment_list)) {
    finish_email <- attachments(finish_email, attachment_list[i])
  }

  smtp(finish_email)
}
