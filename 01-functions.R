### Unit test / data validation functions

check_ucum_api <- function(unit) {
  url <- 'https://clinicaltables.nlm.nih.gov/api/ucum/v3/search?terms='
  df <- jsonlite::fromJSON(URLencode(paste0(url, unit)))
  return(
    tibble::tibble(
      unit = unit,
      valid = ifelse(df[[1]] == 0, "INVALID", "VALID")
    )
  )
}

count_distinct_invalids <- function(table, field, test, schema = NULL, backend = NULL, version = NULL) {
  valueset <- get_valueset(table, field, version = version)
  sql <- glue::glue_sql("
                        SELECT {`field`}, COUNT(*) as N ",
                        ifelse(backend == "Oracle",
                               "FROM {`schema`}.{`table`}
                                WHERE {`field`} NOT IN ({vals*})
                                GROUP BY {`field`}",
                               "FROM {`table`}
                                WHERE {`field`} NOT IN {vals*})"),
                        vals = valueset, .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  return(result)
}

data_latency <- function(table, field, test, schema = NULL, backend = NULL, version = NULL) {
  if (test == "DC 3.07") {
    sql <- glue::glue_sql("
	SELECT
	  COUNT(*) AS N
	FROM (
		SELECT
		  DENSE_RANK() OVER ", ifelse(backend == "Oracle", "(ORDER BY EXTRACT(year from {`field`}) * 100 + EXTRACT(MONTH FROM {`field`}) DESC) AS rk, ", "(ORDER BY YEAR({`field`}) * 100 + MONTH({`field`}) DESC) AS rk, "),
                          "ENC_TYPE
	  FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") a
	WHERE rk = 1 AND ENC_TYPE IN ('AV', 'ED', 'EI', 'IP')
	GROUP BY rk", .con = conn)
  } else {
    sql <- glue::glue_sql("
	SELECT
	  COUNT(*) AS N
	FROM (
		SELECT
		  DENSE_RANK() OVER ", ifelse(backend == "Oracle", "(ORDER BY EXTRACT(year from {`field`}) * 100 + EXTRACT(MONTH FROM {`field`}) DESC) AS rk ", "(ORDER BY YEAR({`field`}) * 100 + MONTH({`field`}) DESC) AS rk "),
                          "FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") a
	WHERE rk = 1
	GROUP BY rk", .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  numerator <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  if (test == "DC 3.07") {
    sql <- glue::glue_sql("
SELECT
  AVG(N) AS baseline
FROM (
	SELECT
	  rk, COUNT(*) AS N
	FROM (
		SELECT
		  DENSE_RANK() OVER ", ifelse(backend == "Oracle", "(ORDER BY EXTRACT(year from {`field`}) * 100 + EXTRACT(MONTH FROM {`field`}) DESC) AS rk, ", "(ORDER BY YEAR({`field`}) * 100 + MONTH({`field`}) DESC) AS rk, "),
                          "ENC_TYPE
	  FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") a
	WHERE rk BETWEEN 10 AND 21 AND ENC_TYPE IN ('AV', 'ED', 'EI', 'IP')
	GROUP BY rk
) b", .con = conn)
  } else {
    sql <- glue::glue_sql("
SELECT
  AVG(N) AS baseline
FROM (
	SELECT
	  rk, COUNT(*) AS N
	FROM (
		SELECT
		  DENSE_RANK() OVER ", ifelse(backend == "Oracle", "(ORDER BY EXTRACT(year from {`field`}) * 100 + EXTRACT(MONTH FROM {`field`}) DESC) AS rk ", "(ORDER BY YEAR({`field`}) * 100 + MONTH({`field`}) DESC) AS rk "),
                          "FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") a
	WHERE rk BETWEEN 10 AND 21
	GROUP BY rk
) b", .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  denominator <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  result <- round(100 * numerator / denominator, 2)
  threshold <- 75
  pass <- ifelse(result < threshold, "FAIL", "PASS")
  txt <- glue::glue("{table} records are {result} percent complete three months prior to the current month.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.character(pass)
  )
  )
}

record_duplication <- function(table, field, test, schema = NULL, backend = NULL) {
  sql <- glue::glue_sql("
                        SELECT
                          COUNT(*)
                        FROM (
                          SELECT
                            {`field`}
                          FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                       "
                        GROUP BY {`field`}
                        HAVING COUNT(DISTINCT PATID) > 1
                        ) a
                        ", .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  numerator <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  sql <- glue::glue_sql("
                        SELECT
                          COUNT(DISTINCT {`field`})
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                        .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  denominator <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  result <- round(100 * numerator / denominator, 2)
  threshold <- 5
  pass <- ifelse(result > threshold, "FAIL", "PASS")
  txt <- glue::glue("{result} percent of {field}s are associated with more than 1 PATID in {table}.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.character(pass)
  )
  )
}

events_per_encounter <- function(table, field, vals, desc, test, schema = NULL, backend = NULL) {
  n_field <- glue::glue("N_", field)
  field_type <- glue::glue(field, "_TYPE")
  sql <- glue::glue_sql("
                        SELECT a.ENC_TYPE, a.{`n_field`}, b.N_ENC, (a.{`n_field`} / b.N_ENC) as RATIO FROM
                        (SELECT ENC_TYPE, COUNT(*) as {`n_field`} ",
                        ifelse(backend == "Oracle",
                               "FROM {`schema`}.{`table`} ",
                               "FROM {`table`} "),
                        "WHERE {`field_type`} IN ({vals*})
                        GROUP BY ENC_TYPE) a
                        FULL OUTER JOIN (
                        SELECT ENC_TYPE, COUNT(*) as N_ENC ",
                        ifelse(backend == "Oracle",
                               "FROM {`schema`}.ENCOUNTER ",
                               "FROM ENCOUNTER "),
                        "GROUP BY ENC_TYPE) b
                        ON a.ENC_TYPE = b.ENC_TYPE",
                        vals = vals, .con = conn)

  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  if (test == "DC 3.01") {
    thresholds <- tibble::tibble(
      ENC_TYPE = c("AV", "ED", "IP", "EI"),
      threshold = c(1.0, 1.0, 1.0, 1.0)
    )
  } else {
    thresholds <- tibble::tibble(
      ENC_TYPE = c("AV", "ED", "IP", "EI"),
      threshold = c(0.75, 0.75, 1.0, 1.0)
    )
  }
  return(result %>%
           filter(!is.na(ENC_TYPE), ENC_TYPE %in% c('AV', 'ED', 'IP', 'EI')) %>%
           mutate(RATIO = round(RATIO, 2),
                  text = glue::glue("Encounter type {ENC_TYPE} has {RATIO} {desc} per encounter."),
                  test = test) %>%
           left_join(., thresholds, by = "ENC_TYPE") %>%
           mutate(result = ifelse(RATIO < threshold, "FAIL", "PASS")) %>%
           select(text, test, result) %>%
           as_tibble())
}

extreme_values <- function(table, field, test, schema = NULL, backend = NULL, version = NULL) {
  if (field == "AGE") {
    bounds <- c("0", "89")
  }
  if (field == "DISPENSE_SUP") {
    bounds <- c("1", "90")
  }
  if (field == "SYSTOLIC") {
    bounds <- c("40", "210")
  }
  if (field == "DIASTOLIC") {
    bounds <- c("40", "120")
  }
  if (field == "HT") {
    bounds <- c("0", "94")
  }
  if (field == "WT") {
    bounds <- c("0", "350")
  }

  if (field=="AGE") {
    sql <- glue::glue_sql("
                          SELECT
                            COUNT(*)
                          FROM (
                            SELECT
                              CASE
                                WHEN death_date IS NULL THEN ",
                                ifelse(backend == "Oracle", "(SYSDATE - birth_date)/365
                                ELSE (death_date - birth_date)/365",
                                "DATEDIFF(year, birth_date, convert(date, GETDATE()))
                                ELSE DATEDIFF(year, birth_date, death_date)"),
                              "END AS age
                            FROM ",
                          ifelse(backend == "Oracle", "{`schema`}.DEMOGRAPHIC dg
                          LEFT JOIN {`schema`}.DEATH dh ON dg.patid = dh.patid
                          )
                          WHERE age NOT BETWEEN 0 and 89",
                          "DEMOGRAPHIC dg
                           LEFT JOIN DEATH dh on dg.patid = dh.patid
                           ) a
                          WHERE age NOT BETWEEN 0 and 89"),
                          low = bounds[1], high = bounds[2], .con = conn
                          )
  } else {
    sql <- glue::glue_sql("
                        SELECT COUNT(*) FROM ",
                          ifelse(backend == "Oracle", "{`schema`}.{`table`} ",
                                 "{`table`} "),
                          "WHERE {`field`} NOT BETWEEN {low} AND {high}",
                          low = bounds[1], high = bounds[2], .con = conn)
  }

  query <- DBI::dbSendQuery(conn, sql)
  num <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  if (field == "HT" | field == "WT" | field == "DIASTOLIC" | field == "SYSTOLIC") {
    sql <- glue::glue("SELECT
                      COUNT({`field`})
                     FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`}"), .con = conn)

  } else {
    sql <- glue::glue("SELECT
                      COUNT(*)
                     FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`}"), .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  denom <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  result <- round(100 * num / denom, 2)
  threshold <- 10
  pass <- ifelse(result > threshold, "FAIL", "PASS")
  txt <- glue::glue("{result} percent of {field} records from {table} are extreme records.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.character(pass)
  )
  )
}

field_conformance <- function(test, schema = NULL, backend = NULL, version = NULL) {
  if (version == "4.1" | version == "4.1_STG") {
    metadata <- readr::read_csv('/app/inst/CDM_41_field_names.csv')
  }
  if (version == "5.1" | version == "5.1_STG" | version == "5.1_HP_STG") {
    metadata <- readr::read_csv('/app/inst/CDM_51_metadata.csv')
  }
  if (backend == "Oracle") {
    sql <- glue::glue_sql("
                      SELECT
                        table_name, column_name, data_type, data_length, column_id
                      FROM all_tab_columns
                      WHERE owner = {schema}
                      ORDER BY table_name, column_id asc
                      ", .con = conn)
    query <- DBI::dbSendQuery(conn, sql)
    result <- DBI::dbFetch(query)
    DBI::dbClearResult(query)

    dtypes <- metadata %>%
      anti_join(., result, by = c("Table" = "TABLE_NAME", "Field" = "COLUMN_NAME", "Oracle_dtype" = "DATA_TYPE")) %>%
      mutate(text = glue::glue("{Field} does not conform to data model specification for data type."),
             test = test,
             result = ifelse(!is.na(text), "FAIL", "PASS")) %>%
      arrange(Table) %>%
      select(text, test, result)
  } else if (backend == "mssql") {
    sql <- glue::glue_sql("
                          SELECT
                            table_name AS TABLE_NAME, column_name AS COLUMN_NAME, upper(data_type) AS DATA_TYPE, character_maximum_length AS DATA_LENGTH
                          FROM information_schema.columns
                          WHERE table_schema = 'dbo'
                          ", .con = conn)
    query <- DBI::dbSendQuery(conn, sql)
    result <- DBI::dbFetch(query)
    DBI::dbClearResult(query)

    dtypes <- metadata %>%
      anti_join(., result, by = c("Table" = "TABLE_NAME", "Field" = "COLUMN_NAME", "mssql_dtype" = "DATA_TYPE")) %>%
      mutate(text = glue::glue("{Field} does not conform to data model specification for data type."),
             test = test,
             result = ifelse(!is.na(text), "FAIL", "PASS")) %>%
      arrange(Table) %>%
      select(text, test, result)
  }

  dlengths <- metadata %>%
    anti_join(., result, by = c("Table" = "TABLE_NAME", "Field" = "COLUMN_NAME", "LENGTH" = "DATA_LENGTH")) %>%
    select(Table, Field, LENGTH)  %>%
    filter(!is.na(LENGTH)) %>%
    mutate(text = glue::glue("{Field} does not conform to data model specification for data length."),
           test = test,
           result = ifelse(!is.na(text), "FAIL", "PASS")) %>%
    arrange(Table) %>%
    select(text, test, result)

  dnames <- metadata %>%
    anti_join(., result, by = c("Table" = "TABLE_NAME", "Field" = "COLUMN_NAME")) %>%
    select(Table, Field)  %>%
    mutate(text = glue::glue("{Field} does not conform to data model specification for field name."),
           test = test,
           result = ifelse(!is.na(text), "FAIL", "PASS")) %>%
    arrange(Table) %>%
    select(text, test, result)

  return(
    bind_rows(dtypes, dlengths, dnames)
  )
}

get_valueset <- function(table, field, version = NULL) {
  if(version == "3.1") {
    parseable <- readr::read_csv('/app/inst/CDM_31_parseable.csv')
  }
  if(version == "4.1") {
    parseable <- readr::read_csv('/app/inst/CDM_41_parseable.csv')
  }
  if(version == "4.1_STG") {
    parseable <- readr::read_csv('/app/inst/staging_parseable.csv')
  }
  if(version == "5.1" | version == "5.1_STG") {
    parseable <- readr::read_csv('/app/inst/CDM_51_parseable.csv')
  }
  return(parseable %>%
           filter(TABLE_NAME == table & FIELD_NAME == field) %>%
           select(VALUESET_ITEM) %>%
           pull())
}


orphans <- function(child, parent, key, test, schema = NULL, backend = NULL, version = NULL) {
  if (version == "4.1_STG" | version == "5.1_STG" | version == "5.1_HP_STG") {
    full_table <- stringr::str_replace(parent, "_STG", "")
    sql <- glue::glue_sql("
                        SELECT COUNT(DISTINCT {`key`}) FROM ",
                          ifelse(backend == "Oracle", "{`schema`}.{`child`} c
                               WHERE NOT EXISTS (SELECT {`key`} FROM
                                                 (SELECT {`key`} FROM {`schema`}.{`parent`}
                                                  UNION
                                                  SELECT {`key`} FROM {`schema`}.{`full_table`}) p ",
                                 "{`child`} c
                               WHERE NOT EXISTS (SELECT {`key`} FROM
                                                  (SELECT {`key`} FROM {`parent`}
                                                   UNION
                                                   SELECT {`key`} FROM {`full_table`}) p "),
                          "WHERE p.{`key`} = c.{`key`})",
                          .con = conn)

  }

  if (test == "DC 1.12") {
    sql <- glue::glue_sql("
                        SELECT COUNT(DISTINCT {`key`}) FROM ",
                          ifelse(backend == "Oracle", "{`schema`}.{`child`} c
                               WHERE NOT EXISTS (SELECT PROVIDERID FROM {`schema`}.{`parent`} p ",
                                 "{`child`} c
                               WHERE NOT EXISTS (SELECT PROVIDERID FROM {`parent`} p "),
                          "WHERE p.PROVIDERID = c.{`key`})",
                          .con = conn)
  } else {
  sql <- glue::glue_sql("
                        SELECT COUNT(DISTINCT {`key`}) FROM ",
                        ifelse(backend == "Oracle", "{`schema`}.{`child`} c
                               WHERE NOT EXISTS (SELECT {`key`} FROM {`schema`}.{`parent`} p ",
                               "{`child`} c
                               WHERE NOT EXISTS (SELECT {`key`} FROM {`parent`} p "),
                        "WHERE p.{`key`} = c.{`key`})",
                        .con = conn)
  }
  if (test == "DC 1.09") {
    query <- DBI::dbSendQuery(conn, sql)
    numerator <- DBI::dbFetch(query)
    DBI::dbClearResult(query)
    sql <- glue::glue_sql("SELECT COUNT(DISTINCT {`key`}) FROM ",
                          ifelse(backend == "Oracle", "{`schema`}.{`child`} ", "{`child`}"), .con = conn)
    query <- DBI::dbSendQuery(conn, sql)
    denominator <- DBI::dbFetch(query)
    DBI::dbClearResult(query)
    result <- round(100 * numerator / denominator, 2)
    txt <- glue::glue("{result} percent of {child} records have orphan {key}s.")
    return(tibble::tibble(text = txt,
                          test = test,
                          result = as.numeric(result),
                          threshold = 5
    ) %>%
      mutate(result = ifelse(result > threshold, "FAIL", "PASS")) %>%
      select(text, test, result)
    )
  } else {
    query <- DBI::dbSendQuery(conn, sql)
    result <- DBI::dbFetch(query)
    DBI::dbClearResult(query)
    txt <- glue::glue("Table {child} has {result} orphan {key}s.")
    return(tibble::tibble(text = txt,
                          test = test,
                          result = as.numeric(result),
                          threshold = 0
    ) %>%
      mutate(result = ifelse(result > threshold, "FAIL", "PASS")) %>%
      select(text, test, result)
    )
  }
}

missing_or_unknown <- function(table, field, test, threshold = NULL, schema = NULL, backend = NULL) {
  if (field == "DISCHARGE_DISPOSITION") {
    sql <- glue::glue_sql("
                      SELECT
                        CASE WHEN b.denom != 0 THEN ROUND(100 * a.num / b.denom, 2)
                             ELSE NULL
                        END as result
                      FROM (
                        SELECT
                          COUNT(*) as num, 1 as id
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "WHERE ({`field`} IN ('NI', 'OT', 'UN') OR {`field`} IS NULL) AND ENC_TYPE IN ('IP', 'EI')
                      ) a
                      LEFT JOIN (
                        SELECT
                          COUNT(*) as denom, 1 as id
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "WHERE ENC_TYPE IN ('IP', 'EI')
                      ) b on a.id = b.id",
                          .con = conn)
  } else if (field == "ADMIT_DATE" | field == "DISCHARGE_DATE") {
    sql <- glue::glue_sql("
                      SELECT
                        CASE WHEN b.denom != 0 THEN ROUND(100 * a.num / b.denom, 2)
                             ELSE NULL
                        END as result
                      FROM (
                        SELECT
                          COUNT(*) as num, 1 as id
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "WHERE {`field`} IS NULL AND ENC_TYPE IN ('IP', 'EI')
                      ) a
                      LEFT JOIN (
                        SELECT
                          COUNT(*) as denom, 1 as id
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "WHERE ENC_TYPE IN ('IP', 'EI')
                      ) b on a.id = b.id",
                          .con = conn)
  } else if (field == "BIRTH_DATE" | field == "MEASURE_DATE" | field == "PX_DATE" | field == "DISPENSE_DATE"
             | field == "RESULT_DATE" | field == "RX_ORDER_DATE" | field == "OBSCLIN_DATE" | field == "OBSGEN_DATE" | field == "DISPENSE_SUP"
             | field == "ENR_START_DATE" | field == "ENR_END_DATE" | field == "PRO_DATE" | field == "VX_RECORD_DATE") {
    sql <- glue::glue_sql("
                      SELECT
                        CASE WHEN b.denom != 0 THEN ROUND(100 * a.num / b.denom, 2)
                             ELSE NULL
                        END as result
                      FROM (
                        SELECT
                          COUNT(*) as num, 1 as id
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "WHERE {`field`} IS NULL
                      ) a
                      LEFT JOIN (
                        SELECT
                          COUNT(*) as denom, 1 as id
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") b on a.id = b.id",
                          .con = conn)
  } else {
    sql <- glue::glue_sql("
                      SELECT
                        CASE WHEN b.denom != 0 THEN ROUND(100 * a.num / b.denom, 2)
                             ELSE NULL
                        END as result
                      FROM (
                        SELECT
                          COUNT(*) as num, 1 as id
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "WHERE ({`field`} IN ('NI', 'OT', 'UN') OR {`field`} IS NULL)
                      ) a
                      LEFT JOIN (
                        SELECT
                          COUNT(*) as denom, 1 as id
                        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") b on a.id = b.id",
                          .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  pass <- ifelse(result > threshold, "FAIL", "PASS")
  txt <- glue::glue("{result} percent of {field} records in {table} are missing or unknown.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.character(pass)
  )
  )
}

normal_range_specification <- function(table, field, test, schema = NULL, backend = NULL) {
  sql <- glue::glue_sql("
                        SELECT
                          CASE WHEN b.denom != 0 THEN ROUND(100 * a.num / b.denom, 2)
                               ELSE NULL
                          END AS result
                        FROM (
                          SELECT
                          	COUNT(*) AS num, 1 AS id
                        	FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                                                "WHERE LAB_LOINC IS NOT NULL
                        	AND RESULT_NUM IS NOT NULL
                        	AND NORM_RANGE_LOW IS NOT NULL
                        	AND NORM_RANGE_HIGH IS NOT NULL
                        	AND NORM_MODIFIER_LOW IS NOT NULL
                        	AND NORM_MODIFIER_HIGH IS NOT NULL) a
                      	LEFT JOIN (
                      		SELECT
                      	  	COUNT(*) AS denom, 1 AS id
                      		FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                         "WHERE LAB_LOINC IS NOT NULL AND RESULT_NUM IS NOT NULL
                      	) b ON a.id = b.id",
   .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  threshold <- 80
  pass <- ifelse(result < threshold, "FAIL", "PASS")
  txt <- glue::glue("{result} percent of quantitative results for tests mapped to LAB_LOINC in {table} fully specify the normal range.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.character(pass)
  )
  )
}

patients_per_encounter <- function(table, field, test, schema = NULL, backend = NULL, version = NULL) {
  n_field <- glue::glue("N_", field)
  if (version == "4.1_STG" | version == "5.1_STG" | version == "5.1_HP_STG") {
  sql <- glue::glue_sql("SELECT 100 * ROUND(({`n_field`} / N_ENC), 3) as RATIO FROM
                        (SELECT COUNT(DISTINCT PATID) as {`n_field`}, 1 as id ",
                          ifelse(backend == "Oracle",
                                 "FROM {`schema`}.{`table`}) a ",
                                 "FROM {`table`}) a "),
                          "LEFT JOIN
                        (SELECT COUNT(DISTINCT PATID) as N_ENC, 1 as id ",
                          ifelse(backend == "Oracle",
                                 "FROM {`schema`}.ENCOUNTER_STG) b ",
                                 "FROM ENCOUNTER_STG) b "),
                          "ON a.id = b.id",
                          .con = conn)
  } else {
   sql <- glue::glue_sql("SELECT 100 * ROUND(({`n_field`} / N_ENC), 3) as RATIO FROM
                        (SELECT COUNT(DISTINCT PATID) as {`n_field`}, 1 as id ",
                        ifelse(backend == "Oracle",
                               "FROM {`schema`}.{`table`}) a ",
                               "FROM {`table`}) a "),
                        "LEFT JOIN
                        (SELECT COUNT(DISTINCT PATID) as N_ENC, 1 as id ",
                        ifelse(backend == "Oracle",
                               "FROM {`schema`}.ENCOUNTER) b ",
                               "FROM ENCOUNTER) b "),
                        "ON a.id = b.id",
                        .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  txt = glue::glue("{result}% of patients with encounters have {field} records.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.numeric(result),
                        threshold = 50
  ) %>%
    mutate(result = ifelse(result < threshold, "FAIL", "PASS")) %>%
    select(text, test, result)
  )
}

potential_code_error <- function(table, test, schema = NULL, backend = NULL) {
  if (table == "DIAGNOSIS" | table == "DIAGNOSIS_STG") {
    sql <- glue::glue_sql("
             SELECT
               a.code_type, records, total, round(100*records/total, 2) AS pct
             FROM (
               SELECT
                 code_type, sum(exceptn) AS records
               FROM (
                 SELECT
                   dx_type AS code_type, dx, ",
                          ifelse(backend == "Oracle", "greatest(unexp_alpha, unexp_length, unexp_numeric, unexp_string) AS exceptn, ",
                                 "CASE WHEN unexp_alpha + unexp_length + unexp_numeric + unexp_string > 1 THEN 1 ELSE 0 END AS exceptn, "),
                          "unexp_alpha, unexp_length, unexp_numeric, unexp_string
           FROM (
             SELECT
               dx_type, dx,
               CASE WHEN dx_type = '09' AND ", ifelse(backend == "Oracle", "regexp_like(dx, '^[A-DF-UW-Z]{{1}}') THEN 1 ",
                                                      "dx LIKE '[A-DF-UW-Z]%' THEN 1 "),
                          "     ELSE 0
               END AS unexp_alpha,
               CASE WHEN dx_type = '09' AND ", ifelse(backend == "Oracle", "length(replace(dx, '.', '')) ",
                                                      "len(replace(dx, '.', '')) "), "NOT BETWEEN 3 AND 5 THEN 1
                    WHEN dx_type = '10' AND ", ifelse(backend == "Oracle", "length(replace(dx, '.', '')) ",
                                                      "len(replace(dx, '.', '')) "), "NOT BETWEEN 3 AND 7 THEN 1
                    ELSE 0
               END AS unexp_length,
               CASE WHEN dx_type = '10' AND ", ifelse(backend == "Oracle", "regexp_like(dx, '^[0-9]{{1}}') THEN 1",
                                                      "dx LIKE '[0-9]%' THEN 1"),
                          "     ELSE 0
               END as unexp_numeric,
               CASE WHEN dx_type = '09' AND dx IN ('000') THEN 1
                    WHEN dx_type = '10' AND dx IN ('000', '999') THEN 1
                    ELSE 0
               END AS unexp_string
            FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`}", "{`table`}"),
                          ") s1
        ) s2
      GROUP BY code_type
    ) a
  INNER JOIN (
    SELECT
      dx_type, count(dx) as total
    FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "GROUP BY dx_type
  ) b on a.code_type = b.dx_type
  WHERE a.code_type IN ('09', '10')
  ", .con = conn)
  }
  if (table == "PROCEDURES" | table == "PROCEDURES_STG") {
    sql <- glue::glue_sql("
      SELECT
        a.code_type, records, total, round(100*records/total, 2) AS pct
      FROM (
      SELECT
        code_type, sum(records) AS records
      FROM (
        SELECT
          px_type as code_type, px,
          sum(exceptn) AS records,
          unexp_alpha, unexp_length, unexp_numeric, unexp_string
        FROM (
          SELECT
            px_type, px, ", ifelse(backend == "Oracle",
                                   "greatest(unexp_alpha, unexp_length, unexp_numeric, unexp_string) AS exceptn, ",
                                   "CASE WHEN unexp_alpha + unexp_length + unexp_numeric + unexp_string > 1 THEN 1 ELSE 0 END AS exceptn, "),
                          "unexp_alpha, unexp_length, unexp_numeric, unexp_string
          FROM (
            SELECT
              px_type, px,
              CASE
                WHEN px_type = '09' AND ", ifelse(backend == "Oracle", "regexp_like(px, '[a-zA-Z]') THEN 1 ",
                                                  "px LIKE '[a-zA-Z]%' THEN 1 "),
                          "    ELSE 0
              END AS unexp_alpha,
              CASE
                WHEN px_type = 'CH' AND ", ifelse(backend == "Oracle", "length(replace(px, '.', '')) < 5 THEN 1 ",
                                                  "len(replace(px, '.', '')) < 5 THEN 1 "),
                          "    WHEN px_type = '09' AND ", ifelse(backend == "Oracle", "length(replace(px, '.', '')) NOT IN (3, 4) THEN 1 ",
                                                                 "len(replace(px, '.', '')) NOT IN (3, 4) THEN 1 "),
                          "    WHEN px_type = '10' AND ", ifelse(backend == "Oracle", "length(replace(px, '.', '')) != 7 THEN 1 ",
                                                                 "len(replace(px, '.', '')) != 7 THEN 1 "),
                          "    ELSE 0
              END AS unexp_length,
              CASE
                WHEN px_type IN ('09', '10', 'CH') AND ", ifelse(backend == "Oracle", "regexp_like(px, '\\d') THEN 0 ",
                                                                 "px LIKE '%[0-9]%' THEN 0 "),
                          "    ELSE 1
              END AS unexp_numeric,
              CASE
                WHEN px_type = 'CH' AND px IN ('00000', '99999') THEN 1
                WHEN px_type = '10' AND px IN ('0000000', '9999999') THEN 1
                ELSE 0
              END AS unexp_string
            FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`}", "{`table`}"),
                          "
           WHERE px_type IN ('CH', '09', '10')
				 ) s1
			 ) s2
        GROUP BY px_type, px, unexp_alpha, unexp_length, unexp_numeric, unexp_string
      ) s3
      GROUP BY code_type
    ) a
    INNER JOIN (
    SELECT
      px_type, count(px) as total
    FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "GROUP BY px_type
    ) b on a.code_type = b.px_type
    ", .con = conn)
  }
  if (table == "CONDITION" | table == "CONDITION_STG") {
    sql <- glue::glue_sql("
             SELECT
               code_type, records, total, round(100*records/total, 2) AS pct
             FROM (
               SELECT
                 code_type, sum(exceptn) AS records
               FROM (
                 SELECT
                   condition_type AS code_type, condition, ",
                          ifelse(backend == "Oracle", "greatest(unexp_alpha, unexp_length, unexp_numeric, unexp_string) AS exceptn, ",
                                 "CASE WHEN unexp_alpha + unexp_length + unexp_numeric + unexp_string > 1 THEN 1 ELSE 0 END AS exceptn, "),
                          "unexp_alpha, unexp_length, unexp_numeric, unexp_string
           FROM (
             SELECT
               condition_type, condition,
               CASE WHEN condition_type = '09' AND ", ifelse(backend == "Oracle", "regexp_like(condition, '^[A-DF-UW-Z]{{1}}') THEN 1 ",
                                                             "condition LIKE '[A-DF-UW-Z]%' THEN 1 "),
                          "     ELSE 0
               END AS unexp_alpha,
               CASE WHEN condition_type = '09' AND ", ifelse(backend == "Oracle", "length(replace(condition, '.', '')) ",
                                                             "len(replace(condition, '.', '')) "), "NOT BETWEEN 3 AND 5 THEN 1
                    WHEN condition_type = '10' AND ", ifelse(backend == "Oracle", "length(replace(condition, '.', '')) ",
                                                             "len(replace(condition, '.', '')) "), "NOT BETWEEN 3 AND 7 THEN 1
                    ELSE 0
               END AS unexp_length,
               CASE WHEN condition_type = '10' AND ", ifelse(backend == "Oracle", "regexp_like(condition, '^[0-9]{{1}}') THEN 1",
                                                             "condition LIKE '[0-9]%' THEN 1"),
                          "     ELSE 0
               END as unexp_numeric,
               CASE WHEN condition_type = '09' AND condition IN ('000') THEN 1
                    WHEN condition_type = '10' AND condition IN ('000', '999') THEN 1
                    ELSE 0
               END AS unexp_string
            FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`}", "{`table`}"),
                          ") s1
        ) s2
      GROUP BY code_type
    ) a
  INNER JOIN (
    SELECT
      condition_type, count(condition) as total
    FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "GROUP BY condition_type
  ) b on a.code_type = b.condition_type
  WHERE a.code_type IN ('09', '10')
  ", .con = conn)
  }
  if (table == "PRESCRIBING" | table == "PRESCRIBING_STG") {
    sql <- glue::glue_sql("
		      SELECT
		        a.code_type, records, total, 100*round(records/total, 2) as pct
		      FROM (
		      SELECT
		        code_type,
		        sum(exceptn) as records
		      FROM (
		        SELECT
		          rxnorm_cui, 'RX' as code_type, ",
                          ifelse(backend == "Oracle", "greatest(unexp_alpha, unexp_length, unexp_numeric, unexp_string) AS exceptn, ",
                                 "CASE WHEN unexp_alpha + unexp_length + unexp_numeric + unexp_string > 1 THEN 1 ELSE 0 END as exceptn, "),
                          "  unexp_alpha, unexp_length, unexp_numeric, unexp_string
		        FROM (
		          SELECT
		            rxnorm_cui,
		            CASE WHEN ", ifelse(backend == "Oracle", "regexp_like(rxnorm_cui, '[a-zA-Z]') THEN 1 ",
		                                "rxnorm_cui LIKE '[a-zA-Z]%' THEN 1 "),
                          "         ELSE 0
		            END AS unexp_alpha,
		            CASE WHEN ", ifelse(backend == "Oracle", "length(replace(rxnorm_cui, '.', '')) NOT BETWEEN 2 AND 7 THEN 1",
		                                "len(replace(rxnorm_cui, '.', '')) NOT BETWEEN 2 AND 7 THEN 1 "),
                          "         ELSE 0
		            END AS unexp_length,
		            0 AS unexp_numeric, 0 as unexp_string
		          FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "
				 ) s1
			 ) s2
		    GROUP BY code_type
		    ) a
		    INNER JOIN (
		      SELECT
		        'RX' as code_type, count(rxnorm_cui) as total
		      FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") b ON a.code_type = b.code_type
		    ", .con = conn)
  }
  if (table == "DISPENSING" | table == "DISPENSING_STG") {
    sql <- glue::glue_sql("
		      SELECT
		        a.code_type, records, total, 100*round(records/total, 2) as pct
		      FROM (
		      SELECT
		        code_type,
		        sum(exceptn) as records
		      FROM (
		        SELECT
		          ndc, 'NC' as code_type, ",
                          ifelse(backend == "Oracle", "greatest(unexp_alpha, unexp_length, unexp_numeric, unexp_string) AS exceptn, ",
                                 "CASE WHEN unexp_alpha + unexp_length + unexp_numeric + unexp_string > 1 THEN 1 ELSE 0 END as exceptn, "),
                          "  unexp_alpha, unexp_length, unexp_numeric, unexp_string
		        FROM (
		          SELECT
		            ndc,
		            CASE WHEN ", ifelse(backend == "Oracle", "regexp_like(ndc, '[a-zA-Z]') THEN 1 ",
		                                "ndc LIKE '[a-zA-Z]%' THEN 1 "),
                          "         ELSE 0
		            END AS unexp_alpha,
		            CASE WHEN ", ifelse(backend == "Oracle", "length(replace(ndc, '.', '')) != 11 THEN 1",
		                                "len(replace(ndc, '.', '')) != 11 THEN 1 "),
                          "         ELSE 0
		            END AS unexp_length,
		            '0' AS unexp_numeric,
		            CASE WHEN ndc IN ('00000000000', '99999999999') THEN 1 ELSE 0 END as unexp_string
		          FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`}", "{`table`}"),
                          "
		        ) s1
		      ) s2
		    GROUP BY code_type
		    ) a
		    INNER JOIN (
		      SELECT
		        'NC' as code_type, count(ndc) as total
		      FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") b on a.code_type = b.code_type
		    ", .con = conn)
  }
  if (table == "LAB_RESULT_CM" | table == "LAB_RESULT_CM_STG") {
    sql <- glue::glue_sql("
      SELECT
        a.code_type, records, total, 100*round(records/total, 2) as pct
      FROM (
      SELECT
        code_type,
        sum(exceptn) as records
      FROM (
        SELECT
          lab_loinc, 'LC' as code_type, ",
                          ifelse(backend == "Oracle", "greatest(unexp_alpha, unexp_length, unexp_numeric, unexp_string) AS exceptn, ",
                                 "CASE WHEN unexp_alpha + unexp_length + unexp_numeric + unexp_string > 1 THEN 1 ELSE 0 END as exceptn, "),
                          "unexp_alpha, unexp_length, unexp_numeric, unexp_string
        FROM (
          SELECT
            lab_loinc,
            CASE WHEN ", ifelse(backend == "Oracle", "regexp_like(lab_loinc, '[a-zA-Z]') THEN 1 ",
                                "lab_loinc LIKE '[a-zA-Z]%' THEN 1 "),
                          "         ELSE 0
            END AS unexp_alpha,
            CASE WHEN ", ifelse(backend == "Oracle", "length(replace(lab_loinc, '.', '')) NOT BETWEEN 3 AND 7 THEN 1",
                                "len(replace(lab_loinc, '.', '')) NOT BETWEEN 3 AND 7 THEN 1 "),
                          "         ELSE 0
            END AS unexp_length,
            '0' AS unexp_numeric,
            CASE WHEN ", ifelse(backend == "Oracle", "length(lab_loinc) - instr(lab_loinc, '-') != 1 THEN 1",
                                "len(lab_loinc) - charindex(lab_loinc, '-') != 1 THEN 1"),
                          "  ELSE 0
            END as unexp_string
          FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`}", "{`table`}"),
                          ") s1
      ) s2
    GROUP BY code_type
      ) a
  INNER JOIN (
  SELECT
      'LC' as code_type, count(lab_loinc) as total
  FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") b on a.code_type = b.code_type
    ", .con = conn)
  }
  if (table == "IMMUNIZATION" | table == "IMMUNIZATION_STG") {
    sql <- glue::glue_sql("
      SELECT
        a.code_type, records, total, round(100*records/total, 2) AS pct
      FROM (
      SELECT
        code_type, sum(records) AS records
      FROM (
        SELECT
          vx_code_type as code_type, vx_code,
          sum(exceptn) AS records,
          unexp_alpha, unexp_length, unexp_numeric, unexp_string
        FROM (
          SELECT
            vx_code_type, vx_code, ", ifelse(backend == "Oracle",
                                             "greatest(unexp_alpha, unexp_length, unexp_numeric, unexp_string) AS exceptn, ",
                                             "CASE WHEN unexp_alpha + unexp_length + unexp_numeric + unexp_string > 1 THEN 1 ELSE 0 END AS exceptn, "),
                          "unexp_alpha, unexp_length, unexp_numeric, unexp_string
          FROM (
            SELECT
              vx_code_type, vx_code,
              CASE
                WHEN vx_code_type in ('CX', 'RX') AND ", ifelse(backend == "Oracle", "regexp_like(vx_code, '[a-zA-Z]') THEN 1 ",
                                                                "vx_code LIKE '[a-zA-Z]%' THEN 1 "),
                          "    ELSE 0
              END AS unexp_alpha,
              CASE
                WHEN vx_code_type = 'CH' AND ", ifelse(backend == "Oracle", "length(replace(vx_code, '.', '')) < 5 THEN 1 ",
                                                       "len(replace(vx_code, '.', '')) < 5 THEN 1 "),
                          "    WHEN vx_code_type = 'CX' AND ", ifelse(backend == "Oracle", "length(replace(vx_code, '.', '')) NOT IN (2, 3) THEN 1 ",
                                                                      "len(replace(vx_code, '.', '')) NOT IN (2, 3) THEN 1 "),
                          "    WHEN vx_code_type = 'RX' AND ", ifelse(backend == "Oracle", "length(replace(vx_code, '.', '')) NOT BETWEEN 2 AND 7 THEN 1",
                                                                      "len(replace(vx_code, '.', '')) NOT BETWEEN 2 AND 7 THEN 1 "),
                          "    ELSE 0
              END AS unexp_length,
              CASE
                WHEN vx_code_type IN ('CX', 'CH', 'RX') AND ", ifelse(backend == "Oracle", "regexp_like(vx_code, '\\d') THEN 0 ",
                                                                      "vx_code LIKE '%[0-9]%' THEN 0 "),
                          "    ELSE 1
              END AS unexp_numeric,
              CASE
                WHEN vx_code_type = 'CH' AND vx_code IN ('00000', '99999') THEN 1
                WHEN vx_code_type = 'CX' AND vx_code IN ('000', '999') THEN 1
                ELSE 0
              END AS unexp_string
            FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`}", "{`table`}"),
                          "
           WHERE vx_code_type IN ('CH', 'CX', 'RX')
				 ) s1
			 ) s2
        GROUP BY vx_code_type, vx_code, unexp_alpha, unexp_length, unexp_numeric, unexp_string
      ) s3
      GROUP BY code_type
    ) a
    INNER JOIN (
    SELECT
      vx_code_type, count(vx_code) as total
    FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "GROUP BY vx_code_type
    ) b on a.code_type = b.vx_code_type
    ", .con = conn)
  }
  if (table == "PRESCRIBING" | table == "PRESCRIBING_STG") {
    sql <- glue::glue_sql("
		      SELECT
		        a.code_type, records, total, 100*round(records/total, 2) as pct
		      FROM (
		      SELECT
		        code_type,
		        sum(exceptn) as records
		      FROM (
		        SELECT
		          rxnorm_cui, 'RX' as code_type, ",
                          ifelse(backend == "Oracle", "greatest(unexp_alpha, unexp_length, unexp_numeric, unexp_string) AS exceptn, ",
                                 "CASE WHEN unexp_alpha + unexp_length + unexp_numeric + unexp_string > 1 THEN 1 ELSE 0 END as exceptn, "),
                          "  unexp_alpha, unexp_length, unexp_numeric, unexp_string
		        FROM (
		          SELECT
		            rxnorm_cui,
		            CASE WHEN ", ifelse(backend == "Oracle", "regexp_like(rxnorm_cui, '[a-zA-Z]') THEN 1 ",
		                                "rxnorm_cui LIKE '[a-zA-Z]%' THEN 1 "),
                          "         ELSE 0
		            END AS unexp_alpha,
		            CASE WHEN ", ifelse(backend == "Oracle", "length(replace(rxnorm_cui, '.', '')) NOT BETWEEN 2 AND 7 THEN 1",
		                                "len(replace(rxnorm_cui, '.', '')) NOT BETWEEN 2 AND 7 THEN 1 "),
                          "         ELSE 0
		            END AS unexp_length,
		            '0' AS unexp_numeric, '0' as unexp_string
		        FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          "
				 ) s1
			 ) s2
		    GROUP BY code_type
		    ) a
		    INNER JOIN (
		      SELECT
		        'RX' as code_type, count(rxnorm_cui) as total
		      FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`} ", "{`table`} "),
                          ") b ON a.code_type = b.code_type
		    ", .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  return(result %>%
           mutate(text = glue::glue("{pct}% of {table} type {code_type} codes do not conform to the expected length or content."),
                  test = test,
                  result = as.numeric(pct),
                  threshold = 5) %>%
           mutate(result = ifelse(result > threshold, "FAIL", "PASS")) %>%
           select(text, test, result) %>%
           as_tibble())
}

primary_key_error <- function(table, field, test, schema = NULL, backend = NULL) {
  fields <- unlist(strsplit(field, split=", "))
  sql <- glue::glue_sql("
                        SELECT
                          COUNT(*)
                        FROM (
                          SELECT
                            {`fields`*}
                          FROM ", ifelse(backend == "Oracle", "{`schema`}.{`table`}", "{`table`}"),
                        "
                          GROUP BY {`fields`*}
                          HAVING COUNT(*) > 1
                        ) a
                        ", .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  txt <- ifelse(result == 0, glue::glue("Primary key(s) {field} in {table} is/are unique."),
                glue::glue("Primary key(s) {field} in {table} is/are not unique."))
  pass <- ifelse(result == 0, "PASS", "FAIL")
  return(tibble::tibble(text = as.character(txt),
                        test = test,
                        result = as.character(pass)
  )
  )
}

replication_error <- function(original, replication, key, field, test, schema = NULL, backend = NULL) {
  if (field == "ADMIT_DATE") {
    sql <- glue::glue_sql("
                        SELECT COUNT(*) FROM ",
                          ifelse(backend == "Oracle", "{`schema`}.{`original`} a
                               INNER JOIN {`schema`}.{`replication`} b ",
                                 "{`original`} a
                               INNER JOIN {`replication`} b "),
                          "on a.{`key`} = b.{`key`} ",
                          ifelse(backend == "Oracle", "WHERE to_char(a.{`field`}, 'YYYY-MM-DD') != to_char(b.{`field`}, 'YYYY-MM-DD')",
                                 "WHERE convert(char, a.{`field`}, 110) != convert(char, b.{`field`}, 110)"),
                          .con = conn)
  } else {
    sql <- glue::glue_sql("
                        SELECT COUNT(*) FROM ",
                        ifelse(backend == "Oracle", "{`schema`}.{`original`} a
                               INNER JOIN {`schema`}.{`replication`} b ",
                               "{`original`} a
                               INNER JOIN {`replication`} b "),
                        "on a.{`key`} = b.{`key`}
                        WHERE a.{`field`} != b.{`field`}",
                        .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  txt <- glue::glue("Field {field} from {replication} has {result} replication errors.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.numeric(result),
                        threshold = 0
  ) %>%
    mutate(result = ifelse(result > threshold, "FAIL", "PASS")) %>%
    select(text, test, result)
  )
}

required_fields <- function(test, schema = NULL, backend = NULL, version = NULL) {
  if (version == "4.1" | version == "4.1_STG") {
    metadata <- readr::read_csv('/app/inst/CDM_41_field_names.csv')
  }
  if (version == "5.1" | version == "5.1_STG" | version == "5.1_HP_STG") {
    metadata <- readr::read_csv('/app/inst/CDM_51_metadata.csv')
  }
  if (backend == "Oracle") {
    sql <- glue::glue_sql("
                      SELECT
                        table_name, column_name, data_type, column_id
                      FROM all_tab_columns
                      WHERE owner = {schema}
                      ORDER BY table_name, column_id asc
                      ", .con = conn)
  } else if (backend == "mssql") {
    sql <- glue::glue_sql("
                          SELECT
                            table_name as TABLE_NAME, column_name as COLUMN_NAME
                          FROM information_schema.columns
                          WHERE table_schema = 'dbo'
                          ", .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  return(
    metadata %>%
      anti_join(., result, by = c("Table" = "TABLE_NAME", "Field" = "COLUMN_NAME")) %>%
      select(Table, Field, Required)  %>%
      filter(Required == "Y") %>%
      arrange(Table) %>%
      mutate(text = glue::glue("Required field {Field} is not present."),
             test = test,
             result = ifelse(!is.na(text), "FAIL", "PASS")) %>%
      select(text, test, result)
  )
}

required_tables <- function(test, required = NULL, schema = NULL, backend = NULL) {
  n_required <- length(required)
  if (backend == "Oracle") {
    sql <- glue::glue_sql("
                        SELECT
                          table_name
                        FROM all_tables
                        WHERE owner = {schema}
                        ", .con = conn)
  } else if (backend == "mssql") {
    sql <- glue::glue_sql("
                          SELECT
                            table_name
                          FROM information_schema.tables
                          WHERE table_schema = 'dbo'
                          ", .con = conn)
  }
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  table_list <- result %>% pull
  schema_intersect <- length(intersect(required, table_list))
  if (n_required == schema_intersect) {
    txt <- glue::glue("All required tables are present.")
    return(
      tibble::tibble(
        text = txt,
        test = test,
        result = "PASS"
      )
    )
  } else {
    missing <- setdiff(required, table_list)
    txt <- glue::glue("Required table {missing} is not present.")
    return(
      tibble::tibble(
        text = txt,
        test = test,
        result = "FAIL"
      )
    )
  }
}

tables_populated <- function(test, tables = NULL, schema = NULL, backend = NULL) {
  if (backend == "Oracle") {
    sql <- glue::glue_sql("
                          SELECT
                            table_name, num_rows
                          FROM all_tables
                          WHERE owner = {schema}
                          AND table_name IN ({tables*})
                          ", .con = conn, tables = tables)
    query <- DBI::dbSendQuery(conn, sql)
    result <- DBI::dbFetch(query)
    DBI::dbClearResult(query)
    return(
      result %>%
             mutate(text = glue::glue("{TABLE_NAME} has {NUM_ROWS} rows."),
                    test = test,
                    result = as.numeric(NUM_ROWS),
                    threshold = 0) %>%
             mutate(result = ifelse(result > threshold, "PASS", "FAIL")) %>%
             select(text, test, result) %>%
             as_tibble()
      )
  } else if (backend == "mssql") {
    sql <- glue::glue_sql("
                          SELECT
                            o.name, ddps.row_count
                          FROM sys.indexes AS i
                          INNER JOIN sys.objects AS o ON i.object_id = o.object_id
                          INNER JOIN sys.dm_db_partition_stats AS ddps ON i.object_id = ddps.object_id
                          AND i.index_id = ddps.index_id
                          WHERE i.index_id < 2 and o.is_ms_shipped = 0
                          AND o.name IN ({tables*})
                          ORDER BY o.name
                          ", .con = conn, tables = tables)
    query <- DBI::dbSendQuery(conn, sql)
    result <- DBI::dbFetch(query)
    DBI::dbClearResult(query)
    return(
      result %>%
        mutate(text = glue::glue("{name} has {row_count} rows."),
               test = test,
               result = as.numeric(row_count),
               threshold = 0) %>%
        mutate(result = ifelse(result > threshold, "PASS", "FAIL")) %>%
        select(text, test, result) %>%
        as_tibble()
    )
  }
}

value_validation <- function(table, field, test, schema = NULL, backend = NULL, version = NULL) {
  valueset <- get_valueset(table, field, version = version)
  sql <- glue::glue_sql("
                        SELECT SUM(CASE WHEN {`field`} NOT IN ({vals*}) THEN 1 ELSE 0 END) ",
                        ifelse(backend == "Oracle",
                               "FROM {`schema`}.{`table`}",
                               "FROM {`table`}"),
                        vals = valueset, .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  txt <- glue::glue("Field {field} from {table} has {result} invalid records")
  if (as.numeric(result) > 0 & !is.na(as.numeric(result))) {
    df <- count_distinct_invalids(table, field, test, schema = schema, backend = backend, version = version)
    readr::write_csv(df, paste0('/app/unit_tests/invalid_values/', field, '_', format(Sys.time(), "%m%d%Y"), '.csv'))
    if (field == "RESULT_UNIT" | field == "OBSCLIN_RESULT_UNIT" | field == "OBSGEN_RESULT_UNIT") {
      df %>%
        select(field) %>%
        pull %>%
        purrr::map_df(check_ucum_api) %>%
        readr::write_csv(., paste0('/app/unit_tests/invalid_values/', field, '_ucum_api_', format(Sys.time(), "%m%d%Y"), '.csv'))
    }
  }
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.numeric(result),
                        threshold = 0
  ) %>%
    mutate(result = ifelse(result > threshold, "FAIL", "PASS")) %>%
    select(text, test, result)
  )
}

value_validation_db <- function(table, field, test, cdm_schema = NULL, ref_schema = NULL, ref_table = NULL, backend = NULL, version = NULL) {
  if (version == "5.1_STG" | version == "5.1_HP_STG") {
    ref_table_filter <- gsub("_STG", "", table)
  } else {
    ref_table_filter <- table
  }
  result <- {if(backend == "Oracle") tbl(conn, in_schema(cdm_schema, table)) else tbl(conn, table)} %>%
    select(field) %>%
    rename(VALUESET_ITEM = field) %>%
    anti_join({if(backend == "Oracle") tbl(conn, in_schema(ref_schema, ref_table)) else tbl(conn, ref_table)} %>%
                filter(TABLE_NAME == ref_table_filter & FIELD_NAME == field),
              by = "VALUESET_ITEM"
    ) %>%
    filter(!is.na(VALUESET_ITEM)) %>%
    count() %>%
    pull
  if (result > 0) {
    df <- {if(backend == "Oracle") tbl(conn, in_schema(cdm_schema, table)) else tbl(conn, table)} %>%
      select(field) %>%
      rename(VALUESET_ITEM = field) %>%
      anti_join({if(backend == "Oracle") tbl(conn, in_schema(ref_schema, ref_table)) else tbl(conn, ref_table)} %>%
                  filter(TABLE_NAME == ref_table_filter & FIELD_NAME == field),
                by = "VALUESET_ITEM"
      ) %>%
      filter(!is.na(VALUESET_ITEM)) %>%
      group_by(VALUESET_ITEM) %>%
      count %>% arrange(-n) %>% collect %T>%
      readr::write_csv(., paste0('/app/unit_tests/invalid_values/', field, '_', format(Sys.time(), "%m%d%Y"), '.csv'))
    if (field == "RESULT_UNIT" | field == "OBSCLIN_RESULT_UNIT" | field == "OBSGEN_RESULT_UNIT") {
      df %>%
        select(VALUESET_ITEM) %>%
        pull %>%
        purrr::map_df(check_ucum_api) %>%
        readr::write_csv(., paste0('/app/unit_tests/invalid_values/', field, '_ucum_api_', format(Sys.time(), "%m%d%Y"), '.csv'))
    }
  }
  txt <- glue::glue("Field {field} from {table} has {result} invalid records")

  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.numeric(result),
                        threshold = 0) %>%
           mutate(result = ifelse(result > threshold, "FAIL", "PASS")) %>%
           select(text, test, result)
  )
}

perform_unit_tests <- function(table, field, test, schema = NULL, ref_schema = NULL, ref_table = NULL, backend = NULL, version = NULL) {
  print(table)
  print(field)
  print(test)
  if (version == "4.1" | version == "4.1_STG") {
    required <- c('DEMOGRAPHIC', 'ENROLLMENT', 'ENCOUNTER',
                  'DIAGNOSIS', 'PROCEDURES', 'VITAL', 'DISPENSING',
                  'LAB_RESULT_CM', 'CONDITION', 'PRO_CM', 'PRESCRIBING',
                  'PCORNET_TRIAL', 'DEATH', 'DEATH_CAUSE', 'HARVEST',
                  'PROVIDER', 'MED_ADMIN', 'OBS_CLIN', 'OBS_GEN')
    populated <- c('DEMOGRAPHIC', 'ENROLLMENT', 'ENCOUNTER', 'DIAGNOSIS', 'PROCEDURES', 'HARVEST')
  }
  if (version == "5.1" | version == "5.1_STG" | version == "5.1_HP_STG") {
    required <- c('DEMOGRAPHIC', 'ENROLLMENT', 'ENCOUNTER',
      'DIAGNOSIS', 'PROCEDURES', 'VITAL', 'DISPENSING',
      'LAB_RESULT_CM', 'CONDITION', 'PRO_CM', 'PRESCRIBING',
      'PCORNET_TRIAL', 'DEATH', 'DEATH_CAUSE', 'HARVEST',
      'PROVIDER', 'MED_ADMIN', 'OBS_CLIN', 'OBS_GEN',
      'HASH_TOKEN', 'IMMUNIZATION', 'LDS_ADDRESS_HISTORY')
    populated <- c('DEMOGRAPHIC', 'ENROLLMENT', 'ENCOUNTER', 'DIAGNOSIS', 'PROCEDURES', 'HARVEST')
  }
  if (test == "DC 1.01") {
    required_tables(test = test, required = required,
                    schema = schema, backend = backend)
  } else if (test == "DC 1.02") {
    tables_populated(test, tables = populated,
                     schema = schema, backend = backend)
  } else if (test == "DC 1.03") {
    required_fields(test, schema = schema, backend = backend, version = version)
  } else if (test == "DC 1.04") {
    field_conformance(test, schema = schema, backend = backend, version = version)
  } else if (test == "DC 1.05") {
    primary_key_error(table, field, test, schema = schema, backend = backend)
  } else if (test == "DC 1.06") {
    if (USE_LOOKUP_TBL == "Y") {
      value_validation_db(table, field, test = test, cdm_schema = schema, ref_schema = ref_schema, ref_table = ref_table, backend = backend, version = version)
    } else {
      value_validation(table, field, test = test, schema = schema, backend = backend, version = version)
    }
  } else if (test == "DC 1.07") {
    missing_or_unknown(table, field, test = test, threshold = 0, schema = schema, backend = backend)
  } else if (test == "DC 1.08") {
    orphans(table, "DEMOGRAPHIC", "PATID", test = test, schema = schema, backend = backend, version = version)
  } else if (test == "DC 1.09") {
    if (version == "4.1_STG" | version == "5.1_STG" | version == "5.1_HP_STG") {
      orphans(table, "ENCOUNTER_STG", "ENCOUNTERID", test = test, schema = schema, backend = backend, version = version)
    } else {
      orphans(table, "ENCOUNTER", "ENCOUNTERID", test = test, schema = schema, backend = backend, version = version)
    }
  } else if (test == "DC 1.10") {
    if (version == "4.1_STG" | version == "5.1_STG" | version == "5.1_HP_STG") {
      replication_error("ENCOUNTER_STG", table, "ENCOUNTERID", field, test = test, schema = schema, backend = backend)
    } else {
      replication_error("ENCOUNTER", table, "ENCOUNTERID", field, test = test, schema = schema, backend = backend)
    }
  } else if (test == "DC 1.11") {
    record_duplication(table, field, test, schema = schema, backend = backend)
  } else if (test == "DC 1.12") {
    orphans(table, "PROVIDER", key = field, test = test, schema = schema, backend = backend, version = version)
  } else if (test == "DC 1.13") {
    potential_code_error(table, test = test, schema = schema, backend = backend)
  } else if (test == "DC 2.02") {
    extreme_values(table, field, test = test, schema = schema, backend = backend, version = version)
  } else if (test == "DC 3.01") {
    events_per_encounter(table, field, c('09', '10', '11', 'SM'), "diagnosis records with known DX_TYPE", test = test, schema = schema, backend = backend)
  } else if (test == "DC 3.02") {
    events_per_encounter(table, field, c('09', '10', '11', 'CH', 'LC', 'ND', 'RE'), "procedure records with known PX_TYPE", test = test, schema = schema, backend = backend)
  } else if (test == "DC 3.03") {
    missing_or_unknown(table, field, test = test, threshold = 10, schema = schema, backend = backend)
  } else if (test == "DC 3.04") {
    patients_per_encounter(table, field, test = test, schema = schema, backend = backend, version = version)
  } else if (test == "DC 3.05") {
    patients_per_encounter(table, field, test = test, schema = schema, backend = backend, version = version)
  } else if (test == "DC 3.07") {
    data_latency(table, field, test = test, schema = schema, backend = backend)
  } else if (test == "DC 3.10") {
    normal_range_specification(table, field, test = test, schema = schema, backend = backend)
  } else if (test == "DC 3.11") {
    data_latency(table, field, test = test, schema = schema, backend = backend)
  }
}

### Data Characterization summary functions

is.character.Date <- function(x) {
  (is.character(x) | lubridate::is.Date(x) | lubridate::is.instant(x))
}

summarize_distribution <- function(conn, backend, schema, table, field) {
  field <- sym(field)
  {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table)) else tbl(conn, table)} %>%
    mutate(p05 = order_by(field, quantile(field, 0.05)),
           p25 = order_by(field, quantile(field, 0.25)),
           median = order_by(field, median(field)),
           p75 = order_by(field, median(field)),
           p95 = order_by(field, quantile(field, 0.95))) %>%
    select(p05, p25, median, p75, p95) %>%
    rename_at(.vars = vars(everything()), list( ~ paste0(gsub("\\_", "", field), '_', .))) %>%
    head(1) %>%
    collect()
}

generate_summary <- function(conn, backend = NULL, version = NULL, schema = NULL, table = NULL,
                             filtered = FALSE, field = NULL, value = NULL) {
  #' generate_summary()
  #' Arguments:
  #' conn = DBI connection object,
  #' backend: name of sql backend (either Oracle or MSSQL)
  #' schema: name of db/schema, required argument if using Oracle
  #' table: name of table to analyze
  #' filtered: set flag to TRUE if running data characterization over a subset of data,
  #'           (for instance over a subset of LOINC codes)
  #' field: name of column to filter over, required if filtered = TRUE
  #' value: name of value to filter in given field, required if filtered = TRUE
  if (version == "4.1" | version == "4.1_STG") {
    metadata <- readr::read_csv('./inst/CDM_41_field_names.csv')
  }
  if (version == "5.1" | version == "5.1_STG") {
    metadata <- readr::read_csv('./inst/CDM_51_metadata.csv')
  }
  # get column info to decide what queries to run
  rs <- DBI::dbSendQuery(conn, paste0("SELECT * FROM ", ifelse(backend=="Oracle", paste0(schema, ".", table), table)))
  colinfo <- DBI::dbColumnInfo(rs)
  DBI::dbClearResult(rs)
  # initiate queries on given table in schema
  {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table)) else tbl(conn, table)} %>%
    {if(filtered == TRUE) filter(., rlang::sym(field) == value) else .} %>%
    rename_at(.vars = vars(contains("RAW")), .funs = list(~gsub("\\RAW", "R", .))) %>%
    rename_at(.vars = vars(contains("_")), .funs = list(~gsub("\\_", "", .))) %>%
    summarize_if(is.character.Date, list(cn = ~ count(), nd = n_distinct, min = min, max = max,
                                         nNULL = ~ sum(case_when(is.na(.) ~ 1, !is.na(.) ~ 0)),
                                         nNI = ~ sum(case_when(as.character(.) %in% c('NI', 'UN', 'OT') ~ 1,
                                                               !(as.character(.) %in% c('NI', 'UN', 'OT')) ~ 0,
                                                               is.na(.) ~ 0)))) %>%
    mutate_all(as.character) %>%
    collect() %>%
    bind_cols(
      {if(sum(1*(colinfo$type==6))>0)
      {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table)) else tbl(conn, table)} %>%
          rename_at(.vars = vars(contains("RAW")), .funs = list(~gsub("\\RAW", "R", .))) %>%
          rename_at(.vars = vars(contains("_")), .funs = list(~gsub("\\_", "", .))) %>%
          select(., -contains("DATE"), -contains("TIME")) %>%
          summarize_if(is.numeric, list(cn = ~ count(), nNULL = ~ sum(case_when(is.na(.) ~ 1, !is.na(.) ~ 0)),
                                        nd = n_distinct, min = min, mean = mean, max = max)) %>%
          collect() %>%
          {if (sum(1*(colinfo$type==6))==1) rename_all(., .funs = list(~paste0(gsub("\\_", "", colinfo[colinfo$type==6,]$name), "_", .))) else . } %>%
          bind_cols(
            {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table)) else tbl(conn, table)} %>% select_if(is.numeric) %>% head(5) %>% collect() %>% names %>%
              map(~ summarize_distribution(conn, backend, schema, table, field = .x)) %>%
              reduce(cbind)
          ) else tibble() }
    ) %>%
    gather(var, val) %>%
    separate(var, c('key', 'var')) %>%
    spread(var, val) %>%
    mutate(pct_null = round(as.numeric(nNULL) / as.numeric(cn), 3) * 100,
           pct_dist = round(as.numeric(nd) / as.numeric(cn), 3) * 100,
           pct_missing = round(as.numeric(nNI) / as.numeric(cn), 3) * 100) %>%
    left_join(., readr::read_csv('./inst/CDM_51_metadata.csv') %>%
                filter(Table == table) %>%
                select(key, Field:Required),
              by = 'key') %>%
    arrange(Order) %>%
    select(-key, -Order) %>%
    select(Field, Required, everything()) %T>%
    purrr::when(filtered == TRUE ~ write.csv(.,
                                             file = paste0('./summaries/CSV/', table, "_", field, "_", value, '.csv'),
                                             row.names = FALSE),
                ~ write.csv(., file = paste0('./summaries/CSV/', table, '.csv'), row.names = FALSE)
    ) %>%
    purrr::when(sum(1*(colinfo$type==6))>0
                ~ select(., Field, Required, cn, nd, pct_dist, nNULL, pct_null, nNI, pct_missing,
                         min, p05, p25, median, mean, p75, p95, max) %>%
                  tibble::column_to_rownames(var = "Field") %>%
                  DT::datatable(options = list(dom = 't', displayLength = -1),
                                colnames = c("Required", "N", "Distinct N", "Distinct %", "Null N", "Null %",
                                             "Missing, NI, UN, or OT N", "Missing, NI, UN, or OT %", "Min", "5th Percentile",
                                             "25th Percentile", "Median", "Mean", "75th Percentile",
                                             "95th Percentile", "Max")
                  ),
                ~ select(., Field, Required, cn, nd, pct_dist, nNULL, pct_null, nNI, pct_missing, min, max) %>%
                  tibble::column_to_rownames(var = "Field") %>%
                  DT::datatable(options = list(dom = 't', displayLength = -1),
                                colnames = c("Required", "N", "Distinct N", "Distinct %", "Null N", "Null %",
                                             "Missing, NI, UN, or OT N", "Missing, NI, UN, or OT %", "Min", "Max")
                  )
    ) %>%
    purrr::when(filtered == TRUE ~ htmlwidgets::saveWidget(.,
                                                           paste0(normalizePath('./summaries/HTML'), '/', table, '_', field, '_', value, '.html'),
                                                           selfcontained = FALSE),
                ~ htmlwidgets::saveWidget(., paste0(normalizePath('./summaries/HTML'), '/', table, '.html'),
                                          selfcontained = FALSE)
    )
}
