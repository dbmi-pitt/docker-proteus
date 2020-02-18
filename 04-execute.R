library(dotenv)
load_dot_env(file = "/app/.env")
if (Sys.getenv("summarize") == "Y") {
  if (Sys.getenv("backend") == "Oracle") {
    system(paste0("sed -i 's/${server}/", Sys.getenv("server"), "/g' /etc/odbc.ini"))
    system(paste0("sed -i 's/${db}/", Sys.getenv("db"), "/g' /etc/odbc.ini"))
    source('/app/03-summarize-tables-oracle.R')
  }
  if (Sys.getenv("backend") == "mssql") {
    source('/app/03-summarize-tables-mssql.R')
  }
	if (Sys.getenv("backend") == "postgres") {
		print("This functionality is not currently supported on PostgreSQL.")
	}
	if (Sys.getenv("backend") == "mysql") {
		print("This functionality is not currently supported on MySQL.")
	}
} else {
  if (Sys.getenv("backend") == "Oracle") {
    system(paste0("sed -i 's/${server}/", Sys.getenv("server"), "/g' /etc/odbc.ini"))
    system(paste0("sed -i 's/${db}/", Sys.getenv("db"), "/g' /etc/odbc.ini"))
    source('/app/02-unit-tests-oracle.R')
  }
  if (Sys.getenv("backend") == "mssql") {
    source('/app/02-unit-tests-mssql.R')
  }
	if (Sys.getenv("backend") == "postgres") {
		source('/app/02-unit-tests-postgres.R')
	}
	if (Sys.getenv("backend") == "mysql") {
		source('/app/02-unit-tests-mysql.R')
	}
}
