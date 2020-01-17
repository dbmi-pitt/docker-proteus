# docker-proteus
Dockerized version of Proteus data characterization suite

## Installation
Create a directory for config files and output

```$ mkdir <your-proteus-folder-name>```

1. Create a configuration file for proteus inside this folder:

```
export server=your-rdbms-hostname.edu
export db=your-db-name
export backend=Oracle-or-mssql
export user=your-user-name
export pass=your-password
export summarize=N
export cdm_schema=your-schema-name
export cdm_version=5.1
export use_ref_tbl=N
export ref_schema=
```

If you want to run valueset validation directly against the db instead of the parseable CSV, set `export use_ref_tbl=Y` and `export ref_schema=schema-where-ref-table-is-located`

2. Pull the image from Docker Hub:

```
$ docker pull pohalloran/proteus
$ docker tag pohalloran/proteus proteus
```

## Launching proteus

To run the data curation test suite, make sure `summarize=N` in your configuration file and then run:

```$ docker run -v /full-path-to-config-file:/app/.env /full-path-to-proteus-folder/results:/app/unit_tests/ proteus```

To run CDM table summarizations, first set `summarize=Y` in your configuration file and then run:

```$ docker run -v /full-path-to-config-file:/app/.env /full-path-to-proteus-folder/results:/app/summaries/ proteus```
