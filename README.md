# Luchtmeetnet Air Quality ETL Pipeline

An **Apache Airflow ETL pipeline** that fetches hourly air quality measurements from
the [Luchtmeetnet API](https://api.luchtmeetnet.nl/open_api), validates and cleans data,
and loads to a **PostgreSQL** database for downstream analysis.

## Overview

This project consisits of an end-to-end **data engineering workflow** built with:
- **Apache Airflow** for workflow orchestration (in Docker)
- **PostgreSQL** for data storage
- **Pandas** for data transformation and validation
- **Docker** for environment management and containerization

### DAG: [Luchtmeetnet_ETL](https://github.com/wheff70/LuchtmeetnetDataPipeline/blob/dags/luchtmeetnet_ETL.py)

**Workflow**:
1. **Extract**: Fetch hourly air quality data from the Luchtmeetnet API.
2. **Validate**: Verify data quality, quarantine invalid data, and log data quality metrics.
3. **Load**: Insert validated data into a Postgres table for downstream applications.

