[![codecov](https://codecov.io/gh/arturogonzalezm/convert_json_to_parquet/graph/badge.svg?token=6GBJPNE0BB)](https://codecov.io/gh/arturogonzalezm/convert_json_to_parquet)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://github.com/arturogonzalezm/convert_json_to_parquet/blob/master/LICENSE)
[![PyLint](https://github.com/arturogonzalezm/convert_json_to_parquet/actions/workflows/workflow.yml/badge.svg)](https://github.com/arturogonzalezm/convert_json_to_parquet/actions/workflows/pylint.yml)

# Convert JSON to Parquet - PySpark

This project demonstrates a structured approach to building an ETL (Extract, Transform, Load) job using Python, PySpark, and custom exceptions. 
The code is organised into separate modules for logging, custom exceptions, Spark session management, and the ETL job itself.

## Modules Overview

```bash
custom_exceptions.py
```


### Defines two custom exception classes:

*ETLJobError:* Raised when errors occur during the ETL job.

*SparkSessionError:* Raised when errors occur related to the Spark session.


Each exception class has attributes for message, stage (for ETLJobError), component (for SparkSessionError), and details.

```bash
logger.py
```

### Provides logging functions:

*log_error(message):* Logs an error message.

*log_info(message):* Logs an info message.


### Uses Python's built-in logging module with a basic configuration.


```bash
spark_session.py
```

### Manages the creation and stopping of Spark sessions:

*get_spark_session(app_name):* Creates or retrieves a Spark session.

*stop_spark_session():* Stops the current Spark session if it exists.


```bash
etl_job.py
```

### Performs ETL operations using PySpark:

*extract_json(spark, input_file_path):* Extracts data from a JSON file.

*transform(df):* Transforms the extracted data by renaming column headers to uppercase.

*load(df, output_file_path):* Loads the transformed data into a Parquet file.

*run(spark, input_file_path_json, output_file_path_parquet):* Runs the ETL job.

```bash
main.py
```

### The main script that runs the ETL job:

Creates or retrieves a Spark session using SparkSessionManager.

Runs the ETL job using ETLJob.

Catches and logs custom exceptions using logger.

## Diagram

```mermaid
graph LR
  A[Main] -->|create/retrieve|> B[SparkSessionManager]
  B -->|get spark session|> C[SparkSession]
  C -->|extract json|> D[ExtractJSON]
  D -->|transform|> E[Transform]
  E -->|load parquet|> F[LoadParquet]
  F -->|stop spark session|> G[StopSparkSession]
  A -.->|catch exceptions|-.> H[Logger]
    H -->|log error|> I[Error]
    H -->|log info|> J[Info]
```
