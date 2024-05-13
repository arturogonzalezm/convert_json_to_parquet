[![codecov](https://codecov.io/gh/arturogonzalezm/convert_json_to_parquet/graph/badge.svg?token=6GBJPNE0BB)](https://codecov.io/gh/arturogonzalezm/convert_json_to_parquet)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://github.com/arturogonzalezm/convert_json_to_parquet/blob/master/LICENSE)
[![PyLint](https://github.com/arturogonzalezm/convert_json_to_parquet/actions/workflows/workflow.yml/badge.svg)](https://github.com/arturogonzalezm/convert_json_to_parquet/actions/workflows/pylint.yml)

# Convert JSON to Parquet - PySpark

This project demonstrates a structured approach to building an ETL (Extract, Transform, Load) job using Python, PySpark, and custom exceptions. 
The code is organised into separate modules for logging, custom exceptions, Spark session management, and the ETL job itself.

## Modules Overview

---
**custom_exceptions.py**


### Defines two custom exception classes:

**ETLJobError:** Raised when errors occur during the ETL job.

**SparkSessionError:** Raised when errors occur related to the Spark session.


Each exception class has attributes for message, stage (for ETLJobError), component (for SparkSessionError), and details.

---

**logger.py**

### Provides logging functions:

**log_error(message):** Logs an error message.

**log_info(message):** Logs an info message.


Uses Python's built-in logging module with a basic configuration.

---

**spark_session.py**

### Manages the creation and stopping of Spark sessions:

**get_spark_session(app_name):** Creates or retrieves a Spark session.

**stop_spark_session():** Stops the current Spark session if it exists.

---

**etl_job.py**

### Performs ETL operations using PySpark:

**extract_json(spark, input_file_path):** Extracts data from a JSON file.

**transform(df):** Transforms the extracted data by renaming column headers to uppercase.

**load(df, output_file_path):** Loads the transformed data into a Parquet file.

**run(spark, input_file_path_json, output_file_path_parquet):** Runs the ETL job.

---

**main.py**

### Usage:

```python
from src.etl_job import ETLJob
from src.spark_session import SparkSessionManager
from src.utilities.custom_exceptions import SparkSessionError, ETLJobError
from src.utilities.logger import log_error


def main():
    """
    Main function to run the ETL job.
    """
    input_file_path_json = 'data/input/craiglist.json'
    output_file_path_parquet = 'data/output/craiglist.parquet'

    try:
        # Create or get SparkSession
        spark = SparkSessionManager.get_spark_session("Read JSON")

        # Run ETL job
        ETLJob.run(spark, input_file_path_json, output_file_path_parquet)

    except SparkSessionError as e:
        log_error(f"SparkSessionError: {e}")
        return

    except ETLJobError as e:
        log_error(f"ETLJobError: {e}")
        return

    finally:
        # Stop SparkSession
        SparkSessionManager.stop_spark_session()


if __name__ == "__main__":
    main()
```

---

## ETL Job Pipeline

```mermaid
graph LR
    A[Main Script] -->|create/get spark session| B[SparkSessionManager]
    B -->|getOrCreate SparkSession| C[SparkSession]
    A -->|run ETL job| D[ETLJob]
    D -->|extract JSON data| E[extract_json]
    E -->|transform data| F[transform]
    F -->|load data into Parquet file| G[load]
    D -->|log error| H[Logger]
    B -->|log error| H
    A -->|stop spark session| I[SparkSessionManager.stop_spark_session]

    classDef default fill:#f9f,stroke:#333,stroke-width:4px;
    class ETLJob,extract_json,transform,load fill:#cce,stroke:#333,stroke-width:2px;
    class SparkSessionManager,Logger fill:#ddf,stroke:#333,stroke-width:2px;
```

## Conclusion

This project demonstrates a structured approach to building an ETL job using Python, PySpark, and custom exceptions. 
The code is organised into separate modules for logging, custom exceptions, Spark session management, and the ETL job itself.
