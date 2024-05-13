from src.etl import ETLJob
from src.spark_session import SparkSessionManager
from src.utilities.custom_exceptions import SparkSessionError, ETLJobError
from src.utilities.logger import log_error


def main():
    INPUT_FILE_PATH_JSON = 'data/input/craiglist.json'
    OUTPUT_FILE_PATH_PARQUET = 'data/output/craiglist.parquet'

    try:
        # Create or get SparkSession
        spark = SparkSessionManager.get_spark_session("Read JSON")

        # Run ETL job
        ETLJob.run(spark, INPUT_FILE_PATH_JSON, OUTPUT_FILE_PATH_PARQUET)

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
