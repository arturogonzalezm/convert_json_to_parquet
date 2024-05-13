"""
Main script to run the ETL job.
"""

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
