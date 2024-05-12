from src.etl import ETLJob
from src.spark_session import SparkSessionManager


def main():
    INPUT_FILE_PATH_JSON = 'data/input/craiglist.json'
    OUTPUT_FILE_PATH_PARQUET = 'data/output/craiglist.parquet'

    # Create or get SparkSession
    spark = SparkSessionManager.get_spark_session("Read JSON")

    # Run ETL job
    ETLJob.run(spark, INPUT_FILE_PATH_JSON, OUTPUT_FILE_PATH_PARQUET)

    # Stop SparkSession
    SparkSessionManager.stop_spark_session()


if __name__ == "__main__":
    main()
