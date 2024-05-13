from pyspark.sql import SparkSession

from src.utilities.custom_exceptions import SparkSessionError
from src.utilities.logger import log_error


class SparkSessionManager:
    _spark = None

    @classmethod
    def get_spark_session(cls, app_name):
        if cls._spark is None:
            try:
                cls._spark = SparkSession.builder.appName(app_name).getOrCreate()
            except Exception as e:
                log_error(f"Failed to create Spark session: {e}")
                raise SparkSessionError("Failed to create Spark session") from e
        return cls._spark

    @classmethod
    def stop_spark_session(cls):
        if cls._spark is not None:
            cls._spark.stop()
            cls._spark = None
