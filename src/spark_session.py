"""
Create Spark Session Manager class to manage the creation and stopping of Spark sessions.
"""

from pyspark.sql import SparkSession

from src.utilities.custom_exceptions import SparkSessionError
from src.utilities.logger import log_error


class SparkSessionManager:
    """
    A class to manage the creation and stopping of Spark sessions.
    """

    _spark = None

    @classmethod
    def get_spark_session(cls, app_name):
        """
        Get a Spark session. If a session does not exist, create a new one.

        Args:
        - app_name (str): The name of the Spark application.

        Returns:
        - SparkSession: The Spark session.
        """
        if cls._spark is None:
            try:
                cls._spark = SparkSession.builder.appName(app_name).getOrCreate()
            except Exception as e:
                log_error(f"Failed to create Spark session: {e}")
                raise SparkSessionError("Failed to create Spark session") from e
        return cls._spark

    @classmethod
    def stop_spark_session(cls):
        """
        Stop the current Spark session if it exists.
        """
        if cls._spark is not None:
            cls._spark.stop()
            cls._spark = None
