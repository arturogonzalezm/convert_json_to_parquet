from pyspark.sql import SparkSession


class SparkSessionManager:
    _spark = None

    @classmethod
    def get_spark_session(cls, app_name):
        if cls._spark is None:
            cls._spark = SparkSession.builder.appName(app_name).getOrCreate()
        return cls._spark

    @classmethod
    def stop_spark_session(cls):
        if cls._spark is not None:
            cls._spark.stop()
            cls._spark = None
