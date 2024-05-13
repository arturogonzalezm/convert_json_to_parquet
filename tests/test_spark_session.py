from src.spark_session import SparkSessionManager


def test_get_spark_session():
    # Test getting the SparkSession
    app_name = "Test"
    spark = SparkSessionManager.get_spark_session(app_name)
    assert spark is not None
    assert spark.conf.get("spark.app.name") == app_name

    # Test getting the same SparkSession instance
    spark2 = SparkSessionManager.get_spark_session("Another Test")
    assert spark == spark2


def test_stop_spark_session():
    # Test stopping the SparkSession
    SparkSessionManager.stop_spark_session()
    assert SparkSessionManager._spark is None
