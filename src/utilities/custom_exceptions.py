"""
This module contains custom exceptions that are raised during the ETL job.
"""


class ETLJobError(Exception):
    """
    Exception raised for errors that occur during the ETL job.

    Attributes:
    - message (str): Explanation of the error.
    """

    pass


class SparkSessionError(Exception):
    """
    Exception raised for errors related to the Spark session.

    Attributes:
    - message (str): Explanation of the error.
    """

    pass
