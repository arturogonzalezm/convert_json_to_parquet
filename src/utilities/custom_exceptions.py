"""
Custom exceptions for the ETL job.
"""


class ETLJobError(Exception):
    """
    Exception raised for errors that occur during the ETL job.

    Attributes:
    - message (str): Explanation of the error.
    - stage (str): The stage of the ETL job where the error occurred (e.g., "extract", "transform", "load").
    - details (str): Additional details about the error.
    """

    def __init__(self, message, stage=None, details=None):
        self.message = message
        self.stage = stage
        self.details = details
        super().__init__(message)

    def __str__(self):
        return f"ETLJobError: {self.message}"


class SparkSessionError(Exception):
    """
    Exception raised for errors related to the Spark session.

    Attributes:
    - message (str): Explanation of the error.
    - component (str): The Spark component where the error occurred (e.g., "SparkSession", "DataFrame").
    - details (str): Additional details about the error.
    """

    def __init__(self, message, component=None, details=None):
        self.message = message
        self.component = component
        self.details = details
        super().__init__(message)

    def __str__(self):
        return f"SparkSessionError: {self.message}"
