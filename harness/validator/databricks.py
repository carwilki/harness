from pyspark.sql import DataFrame, SparkSession

from harness.validator.validator import AbstractValidator


class DataFrameValidator(AbstractValidator):
    @classmethod
    def validate(self, canidate: DataFrame, master: DataFrame, session: SparkSession):
        """
        Validate the data frame
        Args:
            df (DataFrame): Data frame to validate
        """
        pass
