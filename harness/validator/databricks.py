from io import StringIO
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from harness.config.config import ValidatorConfig
from datacompy import SparkCompare
from harness.validator.validator import AbstractValidator
from datetime import datetime


class DataFrameValidatorReport(BaseModel):
    summary: str
    missmatch_sample: str
    validation_date: datetime


class DataFrameValidator(AbstractValidator):
    def __init__(self, config: ValidatorConfig):
        self._config: ValidatorConfig = config

    def validate(self, canidate: DataFrame, master: DataFrame, session: SparkSession):
        """
        Validate the data frame
        Args:
            df (DataFrame): Data frame to validate
        """
        comparison = SparkCompare(
            session, master, canidate, join_columns=self._config.join_keys
        )
        comparison_result = StringIO()
        comparison.report(comparison_result)
        missmatch_both: DataFrame = comparison.rows_both_mismatched
        missmatch_sample = missmatch_both.limit(10).toPandas()

        return DataFrameValidatorReport(
            summary=comparison_result,
            missmatch_sample=missmatch_sample.to_json(),
            validation_date=datetime.now(),
        )
