from abc import abstractmethod

from pyspark.sql import DataFrame

from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class AbstractValidator:
    @abstractmethod
    def validate(self, df: DataFrame)->DataFrameValidatorReport:
        pass
