from abc import abstractmethod
from pyspark.sql import DataFrame


class AbstractValidator:
    @abstractmethod
    def validate(self, df: DataFrame):
        pass
