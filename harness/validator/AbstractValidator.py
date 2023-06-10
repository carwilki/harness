from pyspark.sql import DataFrame


from abc import abstractmethod


class AbstractValidator:
    @abstractmethod
    def validate(self, df: DataFrame):
        pass