from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class AbstractTarget(ABC):
    def __init__(self, session: SparkSession):
        self.session = session

    @abstractmethod
    def write(self, df: DataFrame) -> bool:
        pass
