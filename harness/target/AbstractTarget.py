from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


class AbstractTarget(ABC):
    def __init__(self, session: SparkSession):
        self.session = session

    @abstractmethod
    def write(self, df: DataFrame, prefix: Optional[str] = None):
        pass
