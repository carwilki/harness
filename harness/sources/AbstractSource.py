import abc

from pyspark.sql import DataFrame, SparkSession


class AbstractSource(abc.ABC):
    def __init__(self, session: SparkSession) -> None:
        super().__init__()
        self.session = session

    @abc.abstractmethod 
    def read(self) -> DataFrame:
        pass
