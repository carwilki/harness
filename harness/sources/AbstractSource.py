import abc

from pyspark.sql import DataFrame


class AbstractSource(abc.ABC):
    @abc.abstractmethod
    def read(self) -> DataFrame:
        pass
