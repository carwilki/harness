import abc
from pyspark.sql import SparkSession
from harness.config.config import SourceConfig


class AbstractSource(abc.ABC):
    def __init__(self, config: SourceConfig, session: SparkSession):
        self.config = config
        self.session = session

    @abc.abstractmethod
    def read(self):
        pass
