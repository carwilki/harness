from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from harness.config.config import TargetConfig


class AbstractTarget(ABC):
    def __init__(self, config: TargetConfig, session: SparkSession):
        self.target_config = config
        self.session = session

    @abstractmethod
    def write(self):
        pass
