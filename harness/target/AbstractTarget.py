from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from harness.config.TargetConfig import TargetConfig


class AbstractTarget(ABC):
    def __init__(self, config: TargetConfig, session: SparkSession):
        self.target_config = config
        self.session = session

    @abstractmethod
    def write(self, df: DataFrame) -> bool:
        pass

    @abstractmethod
    def validate(self, df: DataFrame) -> bool:
        pass

    @abstractmethod
    def validate_with(self, canidate: DataFrame, master: DataFrame):
        pass