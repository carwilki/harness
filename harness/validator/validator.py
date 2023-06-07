from abc import abstractmethod
from pyspark.sql import SparkSession
from harness.config.config import ValidatorConfig


class Validator:
    def __init__(self, config: ValidatorConfig):
        self.config = config

    @abstractmethod
    def validate(self, spark: SparkSession):
        pass
