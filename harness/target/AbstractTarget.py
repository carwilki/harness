from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class AbstractTarget(ABC):
    def __init__(
        self,
        harness_job_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        session: SparkSession,
    ):
        self.session = session
        self.harness_job_config = harness_job_config
        self.snapshot_config = snapshot_config

    @abstractmethod
    def write(self, df: DataFrame):
        pass

    @abstractmethod
    def setup_test_target(self):
        pass

    @abstractmethod
    def validate_results(self) -> DataFrameValidatorReport:
        pass
    
    @abstractmethod
    def getSnapshotTableName(self, version: int) -> str:
        pass