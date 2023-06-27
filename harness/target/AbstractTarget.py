from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig


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
