from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from harness.config.EnvironmentEnum import EnvironmentEnum

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig


class AbstractTarget(ABC):
    """
    Abstract class for defining a target to write data to.
    """

    def __init__(
        self,
        harness_job_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        session: SparkSession,
    ):
        """
        Initializes the AbstractTarget object.

        Args:
            harness_job_config (HarnessJobConfig): The Harness job configuration.
            snapshot_config (SnapshotConfig): The snapshot configuration.
            session (SparkSession): The Spark session.
        """
        self.session = session
        self.harness_job_config = harness_job_config
        self.snapshot_config = snapshot_config

    @abstractmethod
    def write(self, df: DataFrame):
        """
        Abstract method to write data to the target.

        Args:
            df (DataFrame): The DataFrame to write.
        """
        pass

    @abstractmethod
    def setupDataForEnv(self, env: EnvironmentEnum):
        """
        Abstract method to set up the target for testing.
        """
        pass

    @abstractmethod
    def getSnapshotTableName(self, version: int) -> str:
        """
        Abstract method to get the snapshot table name.

        Args:
            version (int): The version of the snapshot.

        Returns:
            str: The snapshot table name.
        """
        pass

    @abstractmethod
    def destroy(self):
        """
        Abstract method to destroy the target.
        """
        pass
