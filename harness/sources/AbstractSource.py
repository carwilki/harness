import abc

from pyspark.sql import DataFrame, SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig


class AbstractSource(abc.ABC):
    """
    Abstract base class for data sources in Harness. All data sources should inherit from this class and implement the
    `read` method.
    """

    def __init__(
        self,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        session: SparkSession,
    ) -> None:
        super().__init__()
        self.session = session
        self.harness_config = harness_config
        self.snapshot_config = snapshot_config

    @abc.abstractmethod
    def read(self) -> DataFrame:
        """
        Reads data from the source and returns it as a pandas DataFrame.
        """
        pass
