import abc

from pyspark.sql import SparkSession

from harness.config.SnapshotConfig import SnapshotConfig
from harness.sources.AbstractSource import AbstractSource
from harness.target.AbstractTarget import AbstractTarget


class AbstractSnapshotter(abc.ABC):
    """
    A class representing a snapshotter.

    Attributes:
        config (SnapshotConfig): The snapshot configuration object.
    """    

    def __init__(
        self, config: SnapshotConfig, source: AbstractSource, target: AbstractTarget
    ) -> None:
        self.config: SnapshotConfig = config
        self.source: AbstractSource = source
        self.target: AbstractTarget = target

    @abc.abstractmethod
    def snapshot(self, version: int, session: SparkSession):
        """
            abstract method to be implemented by subclasses to take a snapshot of the data
        Args:
            version (int): version of the snapshot
            session (SparkSession): the spark session to use
        """
        pass

    @abc.abstractmethod
    def destroy(self):
        """
        abstract method to be implemented by subclasses to destroy the snapshotter
        """
        pass
