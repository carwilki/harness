import abc

from pyspark.sql import SparkSession

from harness.config.SnapshotConfig import SnapshotConfig
from harness.sources.AbstractSource import AbstractSource
from harness.target.AbstractTarget import AbstractTarget


class AbstractSnapshotter(abc.ABC):
    def __init__(
        self, config: SnapshotConfig, source: AbstractSource, target: AbstractTarget
    ) -> None:
        self.config = config
        self.source = source
        self.target = target

    @abc.abstractmethod
    def take_snapshot(self, version: int, session: SparkSession):
        pass