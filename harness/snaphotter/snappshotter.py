import abc
from pyspark.sql import SparkSession
from harness.config.config import SnapshotConfig


class Snapshotter(abc.ABC):
    def __init__(self, config: SnapshotConfig) -> None:
        self.config = config

    def take_snapshot(self, version: int, session: SparkSession):
        pass
