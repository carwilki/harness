from abc import abstractclassmethod, abstractmethod
from typing import Any
from pyspark.sql import SparkSession
from harness.config.config import SnapshotConfig


class Snapshotter:
    config: SnapshotConfig

    def __init__(self, config: SnapshotConfig) -> None:
        self.config = config

    @abstractmethod
    def take_snapshot(
        self, version: int, session: SparkSession
    ):
        pass
