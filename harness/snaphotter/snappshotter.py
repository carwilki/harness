from abc import abstractclassmethod
from typing import Any
from pyspark.sql import SparkSession
from harness.config.config import SnapshotConfig


class Snapshotter:
    type: str
    config: SnapshotConfig

    @abstractclassmethod()
    def take_snapshot(version: int, config: SnapshotConfig, session: SparkSession):
        pass
