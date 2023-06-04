import snapshotter
from config import SnapshotConfig, RaptorConfig
from pyspark.sql import SparkSession

spark: SparkSession = spark


def take_snapshot(version: int, config: SnapshotConfig, session: SparkSession):
    