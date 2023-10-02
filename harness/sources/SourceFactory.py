from pyspark.sql import SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.sources.AbstractSource import AbstractSource
from harness.sources.JDBCSource import DatabricksJDBCSource, NetezzaJDBCSource
from harness.sources.SourceConfig import JDBCSourceConfig


class SourceFactory:
    """
    A factory class for creating instances of AbstractSource subclasses based on the provided source configuration.
    """

    @classmethod
    def create(
        cls,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        source_config: SourceConfig,
        session: SparkSession,
    ) -> AbstractSource:
        """
        Creates an instance of AbstractSource based on the provided source configuration.

        Args:
            harness_config (HarnessJobConfig): The Harness job configuration.
            snapshot_config (SnapshotConfig): The snapshot configuration.
            source_config (SourceConfig): The source configuration.
            session (SparkSession): The Spark session.

        Returns:
            AbstractSource: An instance of AbstractSource.
        """
        match source_config.source_type:
            case SourceTypeEnum.netezza_jdbc:
                assert isinstance(source_config, JDBCSourceConfig)
                return NetezzaJDBCSource(
                    harness_config=harness_config,
                    snapshot_config=snapshot_config,
                    config=source_config,
                    session=session,
                )
            case SourceTypeEnum.databricks_jdbc:
                assert isinstance(source_config, JDBCSourceConfig)
                return DatabricksJDBCSource(
                    harness_config=harness_config,
                    snapshot_config=snapshot_config,
                    config=source_config,
                    session=session,
                )
            case _:
                raise Exception(f"Unknown source type {source_config.source_type}")
