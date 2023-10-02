from typing import Optional

from pyspark.sql import SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceConfig import SourceConfig
from harness.config.TargetConfig import TargetConfig
from harness.snaphotter.AbstractSnapshotter import AbstractSnapshotter
from harness.snaphotter.Snapshotter import Snapshotter
from harness.sources.AbstractSource import AbstractSource
from harness.sources.SourceFactory import SourceFactory
from harness.target.AbstractTarget import AbstractTarget
from harness.target.TargetFactory import TargetFactory


class SnapshotterFactory:
    """
    Factory class to create a snapshotter based on the source type
    Returns:
        Optional[Source]: returns the source object based on the source type
    """

    @classmethod
    def create(
        cls,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        session: SparkSession,
    ) -> Optional[AbstractSnapshotter]:
        """
        Provides a factory method to create a snapshotter based on the source type
        Args:
            sourceType (SourceTypeEnum): _description_
        Returns:
            Optional[Snapshotter]: _description_
        """
        return Snapshotter(
            snapshot_config,
            SnapshotterFactory._create_source(
                harness_config=harness_config,
                snapshot_config=snapshot_config,
                source_config=snapshot_config.source,
                session=session,
            ),
            SnapshotterFactory._create_target(
                harness_config=harness_config,
                snapshot_config=snapshot_config,
                config=snapshot_config.target,
                session=session,
            ),
        )

    @classmethod
    def _create_source(
        cls,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        source_config: SourceConfig,
        session: SparkSession,
    ) -> AbstractSource:
        """
        Creates a source object based on the given configuration parameters.

        Args:
            harness_config (HarnessJobConfig): The configuration for the Harness job.
            snapshot_config (SnapshotConfig): The configuration for the snapshot.
            source_config (SourceConfig): The configuration for the source.
            session (SparkSession): The Spark session to use.

        Returns:
            AbstractSource: The created source object.
        """
        return SourceFactory.create(
            harness_config=harness_config,
            snapshot_config=snapshot_config,
            source_config=source_config,
            session=session,
        )

    @classmethod
    def _create_target(
        cls,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        config: TargetConfig,
        session: SparkSession,
    ) -> AbstractTarget:
        """
        Creates a target object using the given configurations and SparkSession.

        Args:
            harness_config (HarnessJobConfig): The configuration for the Harness job.
            snapshot_config (SnapshotConfig): The configuration for the snapshot.
            config (TargetConfig): The configuration for the target.
            session (SparkSession): The SparkSession to use for creating the target.

        Returns:
            AbstractTarget: The created target object.
        """
        return TargetFactory.create(
            session=session,
            harness_config=harness_config,
            snapshot_config=snapshot_config,
            target_config=config,
        )
