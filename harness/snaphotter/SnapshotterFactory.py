from typing import Optional

from pyspark.sql import SparkSession

from harness.config.SnapshotConfig import SnapshotConfig
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
                config=snapshot_config.source, session=session
            ),
            SnapshotterFactory._create_target(
                config=snapshot_config.target, session=session
            ),
        )

    @classmethod
    def _create_source(
        cls, config: SnapshotConfig, session: SparkSession
    ) -> AbstractSource:
        return SourceFactory.create(config, session)

    @classmethod
    def _create_target(
        cls, config: TargetConfig, session: SparkSession
    ) -> AbstractTarget:
        return TargetFactory.create(config, session)
