from typing import Optional
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
        snapshot_config: SnapshotConfig,
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
            SnapshotterFactory._create_source(snapshot_config),
            SnapshotterFactory._create_target(snapshot_config),
        )

    @classmethod
    def _create_source(cls, config: SnapshotConfig) -> AbstractSource:
        return SourceFactory.create(config)

    @classmethod
    def _create_target(cls, config: TargetConfig) -> AbstractTarget:
        return TargetFactory.create(config)
