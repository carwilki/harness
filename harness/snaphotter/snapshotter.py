import abc
from typing import Optional
from pyspark.sql import SparkSession
from harness.config.config import (
    SnapshotConfig,
    TargetConfig,
)
from harness.sources.source import AbstractSource
from harness.sources.source_factory import SourceFactory
from harness.target.factory import TargetFactory
from harness.target.target import AbstractTarget
from logging import Logger as logger
from harness.validator.databricks import DataFrameValidator


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


class Snapshotter(AbstractSnapshotter):
    def __init__(
        self, config: SnapshotConfig, source: AbstractSource, target: AbstractTarget
    ) -> None:
        super().__init__(config=config, source=source, target=target)

    def take_snapshot(self):
        if self.config.version < 0:
            self.config.version = 0
            logger.info("invalid version number provided, setting version to 0")

        logger.info(f"Snapshotting {self.config.name} version {self.config.version}")
        if self.config.version <= 1:
            df = self.source.read()
            self.target.write(df)
            self.config.version += 1

            if self.config.validate is not None:
                DataFrameValidator.validate(df, self.source.read())


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
