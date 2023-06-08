from typing import Optional
from harness.config.config import SourceConfig, SourceTypeEnum
from harness.snaphotter.rocky.rocky_snapshotter import RockySnapshotter
from harness.snaphotter.snappshotter import Snapshotter


class SnapshotterFactory:
    """
    Factory class to create a snapshotter based on the source type
    Returns:
        Optional[Source]: returns the source object based on the source type
    """

    @classmethod
    def get_snapshotter(
        source_config: SourceConfig,
    ) -> Optional[Snapshotter]:
        """
        Provides a factory method to create a snapshotter based on the source type
        Args:
            sourceType (SourceTypeEnum): _description_

        Returns:
            Optional[Snapshotter]: _description_
        """
        match source_config.source_type:
            case SourceTypeEnum.rocky:
                return RockySnapshotter()
            # case SourceTypeEnum.snowflake: return SnowflakeSnapshotter()
            # case SourceTypeEnum.s3: return S3Snapshotter()
            case _:
                return None
