from faker import Faker
from harness.config.config import (
    SnapshotConfig,
    TargetConfig,
    TargetTypeEnum,
    ValidatorTypeEnum,
)

from harness.config.config import SourceConfig, SourceTypeEnum

fake = Faker()


def generate_source_config() -> SourceConfig:
    return SourceConfig(source_type=SourceTypeEnum.jdbc, config=fake.pydict())


def generate_target_config() -> TargetConfig:
    return TargetConfig(
        target_type=TargetTypeEnum.dbrtable, config=fake.pydict(), validator=None
    )


def generate_snapshot_config() -> SnapshotConfig:
    return SnapshotConfig(
        source=generate_source_config(),
        target=generate_target_config(),
        validator=ValidatorTypeEnum.dataframe,
    )
