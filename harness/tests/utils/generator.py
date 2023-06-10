from faker import Faker

from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum
from harness.config.ValidatorTypeEnum import ValidatorTypeEnum

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
