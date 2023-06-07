from pytest import fixture
from faker import Faker
from faker.providers.python import Provider
from harness.config.config import (
    SnapshotConfig,
    TargetConfig,
    TargetTypeEnum,
    ValidatorConfig,
    ValidatorTypeEnum,
)
from harness.snaphotter.snappshotter import Snapshotter
from harness.config.config import SourceConfig, SourceTypeEnum

fake = Faker()


def generate_source_config() -> SourceConfig:
    return SourceConfig(source_type=SourceTypeEnum.netezza, config=fake.pydict())


def generate_target_config() -> TargetConfig:
    return TargetConfig(
        target_type=TargetTypeEnum.delta, config=fake.pydict(), validator=None
    )


def generate_validator_config() -> ValidatorConfig:
    return ValidatorConfig(
        validator_type=ValidatorTypeEnum.raptor, config=fake.pydict()
    )


def generate_snapshot_config() -> SnapshotConfig:
    return SnapshotConfig(
        snapshot_name="test",
        sources={"test": (generate_source_config(), generate_target_config())},
        inputs={"test": (generate_source_config(), generate_target_config())},
    )
