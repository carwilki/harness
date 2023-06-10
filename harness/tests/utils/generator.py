from faker import Faker

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum
from harness.config.ValidatorTypeEnum import ValidatorTypeEnum


def generate_source_config() -> SourceConfig:
    fake = Faker()
    return SourceConfig(source_type=SourceTypeEnum.jdbc, config=fake.pydict())


def generate_target_config() -> TargetConfig:
    fake = Faker()
    return TargetConfig(
        target_type=TargetTypeEnum.dbrtable, config=fake.pydict(), validator=None
    )


def generate_snapshot_config() -> SnapshotConfig:
    return SnapshotConfig(
        source=generate_source_config(),
        target=generate_target_config(),
        validator=ValidatorTypeEnum.dataframe,
    )


def generate_harness_job_config(faker: Faker) -> HarnessJobConfig:
    sources = {}
    for x in range(5):
        sources[f"source_{x}"] = generate_snapshot_config()
    inputs = {}
    for x in range(5):
        inputs[f"input_{x}"] = generate_snapshot_config()

    return HarnessJobConfig(
        job_id=faker.pystr(),
        snapshot_name=faker.pystr(),
        sources=sources,
        inputs=inputs,
    )
