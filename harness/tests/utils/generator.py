from faker import Faker

from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum
from harness.config.ValidatorTypeEnum import ValidatorTypeEnum
from harness.sources.JDBCSourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig


def generate_source_config() -> SourceConfig:
    return SourceConfig(source_type=SourceTypeEnum.jdbc)


def generate_target_config() -> TargetConfig:
    return TargetConfig(target_type=TargetTypeEnum.dbrtable)


def generate_table_target_config(faker: Faker) -> TargetConfig:
    return TableTargetConfig(
        target_type=TargetTypeEnum.dbrtable,
        target_schema=faker.pystr(),
        target_table=faker.pystr(),
    )


def generate_jdbc_source_config(faker: Faker) -> TargetConfig:
    return JDBCSourceConfig(
        source_type=SourceTypeEnum.jdbc,
        source_filter=None,
        source_table=faker.pystr(),
        source_schema=faker.pystr(),
    )


def generate_abstract_snapshot_config() -> SnapshotConfig:
    return SnapshotConfig(
        source=generate_source_config(),
        target=generate_target_config(),
        validator=ValidatorTypeEnum.dataframe,
    )


def generate_abstract_harness_job_config(faker: Faker) -> HarnessJobConfig:
    sources = {}
    for x in range(5):
        sources[f"source_{x}"] = generate_abstract_snapshot_config()
    inputs = {}
    for x in range(5):
        inputs[f"input_{x}"] = generate_abstract_snapshot_config()

    return HarnessJobConfig(
        job_id=faker.pystr(),
        snapshot_name=faker.pystr(),
        sources=sources,
        inputs=inputs,
    )


def generate_standard_snapshot_config(faker: Faker) -> SnapshotConfig:
    return SnapshotConfig(
        source=generate_jdbc_source_config(faker=faker),
        target=generate_table_target_config(faker=faker),
        validator=ValidatorTypeEnum.dataframe,
    )


def generate_standard_harness_job_config(faker: Faker) -> HarnessJobConfig:
    sources = {}
    for x in range(5):
        sources[f"source_{x}"] = generate_standard_snapshot_config(faker)
    inputs = {}
    for x in range(5):
        inputs[f"input_{x}"] = generate_standard_snapshot_config(faker)

    return HarnessJobConfig(
        job_id=faker.pystr(),
        snapshot_name=faker.pystr(),
        sources=sources,
        inputs=inputs,
    )


def generate_env_config(faker: Faker) -> EnvConfig:
    return EnvConfig(
        workspace_url=faker.url(),
        workspace_token=faker.pystr(),
        catalog=faker.pystr(),
        metadata_schema=faker.pystr(),
        metadata_table=faker.pystr(),
        snapshot_schema=faker.pystr(),
        snapshot_table_post_fix=faker.pystr(),
        jdbc_username=faker.pystr(),
        jdbc_password=faker.pystr(),
        jdbc_num_part=faker.pyint(),
        jdbc_url=faker.url(),
        jdbc_driver=faker.pystr(),
        jdbc_user=faker.user_name(),
    )
