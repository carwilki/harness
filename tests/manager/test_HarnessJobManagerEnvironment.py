import os

from faker import Faker
import pytest
from pytest_mock import MockFixture

from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManager import HarnessJobManager
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.sources.SourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig
from utils.generator import generate_env_config


class TestHarnessJobManagerEnvironment:
    @pytest.fixture(autouse=True)
    def bindenv(self, mocker: MockFixture, faker: Faker):
        envconfig = generate_env_config(faker)
        HarnessJobManagerEnvironment.bindenv(envconfig)
        return envconfig

    def test_bind_env(self, mocker: MockFixture, faker: Faker):
        env_config = EnvConfig(
            catalog=faker.first_name_female(),
            workspace_token=faker.credit_card_number(),
            workspace_url=faker.url(),
            metadata_schema=faker.first_name_male(),
            metadata_table=faker.first_name_female(),
            netezza_jdbc_url=faker.url(),
            netezza_jdbc_user=faker.first_name_male(),
            netezza_jdbc_password=faker.password(),
            netezza_jdbc_num_part=faker.random_int(),
            netezza_jdbc_driver=faker.pystr(),
            snapshot_schema=faker.first_name(),
            snapshot_table_post_fix=faker.first_name(),
        )
        HarnessJobManagerEnvironment.bindenv(env_config)

        assert HarnessJobManagerEnvironment.workspace_url() == env_config.workspace_url
        assert (
            HarnessJobManagerEnvironment.metadata_schema() == env_config.metadata_schema
        )
        assert (
            HarnessJobManagerEnvironment.metadata_table() == env_config.metadata_table
        )

    def test_snapshot_should_not_fail_with_given_config(
        self, mocker: MockFixture, faker: Faker, bindenv
    ):
        session = mocker.MagicMock()

        sc = JDBCSourceConfig(source_table="E_CONSOL_PERF_SMRY", source_schema="WMSMIS")
        tc = TableTargetConfig(
            snapshot_target_table="WM_E_CONSOL_PERF_SMRY",
            snapshot_target_schema="hive_metastore.nzmigration",
            test_target_schema="dev-refine",
            test_target_table="WM_E_CONSOL_PERF_SMRY",
            primary_key=["id1", "id2"],
        )
        snc = SnapshotConfig(target=tc, source=sc)
        job_id = "51298d4f-934f-439a-b80d-251987f54415"
        hjc = HarnessJobConfig(
            job_id=job_id,
            job_name=job_id,
            snapshots={"source1": snc},
            inputs={"source1": snc},
        )

        hjm = HarnessJobManager(hjc, session)
        hjm.snapshot()
