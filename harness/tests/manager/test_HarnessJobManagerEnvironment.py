import os

from faker import Faker
from pytest_mock import MockFixture

from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManager import HarnessJobManager
from harness.manager.HarnessJobManagerEnvironment import \
    HarnessJobManagerEnvironment
from harness.sources.JDBCSourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig


class TestHarnessJobManagerEnvironment:
    def test_empty_env(self, mocker: MockFixture, faker: Faker):
        mocker.patch.dict(os.environ, {"__WORKSPACE_URL": ""})
        mocker.patch.dict(os.environ, {"__WORKSPACE_TOKEN": ""})
        mocker.patch.dict(os.environ, {"__CATALOG": ""})
        mocker.patch.dict(os.environ, {"__HARNESS_METADATA_SCHEMA": ""})
        mocker.patch.dict(os.environ, {"__HARNESS_METADATA_TABLE": ""})
        mocker.patch.dict(os.environ, {"__HARNESS_SNAPSHOT_SCHEMA": ""})
        mocker.patch.dict(os.environ, {"__HARNESS_SNAPSHOT_TABLE_POSTFIX": ""})
        mocker.patch.dict(os.environ, {"__JDBC_URL": ""})
        mocker.patch.dict(os.environ, {"__JDBC_USER": ""})
        mocker.patch.dict(os.environ, {"__JDBC_PASSWORD": ""})
        mocker.patch.dict(os.environ, {"__JDBC_NUM_PART": ""})

        assert HarnessJobManagerEnvironment.catalog() is None
        assert HarnessJobManagerEnvironment.workspace_token() is None
        assert HarnessJobManagerEnvironment.workspace_url() is None
        assert HarnessJobManagerEnvironment.metadata_schema() is None
        assert HarnessJobManagerEnvironment.metadata_table() is None
        assert HarnessJobManagerEnvironment.jdbc_url() is None
        assert HarnessJobManagerEnvironment.jdbc_user() is None
        assert HarnessJobManagerEnvironment.jdbc_password() is None
        assert HarnessJobManagerEnvironment.jdbc_num_part() is None

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

        assert HarnessJobManagerEnvironment.catalog() == env_config.catalog
        assert (
            HarnessJobManagerEnvironment.workspace_token() == env_config.workspace_token
        )
        assert HarnessJobManagerEnvironment.workspace_url() == env_config.workspace_url
        assert (
            HarnessJobManagerEnvironment.metadata_schema() == env_config.metadata_schema
        )
        assert (
            HarnessJobManagerEnvironment.metadata_table() == env_config.metadata_table
        )
        assert HarnessJobManagerEnvironment.jdbc_url() == env_config.netezza_jdbc_url
        assert HarnessJobManagerEnvironment.jdbc_user() == env_config.netezza_jdbc_user
        assert (
            HarnessJobManagerEnvironment.jdbc_password()
            == env_config.netezza_jdbc_password
        )
        assert (
            HarnessJobManagerEnvironment.jdbc_num_part()
            == env_config.netezza_jdbc_num_part
        )

    def test_snapshot_should_not_fail_with_given_config(
        self, mocker: MockFixture, faker: Faker
    ):
        session = mocker.MagicMock()
        username = faker.user_name()
        password = faker.password()
        env1 = EnvConfig(
            workspace_url="https://3986616729757273.3.gcp.databricks.com/",
            workspace_token="dapi5492460db39d145778c9d436bbbf1842",
            metadata_schema="hive_metastore.nzmigration",
            metadata_table="databricks_shubham_harness",
            snapshot_schema="hive_metastore.nzmigration",
            snapshot_table_post_fix="databricks_shubham_harness_post",
            netezza_jdbc_url="jdbc:netezza:/172.16.73.181:5480/EDW_PRD",
            netezza_jdbc_user=username,
            netezza_jdbc_password=password,
            netezza_jdbc_driver="org.netezza.Driver",
        )

        sc = JDBCSourceConfig(source_table="E_CONSOL_PERF_SMRY", source_schema="WMSMIS")
        tc = TableTargetConfig(
            snapshot_target_table="WM_E_CONSOL_PERF_SMRY",
            snapshot_target_schema="hive_metastore.nzmigration",
        )
        snc = SnapshotConfig(target=tc, source=sc)
        job_id = "51298d4f-934f-439a-b80d-251987f54415"
        hjc = HarnessJobConfig(
            job_id=job_id, sources={"source1": snc}, inputs={"source1": snc}
        )

        hjm = HarnessJobManager(hjc, env1, session)
        hjm.snapshot()
