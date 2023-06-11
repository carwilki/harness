from faker import Faker
from pytest_mock import MockFixture

from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment


class TestHarnessJobManagerEnvironment:
    def test_empty_env(self, mocker: MockFixture, faker: Faker):
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
            jdbc_url=faker.url(),
            jdbc_user=faker.first_name_male(),
            jdbc_password=faker.password(),
            jdbc_num_part=str(faker.random_int()),
            jdbc_driver=faker.pystr(),
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
        assert HarnessJobManagerEnvironment.jdbc_url() == env_config.jdbc_url
        assert HarnessJobManagerEnvironment.jdbc_user() == env_config.jdbc_user
        assert HarnessJobManagerEnvironment.jdbc_password() == env_config.jdbc_password
        assert HarnessJobManagerEnvironment.jdbc_num_part() == env_config.jdbc_num_part
