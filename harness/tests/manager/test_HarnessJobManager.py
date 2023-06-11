import os

from faker import Faker
from pytest_mock import MockFixture

from harness.manager.HarnessJobManager import HarnessJobManager
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.snaphotter.Snapshotter import Snapshotter
from harness.tests.utils.generator import (
    generate_env_config,
    generate_standard_harness_job_config,
)


class TestHarnessJobManager:
    def test_can_create(self, mocker: MockFixture, faker: Faker):
        config = generate_standard_harness_job_config(0, faker)
        envconfig = generate_env_config(faker)

        bindenv = mocker.patch.object(HarnessJobManagerEnvironment, "bindenv")
        mocker.patch.dict(
            os.environ, {"__HARNESS_METADATA_SCHEMA": envconfig.metadata_schema}
        )
        mocker.patch.dict(
            os.environ, {"__HARNESS_METADATA_TABLE": envconfig.metadata_table}
        )
        create_schema = mocker.patch.object(
            HarnessJobManagerMetaData, "create_metadata_table"
        )
        session = mocker.MagicMock()
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)

        bindenv.assert_called_once_with(envconfig)

        assert HarnessJobManagerEnvironment.metadata_schema() is not None
        assert (
            HarnessJobManagerEnvironment.metadata_schema() == envconfig.metadata_schema
        )
        assert HarnessJobManagerEnvironment.metadata_table() is not None
        assert HarnessJobManagerEnvironment.metadata_table() == envconfig.metadata_table

        create_schema.assert_called_once_with(
            envconfig.metadata_schema, envconfig.metadata_table
        )
        assert manager is not None

    def test_can_snapshot(self, mocker: MockFixture, faker: Faker):
        config = generate_standard_harness_job_config(0, faker)
        envconfig = generate_env_config(faker)
        session = mocker.MagicMock()
        snapshot = mocker.patch.object(Snapshotter, "take_snapshot")
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)
        manager.snapshot()
        snapshot.assert_any_call()
