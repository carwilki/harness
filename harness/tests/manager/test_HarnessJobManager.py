import os

from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.manager.HarnessJobManager import HarnessJobManager
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.sources.JDBCSource import JDBCSource
from harness.target.TableTarget import TableTarget
from harness.tests.utils.generator import (
    generate_env_config,
    generate_standard_harness_job_config,
)


class TestHarnessJobManager:
    def test_constructor(self, mocker: MockFixture, faker: Faker):
        config = generate_standard_harness_job_config(0, faker)
        envconfig = generate_env_config(faker)
        session = mocker.MagicMock()
        constructor = HarnessJobManager(config, envconfig, session)
        assert constructor is not None
        assert type(constructor.config) is HarnessJobConfig
        assert isinstance(constructor._source_snapshoters, dict)
        assert isinstance(constructor._input_snapshoters, dict)
        assert type(constructor._metadataManager) == HarnessJobManagerMetaData
        assert constructor._env == HarnessJobManagerEnvironment

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

    def test_can_snapshot_V1(
        self, spark: SparkSession, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_harness_job_config(0, faker)
        config.version = 0
        envconfig = generate_env_config(faker)
        session = mocker.MagicMock()
        read = mocker.patch.object(JDBCSource, "read")
        write = mocker.patch.object(TableTarget, "write")
        read.return_value(spark.createDataFrame([{"a": 1}]))
        write.return_value(True)
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)
        manager.snapshot()
        read.assert_called()
        write.assert_called()
        for source in manager.config.sources.values():
            assert source.version == 1

    def test_can_snapshot_V2(
        self, spark: SparkSession, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_harness_job_config(1, faker)
        config.version = 1
        envconfig = generate_env_config(faker)
        session = mocker.MagicMock()
        read = mocker.patch.object(JDBCSource, "read")
        write = mocker.patch.object(TableTarget, "write")
        read.return_value(spark.createDataFrame([{"a": 1}]))
        write.return_value(True)
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)
        manager.snapshot()
        read.assert_called()
        write.assert_called()
        for source in manager.config.sources.values():
            assert source.version == 2

    def test_can_not_snapshot_V3(
        self, spark: SparkSession, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_harness_job_config(0, faker)
        config.version = 2
        for source in config.sources.values():
            source.version = 2
        for source in config.inputs.values():
            source.version = 2

        envconfig = generate_env_config(faker)
        session = mocker.MagicMock()
        read = mocker.patch.object(JDBCSource, "read")
        write = mocker.patch.object(TableTarget, "write")
        read.return_value(spark.createDataFrame([{"a": 1}]))
        write.return_value(True)
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)
        manager.snapshot()
        read.assert_not_called()
        write.assert_not_called()

        for source in manager.config.sources.values():
            assert source.version == 2
