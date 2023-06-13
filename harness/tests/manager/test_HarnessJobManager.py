import os

from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture
from harness.config.EnvConfig import EnvConfig

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
        create_metadata_table = mocker.patch.object(
            HarnessJobManagerMetaData, "create_metadata_table", return_value=None
        )
        get: mocker.MagicMock = mocker.patch.object(
            HarnessJobManagerMetaData, "get", return_value=None
        )

        session = mocker.MagicMock()
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)

        assert manager is not None
        assert HarnessJobManagerEnvironment.metadata_schema() is not None
        assert (
            HarnessJobManagerEnvironment.metadata_schema() == envconfig.metadata_schema
        )
        assert HarnessJobManagerEnvironment.metadata_table() is not None
        assert HarnessJobManagerEnvironment.metadata_table() == envconfig.metadata_table

        bindenv.assert_called_once_with(envconfig)
        create_metadata_table.assert_called_once()
        get.assert_called_once()

    def test_can_create_with_existing_metadata(self, mocker: MockFixture, faker: Faker):
        config = generate_standard_harness_job_config(0, faker)
        v1config = generate_standard_harness_job_config(1, faker)
        envconfig = generate_env_config(faker)
        bindenv = mocker.patch.object(HarnessJobManagerEnvironment, "bindenv")
        mocker.patch.dict(
            os.environ, {"__HARNESS_METADATA_SCHEMA": envconfig.metadata_schema}
        )
        mocker.patch.dict(
            os.environ, {"__HARNESS_METADATA_TABLE": envconfig.metadata_table}
        )
        create_metadata_table = mocker.patch.object(
            HarnessJobManagerMetaData, "create_metadata_table", return_value=None
        )
        get: mocker.MagicMock = mocker.patch.object(
            HarnessJobManagerMetaData,
            "get",
            return_value=v1config,
        )

        session = mocker.MagicMock()
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)

        assert manager is not None
        assert HarnessJobManagerEnvironment.metadata_schema() is not None
        assert (
            HarnessJobManagerEnvironment.metadata_schema() == envconfig.metadata_schema
        )
        assert HarnessJobManagerEnvironment.metadata_table() is not None
        assert HarnessJobManagerEnvironment.metadata_table() == envconfig.metadata_table
        assert manager.config == v1config
        bindenv.assert_called_once_with(envconfig)
        create_metadata_table.assert_called_once()
        get.assert_called_once_with(config.job_id)

    def test_can_snapshot_V1(
        self, spark: SparkSession, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_harness_job_config(0, faker)
        config.version = 0
        envconfig = generate_env_config(faker)
        session = mocker.MagicMock()
        read = mocker.patch.object(JDBCSource, "read")
        write = mocker.patch.object(TableTarget, "write")
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        read.return_value(spark.createDataFrame([{"a": 1}]))
        write.return_value(True)
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)
        manager.snapshot()
        read.assert_called()
        write.assert_called()
        update.assert_called()
        for source in manager.config.sources.values():
            assert source.version == 1

    def test_can_snapshot_V2(
        self, spark: SparkSession, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_harness_job_config(1, faker)
        config.version = 1
        envconfig = generate_env_config(faker)
        session = mocker.MagicMock()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        read = mocker.patch.object(JDBCSource, "read")
        write = mocker.patch.object(TableTarget, "write")
        read.return_value(spark.createDataFrame([{"a": 1}]))
        write.return_value(True)
        manager = HarnessJobManager(config=config, envconfig=envconfig, session=session)
        manager.snapshot()
        read.assert_called()
        write.assert_called()
        update.assert_called()
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

        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
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
        update.assert_not_called()

        for source in manager.config.sources.values():
            assert source.version == 2

    def test_env_config_empyt_catalog(self, mocker: MockFixture, faker: Faker):
        
        config = generate_standard_harness_job_config(0, faker)
        env = EnvConfig(
            workspace_url="https://dbc-b703fa4f-373c.cloud.databricks.com",
            workspace_token="dapie631edb76860dd52605706ba22d2c3ec",
            metadata_schema="hive_metastore.default",
            metadata_table="databricks_shubham_harness",
            snapshot_schema="hive_metastore.default",
            snapshot_table_post_fix="databricks_shubham_harness_post",
            jdbc_url="jdbc:spark://dbc-b703fa4f-373c.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/7d6c126fdae128cb",
            jdbc_user="admin",
            jdbc_password="abcde",
            jdbc_driver="abcde",
        )
        session = mocker.MagicMock()
        constructor = HarnessJobManager(config, env, session)
        assert constructor is not None
        assert type(constructor.config) is HarnessJobConfig
        assert isinstance(constructor._source_snapshoters, dict)
        assert isinstance(constructor._input_snapshoters, dict)
        assert type(constructor._metadataManager) == HarnessJobManagerMetaData
        assert constructor._env == HarnessJobManagerEnvironment

