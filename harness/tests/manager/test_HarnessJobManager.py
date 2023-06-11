import os

from faker import Faker
from pytest_mock import MockFixture
from pyspark.sql import SparkSession

from harness.manager.HarnessJobManager import HarnessJobManager
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.sources.JDBCSource import JDBCSource
from harness.target.TableTarget import TableTarget
from harness.tests.utils.generator import (
    generate_env_config,
    generate_standard_harness_job_config,
)
from harness.validator import DataFrameValidator
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.snaphotter.Snapshotter import Snapshotter


class TestHarnessJobManager:
    def test_constructor(self,mocker: MockFixture, faker: Faker,session:SparkSession):
        config = generate_standard_harness_job_config(0, faker)
        envconfig = generate_env_config(faker)
        session = mocker.MagicMock()
        constructor = HarnessJobManager(config,envconfig,session)
        assert constructor is not None
        assert type(constructor.config) == HarnessJobConfig
        assert type(config._source_snapshoters) == dict[str, Snapshotter]
        assert type(config.__input_snapshoters) == dict[str, Snapshotter]
        assert type(config._metadataManager) == HarnessJobManagerMetaData
        assert type(config._env) == envconfig
        
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

    def test_can_snapshot(self, spark: SparkSession, mocker: MockFixture, faker: Faker):
        config = generate_standard_harness_job_config(0, faker)
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
