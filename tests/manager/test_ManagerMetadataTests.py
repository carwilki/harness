from faker import Faker
from pyspark.sql import SparkSession
import pytest
from pytest_mock import MockFixture

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.sources.JDBCSourceConfig import JDBCSourceConfig
from utils.generator import (
    generate_env_config,
    generate_standard_harness_job_config,
)


class TestManagerMetaData:
    @pytest.fixture(autouse=True)
    def bindenv(self, mocker: MockFixture, faker: Faker):
        envconfig = generate_env_config(faker)
        HarnessJobManagerEnvironment.bindenv(envconfig)
        return envconfig

    def test_constructor(self, mocker: MockFixture, faker: Faker):
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        assert metadata is not None
        assert metadata.session is session

    def test_can_create_metadata_table(
        self, mocker: MockFixture, faker: Faker, bindenv: HarnessJobManagerEnvironment
    ):
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        metadata.create_metadata_table()
        session.sql.assert_called_with(
            f"""Create table if not exists {bindenv.metadata_schema}.{bindenv.metadata_table} (id string, value string)"""
        )

    def test_can_get(
        self, mocker: MockFixture, faker: Faker, bindenv: HarnessJobManagerEnvironment
    ):
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        metadata.get(key=1)
        session.sql.assert_called_with(
            f"""Select value from {bindenv.metadata_schema}.{bindenv.metadata_table} where id == '1'"""
        )

    def test_can_delete(
        self, mocker: MockFixture, bindenv: HarnessJobManagerEnvironment
    ):
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        metadata.delete(key=1)
        session.sql.assert_called_with(
            f"""Delete from {bindenv.metadata_schema}.{bindenv.metadata_table} where id == '1'"""
        )

    def test_can_create(
        self, mocker: MockFixture, faker, bindenv: HarnessJobManagerEnvironment
    ):
        table = f"{bindenv.metadata_schema}.{bindenv.metadata_table}"
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        config = generate_standard_harness_job_config(0, faker=faker)
        bin = config.json().encode("utf-8")
        bin = str(bin).removeprefix("b'").removesuffix("'")
        metadata.create(value=config)
        session.sql.assert_called_with(
            f"""Insert into {table} values ('{config.job_id}', '{bin}')"""
        )

    def test_can_update(
        self, mocker: MockFixture, faker: Faker, bindenv: HarnessJobManagerEnvironment
    ):
        table = f"{bindenv.metadata_schema}.{bindenv.metadata_table}"
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        config = generate_standard_harness_job_config(1, faker=faker)
        bin = config.json().encode("utf-8")
        bin = str(bin).removeprefix("b'").removesuffix("'")
        metadata.update(config)
        session.sql.assert_called_with(
            f"""update {table} set value = '{bin}' where id == '{config.job_id}'"""
        )

    def test_can_deserialize_proper_types(self, mocker: MockFixture, faker: Faker):
        metadata = """{
  "job_id": "3adba4a3-fe6a-42bc-9845-83b0647bc141",
  "job_name": "wms_to_scds_4hr_test_demo_prettest",
  "version": 0,
  "testrunner": null,
  "snapshots": {
    "WM_E_DEPT": {
      "name": "WM_E_DEPT",
      "job_id": "3adba4a3-fe6a-42bc-9845-83b0647bc141",
      "target": {
        "target_type": "dbrtable",
        "snapshot_target_schema": "nzmigration",
        "snapshot_target_table": "WM_E_DEPT",
        "test_target_schema": "dev_refine",
        "test_target_table": "WM_E_DEPT",
        "primary_key": [
          "LOCATION_ID",
          "WM_DEPT_ID"
        ]
      },
      "source": {
        "source_type": "netezza_jdbc",
        "source_filter": "1=1",
        "source_table": "WM_E_DEPT",
        "source_schema": "SCDS_PRD"
      },
      "version": 0,
      "validator": null,
      "validated": false,
      "validation_date": null,
      "validation_report": null
    },
    "WM_E_CONSOL_PERF_SMRY": {
      "name": "WM_E_CONSOL_PERF_SMRY",
      "job_id": "3adba4a3-fe6a-42bc-9845-83b0647bc141",
      "target": {
        "target_type": "dbrtable",
        "snapshot_target_schema": "nzmigration",
        "snapshot_target_table": "WM_E_CONSOL_PERF_SMRY",
        "test_target_schema": "dev_refine",
        "test_target_table": "WM_E_CONSOL_PERF_SMRY",
        "primary_key": [
          "LOCATION_ID",
          "WM_PERF_SMRY_TRAN_ID"
        ]
      },
      "source": {
        "source_type": "netezza_jdbc",
        "source_filter": "LOAD_TSTMP >= date_trunc('day', now() - interval '30 days') OR update_tstmp >= date_trunc('day', now() - interval '30 days')",
        "source_table": "WM_E_CONSOL_PERF_SMRY",
        "source_schema": "SCDS_PRD"
      },
      "version": 0,
      "validator": null,
      "validated": false,
      "validation_date": null,
      "validation_report": null
    },
    "WM_UCL_USER": {
      "name": "WM_UCL_USER",
      "job_id": "3adba4a3-fe6a-42bc-9845-83b0647bc141",
      "target": {
        "target_type": "dbrtable",
        "snapshot_target_schema": "nzmigration",
        "snapshot_target_table": "WM_UCL_USER",
        "test_target_schema": "dev_refine",
        "test_target_table": "WM_UCL_USER",
        "primary_key": [
          "LOCATION_ID",
          "USER_NAME"
        ]
      },
      "source": {
        "source_type": "netezza_jdbc",
        "source_filter": "1=1",
        "source_table": "WM_UCL_USER",
        "source_schema": "SCDS_PRD"
      },
      "version": 0,
      "validator": null,
      "validated": false,
      "validation_date": null,
      "validation_report": null
    }
  },
  "validation_reports": {}
}"""
        config = HarnessJobConfig.parse_raw(metadata)
        assert config is not None
        for source in config.snapshots.values():
            assert isinstance(source.source, JDBCSourceConfig)
