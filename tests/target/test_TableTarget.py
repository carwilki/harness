from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture
from harness.config.EnvironmentEnum import EnvironmentEnum
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig

from harness.target.TableTarget import DeltaTableTarget
from harness.target.TableTargetConfig import TableTargetConfig


class TestTableTarget:
    def test_can_be_created(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        tableTargetConfig: TableTargetConfig,
        bindenv,
    ):
        session: SparkSession = mocker.MagicMock()

        target = DeltaTableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        assert target is not None
        assert target.table_config is not None
        assert target.session is not None
        assert (
            target.table_config.snapshot_target_table
            == tableTargetConfig.snapshot_target_table
        )
        assert (
            target.table_config.snapshot_target_schema
            == tableTargetConfig.snapshot_target_schema
        )

    def test_can_create_table_for_write(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        tableTargetConfig: TableTargetConfig,
        bindenv,
    ):
        df = mocker.MagicMock()
        session = mocker.MagicMock()
        session.catalog.tableExists.return_value = False

        target = DeltaTableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        target.write(df)

        assert target.session.catalog.tableExists.call_count == 1
        assert target.session.sql.call_count == 1

    def test_can_overwrite_existing_test_data(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        tableTargetConfig: TableTargetConfig,
        bindenv,
    ):
        df = mocker.MagicMock()
        session = mocker.MagicMock()
        session.catalog.tableExists.return_value = True

        target = DeltaTableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        target.write(df)

        assert target.session.catalog.tableExists.call_count == 1
        assert target.session.sql.call_count == 2

    def test_can_setup_existing_table(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        tableTargetConfig: TableTargetConfig,
        bindenv,
    ):
        session = mocker.MagicMock()
        session.catalog.tableExists.return_value = True
        tableTargetConfig.test_target_schema = (
            "QA_" + tableTargetConfig.test_target_schema
        )
        target = DeltaTableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        target.setupDataForEnv(EnvironmentEnum.QA)

        assert target.session.catalog.tableExists.call_count == 1
        assert target.session.sql.call_count == 2

    def test_can_setup_new_table(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        tableTargetConfig: TableTargetConfig,
        bindenv,
    ):
        session = mocker.MagicMock()
        session.catalog.tableExists.return_value = False
        tableTargetConfig.test_target_schema = (
            "QA_" + tableTargetConfig.test_target_schema
        )
        target = DeltaTableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        target.setupDataForEnv(EnvironmentEnum.QA)

        assert target.session.catalog.tableExists.call_count == 1
        assert target.session.sql.call_count == 1
