from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig

from harness.target.TableTarget import TableTarget
from harness.target.TableTargetConfig import TableTargetConfig
from harness.validator.DataFrameValidator import DataFrameValidator
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


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

        target = TableTarget(
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

        target = TableTarget(
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

        target = TableTarget(
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

        target = TableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        target.setup_test_target()

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

        target = TableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        target.setup_test_target()

        assert target.session.catalog.tableExists.call_count == 1
        assert target.session.sql.call_count == 1

    def test_can_validate(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        tableTargetConfig: TableTargetConfig,
        validatorReport: DataFrameValidatorReport,
        bindenv,
    ):
        session = mocker.MagicMock()

        validateDf = mocker.patch.object(DataFrameValidator, "validateDF")
        validateDf.return_value = validatorReport

        target = TableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        target.validate_results()

        assert target.session.sql.call_count == 2
        assert validateDf.call_count == 1
