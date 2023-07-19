from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.sources.JDBCSourceConfig import JDBCSourceConfig

from harness.target.TableTarget import TableTarget
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

        target = TableTarget(
            harness_job_config=harnessConfig,
            snapshot_config=snapshotConfig,
            table_config=tableTargetConfig,
            session=session,
        )

        assert target is not None
        assert target.config is not None
        assert target.session is not None
        assert (
            target.config.snapshot_target_table
            == tableTargetConfig.snapshot_target_table
        )
        assert (
            target.config.snapshot_target_schema
            == tableTargetConfig.snapshot_target_schema
        )
