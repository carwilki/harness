from faker import Faker
from pytest_mock import MockFixture
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.sources.JDBCSource import DatabricksJDBCSource, NetezzaJDBCSource

from harness.sources.JDBCSourceConfig import JDBCSourceConfig
from harness.sources.SourceFactory import SourceFactory


class TestSourceFactory:
    def test_can_create_source_from_jdbc_config(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        jdbcSourceConfig: JDBCSourceConfig,
        bindenv,
    ):
        session = mocker.MagicMock()
        source = SourceFactory.create(
            harness_config=harnessConfig,
            snapshot_config=snapshotConfig,
            source_config=jdbcSourceConfig,
            session=session,
        )

        assert source is not None

    def test_can_create_dbr_jdbc_source(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        jdbcSourceConfig: JDBCSourceConfig,
        bindenv,
    ):
        session = mocker.MagicMock()
        jdbcSourceConfig.source_type = SourceTypeEnum.databricks_jdbc
        source = SourceFactory.create(
            harness_config=harnessConfig,
            snapshot_config=snapshotConfig,
            source_config=jdbcSourceConfig,
            session=session,
        )

        assert source is not None
        assert isinstance(source, DatabricksJDBCSource)

    def test_can_create_netezza_jdbc_source(
        self,
        mocker: MockFixture,
        faker: Faker,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        jdbcSourceConfig: JDBCSourceConfig,
        bindenv,
    ):
        session = mocker.MagicMock()
        jdbcSourceConfig.source_type = SourceTypeEnum.netezza_jdbc
        source = SourceFactory.create(
            harness_config=harnessConfig,
            snapshot_config=snapshotConfig,
            source_config=jdbcSourceConfig,
            session=session,
        )

        assert source is not None
        assert isinstance(source, NetezzaJDBCSource)