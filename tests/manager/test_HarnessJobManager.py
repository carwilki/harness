from faker import Faker
from pyspark.sql import SparkSession
import pytest
from pytest_mock import MockFixture
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManager import HarnessJobManager
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.snaphotter.Snapshotter import Snapshotter
from harness.sources.JDBCSource import NetezzaJDBCSource
from harness.sources.SourceConfig import JDBCSourceConfig
from harness.target.TableTarget import TableTarget
from harness.target.TableTargetConfig import TableTargetConfig
from utils.generator import (
    generate_env_config,
    generate_standard_harness_job_config,
)


class TestHarnessJobManager:
    @pytest.fixture(autouse=True)
    def bindenv(self, mocker: MockFixture, faker: Faker):
        envconfig = generate_env_config(faker)
        HarnessJobManagerEnvironment.bindenv(envconfig)
        return envconfig

    def test_constructor(self, mocker: MockFixture, faker: Faker, bindenv):
        config = generate_standard_harness_job_config(0, faker)
        session = mocker.MagicMock()
        constructor = HarnessJobManager(config, session)
        assert constructor is not None
        assert type(constructor.config) is HarnessJobConfig
        assert isinstance(constructor.snapshoters, dict)
        assert type(constructor._metadataManager) == HarnessJobManagerMetaData

    def test_can_create(self, mocker: MockFixture, faker: Faker):
        config = generate_standard_harness_job_config(0, faker)
        session: SparkSession = mocker.MagicMock()
        manager = HarnessJobManager(config, session=session)

        assert manager is not None

    def test_can_create_with_existing_metadata(self, mocker: MockFixture, faker: Faker):
        config = generate_standard_harness_job_config(0, faker)
        v1config = generate_standard_harness_job_config(1, faker)
        envconfig = generate_env_config(faker)
        HarnessJobManagerEnvironment.bindenv(envconfig)
        get: mocker.MagicMock = mocker.patch.object(
            HarnessJobManagerMetaData,
            "get",
            return_value=v1config,
        )

        session = mocker.MagicMock()
        manager = HarnessJobManager(config=config, session=session)

        assert manager is not None
        assert HarnessJobManagerEnvironment.metadata_schema() is not None
        assert (
            HarnessJobManagerEnvironment.metadata_schema() == envconfig.metadata_schema
        )
        assert HarnessJobManagerEnvironment.metadata_table() is not None
        assert HarnessJobManagerEnvironment.metadata_table() == envconfig.metadata_table
        assert manager.config == v1config
        get.assert_called_once_with(config.job_id)

    def test_can_snapshot_V1(
        self, spark: SparkSession, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_harness_job_config(0, faker)
        config.version = 0
        session = mocker.MagicMock()
        read = mocker.patch.object(NetezzaJDBCSource, "read")
        write = mocker.patch.object(TableTarget, "write")
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        read.return_value(spark.createDataFrame([{"a": 1}]))
        write.return_value(True)
        manager = HarnessJobManager(config=config, session=session)
        manager.snapshot()
        read.assert_called()
        write.assert_called()
        update.assert_called()
        for source in manager.config.snapshots.values():
            assert source.version == 1

    def test_can_snapshot_V2(
        self, spark: SparkSession, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_harness_job_config(1, faker)
        config.version = 1
        session = mocker.MagicMock()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        read = mocker.patch.object(NetezzaJDBCSource, "read")
        write = mocker.patch.object(TableTarget, "write")
        read.return_value(spark.createDataFrame([{"a": 1}]))
        write.return_value(True)
        manager = HarnessJobManager(config=config, session=session)
        manager.snapshot()
        read.assert_called()
        write.assert_called()
        update.assert_called()
        for source in manager.config.snapshots.values():
            assert source.version == 2

    def test_can_not_snapshot_V3(
        self, spark: SparkSession, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_harness_job_config(0, faker)
        config.version = 2
        for source in config.snapshots.values():
            source.version = 2
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        session = mocker.MagicMock()
        read = mocker.patch.object(NetezzaJDBCSource, "read")
        write = mocker.patch.object(TableTarget, "write")
        read.return_value(spark.createDataFrame([{"a": 1}]))
        write.return_value(True)
        manager = HarnessJobManager(config=config, session=session)
        manager.snapshot()
        read.assert_not_called()
        write.assert_not_called()
        update.assert_not_called()

        for source in manager.config.snapshots.values():
            assert source.version == 2

    def test_env_config_empyt_catalog(self, mocker: MockFixture, faker: Faker):
        config = generate_standard_harness_job_config(0, faker)
        session = mocker.MagicMock()
        constructor = HarnessJobManager(config, session)
        assert constructor is not None
        assert type(constructor.config) is HarnessJobConfig
        assert isinstance(constructor.snapshoters, dict)

    def test_should_be_able_to_snapshot_with_jdbc_sourc_and_table_target(
        self, mocker: MockFixture, faker: Faker
    ):
        session = mocker.MagicMock()
        sc = JDBCSourceConfig(source_table="E_CONSOL_PERF_SMRY", source_schema="WMSMIS")
        tc = TableTargetConfig(
            snapshot_target_table="WM_E_CONSOL_PERF_SMRY",
            snapshot_target_schema="hive_metastore.nzmigration",
            test_target_schema="dev_refine",
            test_target_table="WM_E_CONSOL_PERF_SMRY",
            primary_key=["id1", "id2"],
        )
        snc = SnapshotConfig(target=tc, source=sc)
        job_id = "01298d4f-934f-439a-b80d-251987f54415"

        hjc = HarnessJobConfig(
            job_id=job_id, job_name=job_id, snapshots={"source1": snc}, inputs={}
        )

        hjm = HarnessJobManager(hjc, session)

        hjm.snapshot()

    def test_updateTargetSchema(
        self,
        harnessConfig: HarnessJobConfig,
        faker: Faker,
        mocker: MockFixture,
        tableTargetConfig: TableTargetConfig,
        jdbcSourceConfig: JDBCSourceConfig,
    ):
        snapshotName = faker.pystr()
        expected = faker.pystr()
        inital = faker.pystr()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        tableTargetConfig.test_target_schema = inital
        harnessConfig.snapshots[snapshotName] = SnapshotConfig(
            name=snapshotName, target=tableTargetConfig, source=jdbcSourceConfig
        )
        hjm = HarnessJobManager(config=harnessConfig, session=mocker.MagicMock())
        assert hjm.config.snapshots[snapshotName].target.test_target_schema == inital
        hjm.updateTargetSchema(snapshotName, expected)
        assert hjm.config.snapshots[snapshotName].target.test_target_schema == expected
        assert update.call_count == 1

    def test_updateTargetTable(
        self,
        harnessConfig: HarnessJobConfig,
        faker: Faker,
        mocker: MockFixture,
        tableTargetConfig: TableTargetConfig,
        jdbcSourceConfig: JDBCSourceConfig,
    ):
        snapshotName = faker.pystr()
        expected = faker.pystr()
        inital = faker.pystr()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        tableTargetConfig.test_target_table = inital

        harnessConfig.snapshots[snapshotName] = SnapshotConfig(
            name=snapshotName, target=tableTargetConfig, source=jdbcSourceConfig
        )
        hjm = HarnessJobManager(config=harnessConfig, session=mocker.MagicMock())

        assert hjm.config.snapshots[snapshotName].target.test_target_table == inital
        hjm.updateTargetTable(snapshotName, expected)
        assert hjm.config.snapshots[snapshotName].target.test_target_table == expected
        assert update.call_count == 1

    def test_updateSnapshotSchema(
        self,
        harnessConfig: HarnessJobConfig,
        faker: Faker,
        mocker: MockFixture,
        tableTargetConfig: TableTargetConfig,
        jdbcSourceConfig: JDBCSourceConfig,
    ):
        snapshotName = faker.pystr()
        expected = faker.pystr()
        inital = faker.pystr()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")

        tableTargetConfig.snapshot_target_schema = inital
        harnessConfig.snapshots[snapshotName] = SnapshotConfig(
            name=snapshotName, target=tableTargetConfig, source=jdbcSourceConfig
        )
        hjm = HarnessJobManager(config=harnessConfig, session=mocker.MagicMock())
        assert (
            hjm.config.snapshots[snapshotName].target.snapshot_target_schema == inital
        )
        hjm.updateSnapshotSchema(snapshotName, expected)
        assert (
            hjm.config.snapshots[snapshotName].target.snapshot_target_schema == expected
        )
        assert update.call_count == 1

    def test_updateSnapshotTable(
        self,
        harnessConfig: HarnessJobConfig,
        faker: Faker,
        mocker: MockFixture,
        tableTargetConfig: TableTargetConfig,
        jdbcSourceConfig: JDBCSourceConfig,
    ):
        snapshotName = faker.pystr()
        expected = faker.pystr()
        inital = faker.pystr()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        tableTargetConfig.snapshot_target_table = inital
        harnessConfig.snapshots[snapshotName] = SnapshotConfig(
            name=snapshotName, target=tableTargetConfig, source=jdbcSourceConfig
        )
        hjm = HarnessJobManager(config=harnessConfig, session=mocker.MagicMock())
        assert hjm.config.snapshots[snapshotName].target.snapshot_target_table == inital
        hjm.updateSnapshotTable(snapshotName, expected)
        assert (
            hjm.config.snapshots[snapshotName].target.snapshot_target_table == expected
        )
        assert update.call_count == 1

    def test_updateAllTargetSchema(
        self,
        harnessConfig: HarnessJobConfig,
        faker: Faker,
        mocker: MockFixture,
    ):
        expected = faker.pystr()
        inital = faker.pystr()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        for cfg in harnessConfig.snapshots.values():
            cfg.target.test_target_schema = inital

        hjm = HarnessJobManager(config=harnessConfig, session=mocker.MagicMock())

        for cfg in harnessConfig.snapshots.values():
            assert cfg.target.test_target_schema == inital

        hjm.updateAllTargetSchema(expected)

        for cfg in harnessConfig.snapshots.values():
            assert cfg.target.test_target_schema == expected

        update.assert_called_once()

    def test_updateAllTargetTable(
        self,
        harnessConfig: HarnessJobConfig,
        faker: Faker,
        mocker: MockFixture,
    ):
        expected = faker.pystr()
        inital = faker.pystr()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        for cfg in harnessConfig.snapshots.values():
            cfg.target.test_target_table = inital

        hjm = HarnessJobManager(config=harnessConfig, session=mocker.MagicMock())

        for cfg in harnessConfig.snapshots.values():
            assert cfg.target.test_target_table == inital

        hjm.updateAllTargetTable(expected)

        for cfg in harnessConfig.snapshots.values():
            assert cfg.target.test_target_table == expected

        update.assert_called_once()

    def test_disableSnapshot(
        self,
        harnessConfig: HarnessJobConfig,
        faker: Faker,
        mocker: MockFixture,
        tableTargetConfig: TableTargetConfig,
        jdbcSourceConfig: JDBCSourceConfig,
    ):
        snapshotName = faker.pystr()
        inital = faker.pystr()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        tableTargetConfig.snapshot_target_table = inital
        harnessConfig.snapshots[snapshotName] = SnapshotConfig(
            name=snapshotName, target=tableTargetConfig, source=jdbcSourceConfig
        )
        hjm = HarnessJobManager(config=harnessConfig, session=mocker.MagicMock())
        assert hjm.config.snapshots[snapshotName].enabled is True
        hjm.disableSnapshot(snapshotName)
        assert hjm.config.snapshots[snapshotName].enabled is False
        update.assert_called_once()
        
    def test_enableSnapshot(
        self,
        harnessConfig: HarnessJobConfig,
        faker: Faker,
        mocker: MockFixture,
        tableTargetConfig: TableTargetConfig,
        jdbcSourceConfig: JDBCSourceConfig,
    ):
        snapshotName = faker.pystr()
        inital = faker.pystr()
        update = mocker.patch.object(HarnessJobManagerMetaData, "update")
        tableTargetConfig.snapshot_target_table = inital
        harnessConfig.snapshots[snapshotName] = SnapshotConfig(
            name=snapshotName,
            target=tableTargetConfig,
            source=jdbcSourceConfig,
            enabled=False,
        )
        hjm = HarnessJobManager(config=harnessConfig, session=mocker.MagicMock())
        assert hjm.config.snapshots[snapshotName].enabled is False
        hjm.enableSnapshot(snapshotName)
        assert hjm.config.snapshots[snapshotName].enabled is True
        update.assert_called_once()
