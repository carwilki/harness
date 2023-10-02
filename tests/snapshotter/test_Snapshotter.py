from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture
from harness.snaphotter.Snapshotter import Snapshotter
from harness.config.SnapshotConfig import SnapshotConfig
from utils.generator import (
    generate_standard_snapshot_config,
)


class TestSnapshotter:
    def test_can_create_snapshotter_without_validator(
        self, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_snapshot_config(0, faker)
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        sut = Snapshotter(config=config, source=source, target=target)
        assert sut is not None
        assert isinstance(sut, Snapshotter)
        assert sut.config == config
        assert sut.source == source
        assert sut.target == target
        assert sut._validator is None
        source.assert_not_called()
        target.assert_not_called()

    def test_can_create_snapshot_V1(
        self, mocker: MockFixture, faker: Faker, spark: SparkSession
    ):
        config = generate_standard_snapshot_config(0, faker)
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        source.read.return_value = spark.createDataFrame([{"a": 1}])
        sut = Snapshotter(config=config, source=source, target=target)
        sut.snapshot()

        assert sut is not None
        assert isinstance(sut, Snapshotter)
        assert sut.config == config
        assert sut.source == source
        assert sut.target == target
        assert sut.config.version == 1
        source.read.assert_called_once()
        target.write.assert_called_once()

    def test_can_create_snapshot_V2(
        self, mocker: MockFixture, faker: Faker, spark: SparkSession
    ):
        config = generate_standard_snapshot_config(1, faker)
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        source.read.return_value = spark.createDataFrame([{"a": 1}])
        sut = Snapshotter(config=config, source=source, target=target)
        sut.snapshot()

        assert sut is not None
        assert isinstance(sut, Snapshotter)
        assert sut.config == config
        assert sut.source == source
        assert sut.target == target
        assert sut.config.version == 2
        source.read.assert_called_once()
        target.write.assert_called_once()

    def test_updateTargetSchema(
        self, snapshotConfig: SnapshotConfig, mocker: MockFixture
    ):
        snapshotConfig.target.test_target_schema = "test_target_schema"
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        sut = Snapshotter(config=snapshotConfig, source=source, target=target)
        assert sut.config.target.test_target_schema == "test_target_schema"
        sut.updateTestTargetSchema("new_target_schema")
        assert sut.config.target.test_target_schema == "new_target_schema"

    def test_updateTargetTable(
        self, snapshotConfig: SnapshotConfig, mocker: MockFixture
    ):
        initial_value = "test_target_table"
        expected_value = "new_test_target_table"
        snapshotConfig.target.test_target_table = initial_value
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        sut = Snapshotter(config=snapshotConfig, source=source, target=target)
        assert sut.config.target.test_target_table == initial_value
        sut.updateTestTargetTable(expected_value)
        assert sut.config.target.test_target_table == expected_value

    def test_updateSnapshotSchema(
        self, snapshotConfig: SnapshotConfig, mocker: MockFixture
    ):
        initial_value = "test_snapshot_schema"
        expected_value = "new_test_snapshot_schema"
        snapshotConfig.target.snapshot_target_schema = initial_value
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        sut = Snapshotter(config=snapshotConfig, source=source, target=target)
        assert sut.config.target.snapshot_target_schema == initial_value
        sut.updateSnapshotSchema(expected_value)
        assert sut.config.target.snapshot_target_schema == expected_value

    def test_updateSnapshotTable(
        self, snapshotConfig: SnapshotConfig, mocker: MockFixture
    ):
        initial_value = "test_snapshot_table"
        expected_value = "new_test_snapshot_table"
        snapshotConfig.target.test_target_table = initial_value
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        sut = Snapshotter(config=snapshotConfig, source=source, target=target)
        assert sut.config.target.test_target_table == initial_value
        sut.updateTestTargetTable(expected_value)
        assert sut.config.target.test_target_table == expected_value
