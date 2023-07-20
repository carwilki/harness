from unittest.mock import MagicMock

from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture
from harness.snaphotter.Snapshotter import Snapshotter
from harness.tests.utils.generator import (
    generate_standard_snapshot_config,
)
from harness.validator.DataFrameValidator import DataFrameValidator


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

    def test_can_create_snapshot_V1_without_validator(
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
