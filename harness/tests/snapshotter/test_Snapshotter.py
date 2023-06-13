from datetime import datetime
from unittest.mock import MagicMock

from faker import Faker
from pytest_mock import MockFixture
from pyspark.sql import SparkSession
from harness.snaphotter.Snapshotter import Snapshotter
from harness.tests.utils.generator import (
    generate_standard_snapshot_config,
    generate_standard_validator_config,
)
from harness.validator.DataFrameValidator import DataFrameValidator
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


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

    def test_can_create_snappshotter_with_validator(
        self, mocker: MockFixture, faker: Faker
    ):
        config = generate_standard_snapshot_config(0, faker)
        config.validator = generate_standard_validator_config(faker)
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        source.read.return_value = faker.pystr()
        validator: MagicMock = mocker.patch.object(
            DataFrameValidator, "validate"
        ).return_value(
            DataFrameValidatorReport(
                summary="test",
                missmatch_sample=faker.paragraph(nb_sentences=10),
                validation_date=faker.date_time(),
            )
        )
        sut = Snapshotter(config=config, source=source, target=target)

        assert sut is not None
        assert isinstance(sut, Snapshotter)
        assert sut.config == config
        assert sut.source == source
        assert sut.target == target
        assert sut._validator is not None
        source.assert_not_called()
        target.assert_not_called()
        validator.assert_not_called()

    def test_can_create_snapshot_V1_without_validator(
        self, mocker: MockFixture, faker: Faker, spark: SparkSession
    ):
        config = generate_standard_snapshot_config(0, faker)
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        source.read.return_value = spark.createDataFrame([{"a": 1}])
        validator: MagicMock = mocker.patch.object(DataFrameValidator, "validate")
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
        validator.assert_not_called()

    def test_can_create_snapshot_V1_with_validator(
        self, mocker: MockFixture, faker: Faker, spark: SparkSession
    ):
        config = generate_standard_snapshot_config(0, faker)
        config.validator = generate_standard_validator_config(faker)
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        source.read.return_value = spark.createDataFrame([{"a": 1}])
        validation_report = DataFrameValidatorReport(
            summary="test",
            missmatch_sample=faker.paragraph(nb_sentences=10),
            validation_date=faker.date_time(),
        )

        validator: MagicMock = mocker.patch.object(DataFrameValidator, "validate")

        validator.return_value = validation_report

        sut = Snapshotter(config=config, source=source, target=target)
        sut.snapshot()

        assert sut is not None
        assert isinstance(sut, Snapshotter)
        assert sut.config == config
        assert sut.source == source
        assert sut.target == target
        assert sut._validator is not None
        assert len(sut.config.validator.validator_reports) == 1
        assert sut.config.version == 1
        source.read.assert_called_once()
        target.write.assert_called_once()
        validator.assert_called_once()

    def test_can_create_snapshot_V2_with_validator(
        self, mocker: MockFixture, faker: Faker, spark: SparkSession
    ):
        validation_report = DataFrameValidatorReport(
            summary="test",
            missmatch_sample=faker.paragraph(nb_sentences=10),
            validation_date=faker.date_time(),
        )

        config = generate_standard_snapshot_config(1, faker)
        config.validator = generate_standard_validator_config(faker)
        config.validator.validator_reports = {datetime.now(): validation_report}
        source = mocker.MagicMock()
        target = mocker.MagicMock()
        
        source.read.return_value = spark.createDataFrame([{"a": 1}])

        validator: MagicMock = mocker.patch.object(DataFrameValidator, "validate")

        validator.return_value = validation_report

        sut = Snapshotter(config=config, source=source, target=target)
        sut.snapshot()

        assert sut is not None
        assert isinstance(sut, Snapshotter)
        assert sut.config == config
        assert sut.source == source
        assert sut.target == target
        assert sut._validator is not None
        assert len(sut.config.validator.validator_reports) == 2
        assert sut.config.version == 2
        source.read.assert_called_once()
        target.write.assert_called_once()
        validator.assert_called_once()

    def test_can_not_create_snapshot_V3_with_validator(
        self, mocker: MockFixture, faker: Faker, spark: SparkSession
    ):
        validation_report = DataFrameValidatorReport(
            summary="test",
            missmatch_sample=faker.paragraph(nb_sentences=10),
            validation_date=faker.date_time(),
        )

        config = generate_standard_snapshot_config(2, faker)
        config.validator = generate_standard_validator_config(faker)

        source = mocker.MagicMock()
        target = mocker.MagicMock()
        source.read.return_value = spark.createDataFrame([{"a": 1}])

        validator: MagicMock = mocker.patch.object(DataFrameValidator, "validate")

        validator.return_value = validation_report

        sut = Snapshotter(config=config, source=source, target=target)
        sut.snapshot()

        assert sut is not None
        assert isinstance(sut, Snapshotter)
        assert sut.config == config
        assert sut.source == source
        assert sut.target == target
        assert sut._validator is not None
        assert sut.config.version == 2
        source.read.assert_not_called()
        target.write.assert_not_called()
        validator.assert_not_called()
