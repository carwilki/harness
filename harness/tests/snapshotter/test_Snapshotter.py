from unittest.mock import MagicMock

from faker import Faker
from pytest_mock import MockFixture

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
