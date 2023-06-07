from pytest import fixture
from faker import Faker
from faker.providers.python import Provider
from harness.config.config import SnapshotConfig
from harness.snaphotter.snappshotter import Snapshotter
from harness.config.config import SourceConfig, SourceTypeEnum
from harness.tests.utils.generator import (
    generate_source_config,
    generate_target_config,
    generate_validator_config,
)
from pyspark.sql import SparkSession

from harness.validator.validator import Validator


class TestValidator:
    def test_config(self):
        config = generate_validator_config()
        validator = Validator(config=config)
        assert validator.config == config

    def test_take_snapshot(self, mocker):
        session: SparkSession = mocker.MagicMock()
        config = generate_validator_config()
        validator = Validator(config=config)
        validator.validate(spark=session)
