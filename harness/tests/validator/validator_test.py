from pyspark.sql import SparkSession
from harness.tests.utils.generator import generate_validator_config

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
