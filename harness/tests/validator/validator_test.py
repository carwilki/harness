from pyspark.sql import SparkSession

from harness.validator.validator import AbstractValidator


class TestValidator:
    def test_config(self):
        validator = AbstractValidator()
        assert validator is not None

    def test_take_snapshot(self, mocker):
        session: SparkSession = mocker.MagicMock()
        validator = AbstractValidator()
        validator.validate(spark=session)
