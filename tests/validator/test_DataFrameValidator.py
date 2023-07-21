import numpy as np
import pandas as pd
from pytest_mock import MockFixture
from harness.config.ValidatorConfig import ValidatorConfig
from harness.validator.DataFrameValidator import DataFrameValidator
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport
from pyspark.sql.types import StructType


class TestDataFrameValidator:
    def test_can_create(self):
        validator = DataFrameValidator()
        assert validator is not None

    def test_can_validate_dataframe(
        self, mocker: MockFixture, spark: SparkSession, bindenv
    ):
        df = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )
        tdf1 = spark.createDataFrame(df)
        tdf2 = spark.createDataFrame(df)
        session = mocker.MagicMock()
        validator = DataFrameValidator()
        report = validator.validateDF(
            session=session, name="test", canidate=tdf2, master=tdf1, primary_keys=["A"]
        )
        assert report is not None

    def test_can_not_validate_two_empty_dataframes(
        self, mocker: MockFixture, spark: SparkSession, bindenv, freezer
    ):
        tdf1 = spark.createDataFrame([], StructType([]))
        tdf2 = spark.createDataFrame([], StructType([]))
        session = mocker.MagicMock()
        validator = DataFrameValidator()
        report: DataFrameValidatorReport = validator.validateDF(
            session=session, name="test", canidate=tdf2, master=tdf1, primary_keys=["A"]
        )
        expected = DataFrameValidatorReport.empty()
        assert report == expected

    def test_colums_with_base_prefix_cause_errors(
        self, mocker: MockFixture, spark: SparkSession, bindenv, freezer
    ):
        tdf1 = spark.createDataFrame([], StructType([]))
        tdf2 = spark.createDataFrame([], StructType([]))
        session = mocker.MagicMock()
        validator = DataFrameValidator()
        report: DataFrameValidatorReport = validator.validateDF(
            session=session, name="test", canidate=tdf2, master=tdf1, primary_keys=["A"]
        )
        expected = DataFrameValidatorReport.empty()
        assert report == expected
