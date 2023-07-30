import numpy as np
import pandas as pd
from pytest_mock import MockFixture
from harness.validator.DataFrameValidator import DataFrameValidator
from pyspark.sql import SparkSession
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport
from pyspark.sql.types import StructType, StructField, IntegerType, LongType


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
            session=session, name="test", compare=tdf2, base=tdf1, primary_keys=["A"]
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
            session=session, name="test", compare=tdf2, base=tdf1, primary_keys=["A"]
        )
        expected = DataFrameValidatorReport.empty()
        assert report == expected

    def test_columns_with_base_postfix_should_not_cause_errors(
        self, faker, mocker: MockFixture, spark: SparkSession, bindenv, freezer
    ):
        write = mocker.patch("pyspark.sql.DataFrame.write")
        write.return_value = mocker.MagicMock()
        df = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 5)),
            columns=list(["id", "f2_base", "f3_base", "f2", "f3"]),
        )

        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("f2_base", IntegerType(), True),
                StructField("f3_base", IntegerType(), True),
                StructField("f2", LongType(), True),
                StructField("f3", LongType(), True),
            ]
        )

        df1 = spark.createDataFrame(df, schema)
        df2 = spark.createDataFrame(df, schema)

        validator = DataFrameValidator()
        try:
            validator.validateDF(
                session=spark,
                name="test",
                compare=df2,
                base=df1,
                primary_keys=["id"],
            )
        except Exception as e:
            assert False, f"Exception raised: {e}"
