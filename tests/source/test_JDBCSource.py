from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DecimalType,
    ShortType,
    IntegerType,
    LongType,
)
from pytest_mock import MockFixture
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from pyspark.sql.dataframe import DataFrame
from harness.sources.JDBCSource import NetezzaJDBCSource
from harness.sources.JDBCSourceConfig import JDBCSourceConfig


class TestJDBCSource:
    def test_can_create_source(
        self,
        mocker: MockFixture,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        jdbcSourceConfig: JDBCSourceConfig,
    ):
        session: SparkSession = mocker.MagicMock()

        source = NetezzaJDBCSource(
            harness_config=harnessConfig,
            snapshot_config=snapshotConfig,
            config=jdbcSourceConfig,
            session=session,
        )

        assert source is not None

    def test_can_read_from_source(
        self,
        mocker: MockFixture,
        spark: SparkSession,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        jdbcSourceConfig: JDBCSourceConfig,
        bindenv,
    ):
        rdf: DataFrame = spark.createDataFrame([{"a": 1}])
        spark_mock = mocker.MagicMock()
        type(spark_mock).write = spark_mock
        type(spark_mock).read = spark_mock
        spark_mock.format.return_value = spark_mock
        spark_mock.option.return_value = spark_mock
        spark_mock.options.return_value = spark_mock
        spark_mock.load.return_value = rdf

        source = NetezzaJDBCSource(
            harness_config=harnessConfig,
            snapshot_config=snapshotConfig,
            config=jdbcSourceConfig,
            session=spark_mock,
        )

        df = source.read()

        assert df.collect() == rdf.collect()
        spark_mock.format.assert_called_once_with("jdbc")
        spark_mock.options.assert_called()
        spark_mock.load.assert_called_once()

    def test_can_convert_decimal_n_0_precions_to_int_types(
        self,
        mocker: MockFixture,
        spark: SparkSession,
        harnessConfig: HarnessJobConfig,
        snapshotConfig: SnapshotConfig,
        jdbcSourceConfig: JDBCSourceConfig,
        bindenv,
    ):
        values = [
            [Decimal(1), Decimal(322.0), Decimal(38885), Decimal(4.0), Decimal(5.0)]
        ]
        schema = StructType(
            [
                StructField("f1", DecimalType(1, 0), True),
                StructField("f2", DecimalType(4, 0), True),
                StructField("f3", DecimalType(8, 0), True),
                StructField("f4", DecimalType(14, 0), True),
                StructField("f5", DecimalType(22, 0), True),
            ]
        )
        
        expected = StructType(
            [
                StructField("f1", ShortType(), True),
                StructField("f2", ShortType(), True),
                StructField("f3", IntegerType(), True),
                StructField("f4", LongType(), True),
                StructField("f5", DecimalType(22, 0), True),
            ]
        )
        
        rdf: DataFrame = spark.createDataFrame(values, schema=schema)

        spark_mock = mocker.MagicMock()
        type(spark_mock).write = spark_mock
        type(spark_mock).read = spark_mock
        spark_mock.format.return_value = spark_mock
        spark_mock.option.return_value = spark_mock
        spark_mock.options.return_value = spark_mock
        spark_mock.load.return_value = rdf

        source = NetezzaJDBCSource(
            harness_config=harnessConfig,
            snapshot_config=snapshotConfig,
            config=jdbcSourceConfig,
            session=spark_mock,
        )

        df = source.read()
       
        assert df.schema == expected

        spark_mock.format.assert_called_once_with("jdbc")
        spark_mock.options.assert_called()
        spark_mock.load.assert_called_once()
