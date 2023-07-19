from faker import Faker
from pyspark.sql import SparkSession
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
