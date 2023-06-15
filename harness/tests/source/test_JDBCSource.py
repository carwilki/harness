from faker import Faker
from pytest_mock import MockFixture
from harness.sources.JDBCSource import JDBCSource
from pyspark.sql import SparkSession
from harness.sources.JDBCSourceConfig import JDBCSourceConfig


class TestJDBCSource:
    def test_can_create_source(self, mocker: MockFixture, faker: Faker):
        session: SparkSession = mocker.MagicMock()
        config = JDBCSourceConfig(
            source_filter=None,
            source_table=faker.pystr(),
            source_schema=faker.pystr(),
        )
        source = JDBCSource(config, session=session)

        assert source is not None

    def test_can_read_from_source(
        self, mocker: MockFixture, faker: Faker, spark: SparkSession
    ):
        rdf = spark.createDataFrame([{"a": 1}])
        spark_mock = mocker.MagicMock()
        type(spark_mock).write = spark_mock
        type(spark_mock).read = spark_mock
        spark_mock.format.return_value = spark_mock
        spark_mock.option.return_value = spark_mock
        spark_mock.options.return_value = spark_mock
        spark_mock.load.return_value = rdf

        session: SparkSession = mocker.MagicMock()
        session.read.format().option().options().load().return_value = rdf

        config = JDBCSourceConfig(
            source_filter=None,
            source_table=faker.pystr(),
            source_schema=faker.pystr(),
        )

        source = JDBCSource(config, session=spark_mock)
        df = source.read()
        assert df is rdf
        spark_mock.format.assert_called_once_with("jdbc")
        spark_mock.option.assert_called()
        spark_mock.options.assert_called()
        spark_mock.load.assert_called_once()
