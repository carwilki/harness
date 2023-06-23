from harness.sources.AbstractSource import AbstractSource
from pyspark.sql import SparkSession

from harness.sources.JDBCSource import JDBCSource
from harness.target.TableTarget import TableTarget


class TestDataManager:
    @classmethod
    def coinfigureTestData(cls, source: AbstractSource, spark: SparkSession):
        match source:
            case _ if isinstance(source, JDBCSource):
                cls.configureJDBCSourceForTest(source, spark)

    # TODO:this is a hacky way to do this, but it works for now. will need to generalize this
    @classmethod
    def configureJDBCSourceForTest(
        cls,
        source: JDBCSource,
        target: TableTarget,
        session: SparkSession,
        isBase: bool,
    ):
        if isBase:
            schema = "qa_refine"
        else:
            schema = "qa_raw"

        table = source.config.source_table
        session.sql(f"DROP TABLE IF EXISTS {schema}.{table};").collect()
        session.sql(
            f"CREATE TABLE {schema}.{table} as select * from {target.config.target_schema}.{target.config.target_table} version as of 0;"
        ).collect()
