from harness.sources.AbstractSource import AbstractSource
from pyspark.sql import SparkSession
from pyspark.sql import Catalog
from harness.sources.JDBCSource import NetezzaJDBCSource
from harness.target.TableTarget import TableTarget


class TestDataManager:
    @classmethod
    def coinfigureTestData(cls, source: AbstractSource, spark: SparkSession):
        match source:
            case _ if isinstance(source, NetezzaJDBCSource):
                cls.configureJDBCSourceForTest(source, spark)

    # TODO:this is a hacky way to do this, but it works for now. will need to generalize this
    @classmethod
    def configureJDBCSourceForTest(
        cls,
        source: NetezzaJDBCSource,
        target: TableTarget,
        session: SparkSession,
        isBase: bool,
    ):
        cat = Catalog(session)
        if isBase:
            schema = "qa_refine"
        else:
            schema = "qa_raw"

        table = source.config.source_table
        if cat.tableExists(schema, table):
            session.sql(f"truncate table {schema}.{table}")
            session.sql(
                f"insert into {schema}.{table} select * from {source.config.source_schema}.{source} version as of 0;"
            ).collect()
        else:
            session.sql(
                f"CREATE TABLE  if not exists {schema}.{table} as select * from {target.config.snapshot_target_schema}.{target.config.snapshot_target_table} version as of 0;"
            ).collect()
