from pyspark.sql import SparkSession
from harness.config.config import SourceConfig
from harness.sources.source import AbstractSource


class JDBCSource(AbstractSource):
    def __init__(self, config: SourceConfig, session: SparkSession):
        super().__init__(config, session)

    def read(self, table_name):
        SQL = f"(select * from {table_name} where day_dt >= '2020-01-01') as src"
               
        # user_name = dbutils.secrets.get(scope = "netezza_petsmart_keys", key = "username")
        # password = dbutils.secrets.get(scope = "netezza_petsmart_keys", key = "password")
        
        user_name = self.session.conf.get("spark.user")
        password = self.session.conf.get("spark.password")
        jdbc_url = "jdbc:netezza:/172.16.73.181:5480/EDW_PRD"
        num_part = 3
        reader_options = {"url": jdbc_url, "dbtable": f"{SQL}", "user": user_name, "password": password, "numPartitions" : num_part}
 
        nz_Source_DF = self.session.read \
                            .format("jdbc")\
                            .option("driver","org.netezza.Driver")\
                            .options(**reader_options)\
                            .load()
                            
        return nz_Source_DF