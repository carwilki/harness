from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from pyspark.sql import SparkSession
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from pyspark.sql.types import ShortType, IntegerType, LongType, DecimalType
from pyspark.sql.functions import col
from logging import getLogger


def _convert_decimal_to_int_types(df):
    for feild in df.schema.fields:
        if isinstance(feild.dataType, DecimalType):
            if feild.dataType.scale == 0:
                if 0 < feild.dataType.precision < 5:
                    df = df.withColumn(feild.name, col(feild.name).cast(ShortType()))
                elif 5 < feild.dataType.precision < 12:
                    df = df.withColumn(feild.name, col(feild.name).cast(IntegerType()))
                elif 12 < feild.dataType.precision < 22:
                    df = df.withColumn(feild.name, col(feild.name).cast(LongType()))
    return df


username = dbutils.secrets.get(scope="netezza_petsmart_keys", key="username")
password = dbutils.secrets.get(scope="netezza_petsmart_keys", key="password")
token = dbutils.secrets.get(scope="netezza_petsmart_keys", key="workspace_token")

env = EnvConfig(
    workspace_url="https://3986616729757273.3.gcp.databricks.com/",
    workspace_token=token,
    metadata_schema="nzmigration",
    metadata_table="harness_metadata_v2",
    snapshot_schema="nzmigration",
    netezza_jdbc_url="jdbc:netezza://172.16.73.181:5480/",
    netezza_jdbc_user=username,
    netezza_jdbc_password=password,
    netezza_jdbc_driver="org.netezza.Driver",
    netezza_jdbc_num_part=9,
)
spark: SparkSession = spark
job_id = "01298d4f-934f-439a-b80d-251987f5422"
HarnessJobManagerEnvironment.bindenv(env)
mdm = HarnessJobManagerMetaData(spark)
config: HarnessJobConfig = mdm.getJobById(job_id, spark)
tables = []

# get all of the tables in the snapshot
for snapshot in config.snapshots.values():
    snapshot: SnapshotConfig = snapshot
    table = f"{config.job_name}_{snapshot.target.snapshot_target_table}"  # noqa: E501
    table_v1 = table + "_V1"
    table_v2 = table + "_V2"
# pt 1
# write it all out to temp tables for now
# for table in tables:
#     getLogger().info(f"creating temp_{table}")
#     df = spark.sql(f"select * from {snapshot.target.snapshot_target_schema}.{table}")
#     getLogger().info(f"converting table {table} types to int types")
#     df = _convert_decimal_to_int_types(df)
#     getLogger().info(f"Writing temp_{table} to table")
#     df.write.mode("overwrite").format("delta").saveAsTable(f"{snapshot.target.snapshot_target_schema}.temp_{table}")

# pt 2
# this is a structual migration so we need to drop and recreate the tables
for snapshot in config.snapshots.values():
    snapshot: SnapshotConfig = snapshot
    table = f"{config.job_name}_{snapshot.target.snapshot_target_table}"  # noqa: E501
    table_v1 = table + "_V1"
    table_v2 = table + "_V2"
    spark.sql(f"""drop table if exists {snapshot.target.snapshot_target_schema}.temp_{table_v1}""")
    spark.sql(f"""drop table if exists {snapshot.target.snapshot_target_schema}.temp_{table_v2}""")