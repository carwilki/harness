import csv
from json import loads
from typing import Optional

from pydantic import BaseModel
from pyspark.sql import SparkSession

from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.manager.HarnessApi import HarnessApi
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.sources.SourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig

username = dbutils.secrets.get(scope="netezza_petsmart_keys", key="username")
password = dbutils.secrets.get(scope="netezza_petsmart_keys", key="password")
token = dbutils.secrets.get(scope="netezza_petsmart_keys", key="workspace_token")

env = EnvConfig(
    workspace_url="https://3986616729757273.3.gcp.databricks.com/",
    workspace_token=token,
    metadata_schema="nzmigration",
    metadata_table="harness_metadata_v2",
    snapshot_schema="nzmigration",
    snapshot_table_post_fix="_gold",
    netezza_jdbc_url="jdbc:netezza://172.16.73.181:5480/",
    netezza_jdbc_user=username,
    netezza_jdbc_password=password,
    netezza_jdbc_driver="org.netezza.Driver",
    netezza_jdbc_num_part=9,
)
spark: SparkSession = spark
job_id = "01298d4f-934f-439a-b80d-251987f5422"
HarnessJobManagerEnvironment.bindenv(env)
with open("./wms_to_scds_daily_final.csv", "r") as file:
    tableList = csv.DictReader(file)
    sources = dict()
    for table in tableList:
        sc = JDBCSourceConfig(
            source_table=table["source_table"],
            source_schema=table["source_schema"],
            source_filter=table["source_filter"],
            source_type=SourceTypeEnum.netezza_jdbc,
        )
        tc = TableTargetConfig(
            primary_key=table["primary_key"].split("|"),
            snapshot_target_table=table["snapshot_target_table"],
            snapshot_target_schema=table["snapshot_target_schema"],
            test_target_schema=table["test_target_schema"],
            test_target_table=table["test_target_table"],
        )
        snc = SnapshotConfig(
            job_id=job_id, target=tc, source=sc, name=table["name"], version=2
        )
        sources[table["name"]] = snc
hjc = HarnessJobConfig(
    job_id=job_id, job_name="WMS_TO_SCDS_DAILY", snapshots=sources, version=2
)
mdm = HarnessJobManagerMetaData(spark)
mdm.create(hjc)

for snapshot in hjc.snapshots.values():
    table = f"{env.snapshot_schema}.{hjc.job_name}_{snapshot.target.snapshot_target_table}_V1"
    spark.sql(
        f"""create table if not exists {table}
            as select * from {env.snapshot_schema}.{snapshot.source.source_table} version as of 0"""
    )
    table = f"{env.snapshot_schema}.{hjc.job_name}_{snapshot.target.snapshot_target_table}_V2"
    spark.sql(
        f"""create table if not exists {table}
            as select * from {env.snapshot_schema}.{snapshot.source.source_table}"""
    )
