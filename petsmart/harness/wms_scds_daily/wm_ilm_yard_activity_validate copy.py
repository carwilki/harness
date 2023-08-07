dbutils.library.restartPython()

from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max, min

from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi
from harness.target.TableTargetConfig import TableTargetConfig
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport
from petsmart.validation.wms_scds_daily.validate_pets_with_pre_tables import (
    validate_pets_with_pre_table,
)

spark: SparkSession = spark
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

api = HarnessApi(env, spark)
snapshot_name = "WM_ILM_YARD_ACTIVITY"
hjm = api.getHarnessJobById("01298d4f-934f-439a-b80d-251987f5422")
snapshotter = hjm.snapshoters.get(snapshot_name)
target: TableTargetConfig = snapshotter.config.target

v1snapshot = snapshotter.target.getSnapshotTableName(1)
v2snapshot = snapshotter.target.getSnapshotTableName(2)

filter = "location_id in(1288,1186)"
pre_filter = "dc_nbr in(36,38)"
keys = target.primary_key
raw_keys = ["LOCATION_ID", "YARD_ID","YARD_ZONE_ID"]

site_profile = spark.sql(
    "select STORE_NBR,LOCATION_ID from qa_legacy.SITE_PROFILE"
).cache()
refine_full = spark.sql(
    f"select * from {target.test_target_schema}.{target.test_target_table} where {filter}"
).cache()
refine_keys = refine_full.select(keys).repartition(25).cache()
v1 = spark.sql(f"select * from {v1snapshot} where {filter}").cache()
v2 = spark.sql(f"select * from {v2snapshot} where {filter}").cache()
v1_keys = v1.select(keys).cache()
v2_keys = v2.select(keys).cache()
pre_full = (
    spark.sql(f"select * from qa_raw.{target.test_target_table}_PRE where {pre_filter}")
    .drop("LOCATION_ID")
    .cache()
)

pre_post_join = pre_full.join(
    site_profile, pre_full.DC_NBR == site_profile.STORE_NBR
).select(pre_full["*"], site_profile["location_id"])
pre_keys = pre_post_join.select(raw_keys)
report: DataFrameValidatorReport = snapshotter.validateResults()
vA = v1_keys.unionByName(v2_keys).distinct().cache()
v1_only = v1_keys.exceptAll(v2_keys).cache()
v2_only = v2_keys.exceptAll(v1_keys).cache()
v1_v2_shared = vA.exceptAll(v1_only).exceptAll(v2_only).cache()
v2_missing_from_pre = v2_only.exceptAll(pre_keys)
new_inserts = refine_keys.exceptAll(v1_keys).cache()
records_in_pre_not_in_refine = pre_keys.exceptAll(refine_keys).cache()
records_in_v1_not_in_refine = v1_only.exceptAll(refine_keys).cache()
records_in_v2_not_in_refine = (
    v2_only.exceptAll(v2_missing_from_pre).exceptAll(refine_keys).cache()
)
new_records = pre_keys.exceptAll(v1_keys).cache()
new_records_in_refine_but_not_pre = (
    refine_keys.exceptAll(new_inserts).exceptAll(v2_keys).exceptAll(v1_keys)
)
v2_in_refine = v2_keys.exceptAll(v2_missing_from_pre)
ret = "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
ret += "start of validation report\n"
ret += f"\n{report.summary}\n"
ret += f"pre-table records:                                  {pre_full.count()}\n"
ret += f"pre-keys post join records:                         {pre_post_join.count()}\n"
ret += f"unique_v1:                                          {v1_only.count()}\n"
ret += f"unique_v2:                                          {v2_only.count()}\n"
ret += f"v1_v2_shared_records:                               {v1_v2_shared.count()}\n"
ret += f"all versions(v1 & v2):                              {vA.count()}\n"
ret += "\n"
ret += f"records already in refine:                          {v1.count()}\n"
ret += f"new records in pre-table:                           {new_records.count()}\n"
ret += f"expected inserted records:                          {new_records.count()}\n"
ret += f"records inserted into refine:                       {new_inserts.count()}\n"
ret += f"records in refine:                                  {refine_keys.count()}\n"
ret += f"master records not in pre:                          {v2_missing_from_pre.count()}\n"
ret += f"master records in refine:                           {v2_in_refine.count()}\n"
ret += f"v1 records missing in refine:                       {records_in_v1_not_in_refine.count()}\n"
ret += f"master not in pre and missing from refine:          {records_in_v2_not_in_refine.count()}\n"
ret += f"new records from pre not in refine:                 {records_in_pre_not_in_refine.count()}\n"
ret += f"new records in refine but not pre:                  {new_records_in_refine_but_not_pre.count()}\n"
ret += "end of validation report\n"
ret += "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
snapshotter.config.snapshot_report = ret
print(ret)
hjm.update()
