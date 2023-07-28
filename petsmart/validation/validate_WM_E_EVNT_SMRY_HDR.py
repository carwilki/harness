from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi
from pyspark.sql import SparkSession, DataFrame
from uuid import uuid4

from harness.target.TableTargetConfig import TableTargetConfig
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport

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
snapshot_name = "WM_E_EVNT_SMRY_HDR"
hjm = api.getHarnessJobById("01298d4f-934f-439a-b80d-251987f5422")
hjm.updateValidaitonFilter(snapshotName=snapshot_name, filter="""""")
target: TableTargetConfig = hjm.getTargetConfigForSnapshot(snapshot_name)

keys = target.primary_key
raw_keys = []
for key in keys:
    raw_keys.append(key.lower().replace("wm_", ""))

refine = (
    spark.sql(f"select * from {target.test_target_schema}.{target.test_target_table}")
    .select(keys)
    .distinct()
    .cache()
)

pre = (
    spark.sql(f"select * from qa_raw.{target.test_target_table}_PRE")
    .distinct()
    .repartition(50)
    .cache()
)
v1 = (
    spark.sql(f"select * from {hjm.getSnapshotTable(snapshot_name, 1)}")
    .repartition(50)
    .select(keys)
    .distinct()
    .cache()
)
v2 = (
    spark.sql(f"select * from {hjm.getSnapshotTable(snapshot_name, 2)}")
    .repartition(50)
    .select(keys)
    .distinct()
    .cache()
)
site_profile = spark.sql(
    "select store_nbr,location_id from qa_legacy.SITE_PROFILE"
).cache()

report: DataFrameValidatorReport = None
report: DataFrameValidatorReport = hjm.runSingleValidation(snapshot_name)

pre_location_id = (
    pre.join(site_profile, pre.DC_NBR == site_profile.store_nbr)
    .select(raw_keys)
    .distinct()
).cache()

v1_count = v1.count()
v2_count = v2.count()
vA = v1.unionByName(v2).distinct().cache()
v1_only = v1.exceptAll(v2).cache()
v2_only = v2.exceptAll(v1).cache()
vA_minus_v1_only = vA.exceptAll(v1_only).cache()
vA_minus_v2_only = vA.exceptAll(v2_only).cache()
vA_count = vA.count()
vA_minus_v1_count = vA_minus_v1_only.count()
vA_minus_v2_count = vA_minus_v2_only.count()
inserts_not_in_snapshots = pre_location_id.exceptAll(vA)
new_inserts = refine.exceptAll(v1).cache()
new_pre_records = pre_location_id.exceptAll(v1).cache()
unaccounted_inserts = refine.exceptAll(pre_location_id).exceptAll(v1)
print(
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
)
print("start of validation report")
print(report.summary)
print(f"v1 count:                       {v1_count}")
print(f"v2 count:                       {v2_count}")
print(f"unique_v1:                      {v1_only.count()}")
print(f"unique_v2:                      {v2_only.count()}")
print(
    f"v1_v2_shared_records:           {vA.exceptAll(v1_only).exceptAll(v2_only).count()}"
)
print(f"all version:                    {vA.count()}")
print(f"pre existing records:           {v1.count()}")
print(f"new records in pre-table:       {new_pre_records.count()}")
print(f"records not in snapshot:        {inserts_not_in_snapshots.count()}")
print(f"expected inserted records:      {new_pre_records.count()}")
print(f"records inserted into refine:   {new_inserts.count()}")
print(f"records in refine:              {refine.count()}")
print(f"records not accounted for:      {unaccounted_inserts.count()}")
print("end of validation report")
print(
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
)
