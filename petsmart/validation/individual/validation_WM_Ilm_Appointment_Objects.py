from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max, min

from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi
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
snapshot_name = "WM_ILM_APPOINTMENT_OBJECTS"
hjm = api.getHarnessJobById("01298d4f-934f-439a-b80d-251987f5422")
hjm.updateValidaitonFilter(snapshotName=snapshot_name, filter="""""")
target: TableTargetConfig = hjm.getTargetConfigForSnapshot(snapshot_name)

keys = target.primary_key
raw_keys = ["ID", "LOCATION_ID"]
# for key in keys:
#     raw_keys.append(key.lower().replace("wm_", ""))

site_profile = spark.sql(
    "select STORE_NBR,LOCATION_ID from qa_legacy.SITE_PROFILE"
).cache()


refine_full = (
    spark.sql(f"select * from {target.test_target_schema}.{target.test_target_table}")
    .repartition(25)
    .distinct()
    .cache()
)

refine_keys = refine_full.select(keys).repartition(25).distinct().cache()

pre_full = (
    spark.sql(f"select * from qa_raw.{target.test_target_table}_PRE")
    .distinct()
    .repartition(25)
    .cache()
)

pre_keys = (
    pre_full.join(site_profile, pre_full.DC_NBR == site_profile.STORE_NBR)
    .repartition(25)
    .select(raw_keys)
    .withColumnRenamed("ID", "WM_ILM_APPOINTMENT_OBJECTS_ID")
    .distinct()
    .cache()
)

v1 = (
    spark.sql(f"select * from {hjm.getSnapshotTable(snapshot_name, 1)}")
    .repartition(25)
    .distinct()
    .cache()
)

v2 = (
    spark.sql(f"select * from {hjm.getSnapshotTable(snapshot_name, 2)}")
    .repartition(25)
    .distinct()
    .cache()
)

v1_keys = v1.select(keys).cache()

v2_keys = v2.select(keys).cache()

report: DataFrameValidatorReport = None
report: DataFrameValidatorReport = hjm.runSingleValidation(snapshot_name)

vA = v1_keys.unionByName(v2_keys).distinct().cache()
v1_only = v1_keys.exceptAll(v2_keys).cache()
v2_only = v2_keys.exceptAll(v1_keys).cache()
v1_v2_shared = vA.exceptAll(v1_only).exceptAll(v2_only).cache()
inserts_not_in_snapshots = pre_keys.exceptAll(v2_only)
new_inserts = refine_keys.exceptAll(v1_keys).cache()
delta_pre = pre_keys.exceptAll(v1_keys).cache()
expected_records_inserted = delta_pre.unionAll(v2_only).exceptAll(v1_only).cache()
records_not_inserted = expected_records_inserted.exceptAll(refine_keys).cache()
unexpected_records_in_refine = (
    refine_keys.exceptAll(pre_keys).exceptAll(v1_keys).exceptAll(v2_keys).cache()
)
records_in_pre_not_in_refine = pre_keys.exceptAll(refine_keys).cache()
records_in_refine_not_in_pre = refine_keys.exceptAll(refine_keys).cache()
records_in_v2_not_in_refine = v2_only.exceptAll(refine_keys).cache()
records_in_v1_not_in_refine = v1_only.exceptAll(refine_keys).cache()
min_test_timstamp = v1.select(min("WM_CREATED_TSTMP")).collect()[0][0]
max_test_timstamp = v2.select(max("WM_CREATED_TSTMP")).collect()[0][0]
print(
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
)
print("start of validation report")
print(report.summary)
print(f"pre-table records:                                  {pre_full.count()}")
print(f"pre-table post join                                 {delta_pre.count()}")
print(
    f"min and max snapshot timestamps:                    {min_test_timstamp}, {max_test_timstamp}"
)
print(f"unique_v1:                                          {v1_only.count()}")
print(f"unique_v2:                                          {v2_only.count()}")
print(f"v1_v2_shared_records:                               {v1_v2_shared.count()}")
print(f"all version:                                        {vA.count()}")
print(f"pre existing records:                               {v1_keys.count()}")
print(f"new records in pre-table:                           {delta_pre.count()}")
print(
    f"records not in snapshot:                            {inserts_not_in_snapshots.count()}"
)
print(
    f"expected inserted records:                          {expected_records_inserted.count()}"
)
print(f"records inserted into refine:                       {new_inserts.count()}")
print(f"records in refine:                                  {refine_keys.count()}")
print(
    f"records not inserted:                               {records_not_inserted.count()}"
)
print(
    f"unexpected records in refine:                       {unexpected_records_in_refine.count()}"
)
print(
    f"records in pre but not in refine:                   {records_in_pre_not_in_refine.count()}"
)
print(
    f"records in refine but not pre:                      {records_not_inserted.count()}"
)
print(
    f"records in v1 but not in refine:                    {records_in_v1_not_in_refine.count()}"
)
print(
    f"records in v2 but not in refine:                    {records_in_v2_not_in_refine.count()}"
)
print(
    f"records that are filtered by null update date:      {pre_full.filter('LAST_UPDATED_DTTM is null').count()}"
)
print("end of validation report")
print(
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
)
