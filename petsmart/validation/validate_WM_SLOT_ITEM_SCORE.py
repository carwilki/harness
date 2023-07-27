from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi
from pyspark.sql import SparkSession


def generate_comparison_query(refine, raw, v1, refine_keys, raw_keys) -> str:
    return f"""with rf_only as (
  (
  --create a set of locid and msdid
  select {refine_keys} from {refine}
  where LOCATION_ID =1288 or LOCATION_id=1186)
except
  ( --remove all the ids that are present in the pre_table
  select {raw_keys} from qa_legacy.SITE_PROFILE
  inner join {raw} on store_nbr = DC_NBR
  where LOCATION_ID =1288 or LOCATION_id=1186)),
final as (
  (select {refine_keys} from rf_only)
  except(
    --except all from the remainder if they existed in the refine tables pre state.
    Select {refine_keys} from {v1})
    )
select * from final;
"""


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
snapshot_name = "WM_SLOT_ITEM_SCORE"
hjm = api.getHarnessJobById("01298d4f-934f-439a-b80d-251987f5422")

hjm.updateValidaitonFilter(
    snapshotName=snapshot_name,
    filter="""where ('2023-06-15 22:04:06.000'<WM_CREATE_TSTMP and WM_CREATE_TSTMP <'2023-06-23 00:03:33.000')
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)"""
)

print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
print("start of validation\n")


hjm.runSingleValidation(snapshot_name)

report = hjm.getReport(snapshot_name)
target = hjm.getTargetConfigForSnapshot(snapshot_name)

refine = f"{target.test_target_schema}.{target.test_target_table}"
pre = f"qa_raw.{target.test_target_table}_PRE"
keys = ",".join(str(e) for e in target.primary_key).lower()
raw_keys = ",".join(str(e) for e in target.primary_key).lower().replace("wm_", "")
v1 = hjm.getSnapshotTable(snapshot_name, 1)

all_records_not_apearing_in_either_v1_pre_v2 = generate_comparison_query(refine, pre, v1, keys, raw_keys)

not_present_in_pre_v1_v2_tests = spark.sql(
    all_records_not_apearing_in_either_v1_pre_v2
).count()

print(f"{not_present_in_pre_v1_v2_tests} records not present in either v1 or v2 or pre\n")

print("end of validation\n")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
