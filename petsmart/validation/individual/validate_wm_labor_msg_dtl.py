from pyspark.sql import SparkSession

from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi

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
hjm = api.getHarnessJobById("01298d4f-934f-439a-b80d-251987f5422")
hjm.updateValidaitonFilter(
    snapshotName="WM_LABOR_MSG_DTL",
    filter="""where ('2023-06-15 01:52:33.000'<WM_CREATED_TSTMP and WM_CREATED_TSTMP <'2023-06-23 05:21:56.000')
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)

print(
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
)
print("start of validation\n")

hjm.runSingleValidation("WM_LABOR_MSG_DTL")
report = hjm.getReport("WM_LABOR_MSG_DTL")
all_records_not_apearing_in_either_v1_pre_v2 = """with rf_only as ((
  --create a set of locid and msdid
  select rf.location_id,rf.WM_LABOR_MSG_DTL_ID from qa_refine.WM_LABOR_MSG_DTL rf
  where rf.LOCATION_ID =1288 or rf.LOCATION_id=1186)
except
( --remove all the ids that are present in the pre_table
  select sp.location_id,r.LABOR_MSG_DTL_ID from qa_legacy.SITE_PROFILE sp
inner join qa_raw.WM_LABOR_MSG_DTL_PRE r on sp.store_nbr = r.DC_NBR
where sp.LOCATION_ID =1288 or sp.LOCATION_id=1186)),
final as ((
  --build a set of ids from refine
  select rf.location_id,rf.WM_LABOR_MSG_DTL_ID from qa_refine.WM_LABOR_MSG_DTL rf
  --join in the ids that are in the refine only table to create a set of ids that are in refine but were
  --seen in the pre table
  inner join rf_only o on rf.location_id = o.location_id and rf.WM_LABOR_MSG_DTL_ID = o.WM_LABOR_MSG_DTL_ID)
except(
  --except all from the remainder if they existed in the refine tables pre state.
  Select location_id,WM_LABOR_MSG_DTL_ID from nzmigration.wms_to_scds_daily_WM_LABOR_MSG_DTL_v1))
select * from final;
"""

not_present_in_pre_v1_v2_tests = spark.sql(
    all_records_not_apearing_in_either_v1_pre_v2
).count()

print(
    f"{not_present_in_pre_v1_v2_tests} records not present in either v1 or v2 or pre\n"
)

print("end of validation\n")
print(
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
)
