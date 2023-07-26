from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi
from pyspark.sql import SparkSession

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
    snapshotName="WM_E_AUD_LOG",
    filter="""where ('2023-06-11 05:17:13'<WM_CREATE_TSTMP and WM_CREATE_TSTMP <'2023-06-23 05:25:35')
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)

hjm.runSingleValidation("WM_E_AUD_LOG")
report = hjm.getReport("WM_E_AUD_LOG")
compare_only = report.compare_only
site_profile = "qa_legacy.SITE_PROFILE"
pre = "qa_raw.WM_E_AUD_LOG_PRE"
refine = "qa_refine.wm_e_aud_log"

dc_to_location_from_raw = f"""with 
refine as(select * from {refine} where LOCATION_ID =1288 or LOCATION_id=1186),
site_profile as (select * from {site_profile} where store_nbr = 36 or store_nbr=38),
raw as (select * from {pre} where DC_NBR =36 or DC_NBR = 38)
select count(distinct(m.location_id,m.wm_aud_id)) 
from refine r 
inner join site_profile sp on m.location_id = sp.location_id
inner join raw r on m.wm_aud_id = r.aud_id and sp.store_nbr = r.dc_nbr;"""

raw_query = (
    f"""select count(*) from {pre} where DC_NBR =36 or DC_NBR = 38;"""
)
raw_count = spark.sql(raw_query).collect()[0][0]
refined_count = spark.sql(dc_to_location_from_raw).collect()[0][0]
print(f"raw count:      {raw_count}")
print(f"refined count:  {refined_count}")