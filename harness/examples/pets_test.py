from harness.config.EnvConfig import EnvConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.manager.HarnessApi import HarnessApi

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
job_id = "eddca4a3-fe6a-42bc-9845-83b0647bc141"
api = HarnessApi(env, spark)
#print(api.resetEverything())

hjm = api.getHarnessJobById(job_id)
if hjm is None:
    # raise Exception(f"Could not find Harness job with id: {job_id}")
    hjm = api.createHarnessJobFromCSV(
        id=job_id,
        name="wms_to_scds_4hr_test_2",
        path="tableList_WMS_To_SCDS_4hr.csv",
        sourceType=SourceTypeEnum.netezza_jdbc,
    )

hjm.snapshot()
hjm.snapshot()
hjm.setupTestData()
# hjm.executeTestCase()
print(hjm.validateResults())
