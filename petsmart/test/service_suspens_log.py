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

job_id = "wf_SVC_Service_Suspension_Log"
api = HarnessApi(env, spark)

hjm = api.getHarnessJobById(job_id)

# hjm.setupTestData()

# print(hjm.validateResults())
