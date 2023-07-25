from harness.config.EnvConfig import EnvConfig
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
    netezza_jdbc_url="jdbc:netezza://172.16.73.181:5480/",
    netezza_jdbc_user=username,
    netezza_jdbc_password=password,
    netezza_jdbc_driver="org.netezza.Driver",
    netezza_jdbc_num_part=9,
)

api = HarnessApi(env, spark)
hjm = api.getHarnessJobById("01298d4f-934f-439a-b80d-251987f5422")
snapshots = hjm.config.snapshots.values()

#hjm.updateAllValidationFilters(filter="LOCATION_ID = 1288 or LOCATION_ID=1186")
#hjm.updateValidaitonFilter(snapshotName="WM_E_AUD_LOG",filter="(LOCATION_ID = 1288 or LOCATION_ID=1186) and ( WM_CREATE_TSTMP < '2023-06-23 05:25:35.000' and WM_CREATE_TSTMP >'2023-06-22 04:38:49.000' )")
hjm.setupTestData()
