from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi

dbrToken = dbutils.secrets.get(scope="db-field-eng", key="harness_dbr_token")
workspace = dbutils.secrets.get(scope="db-field-eng", key="harness_dbr_ws_url")

env = EnvConfig(
    workspace_url="https://3986616729757273.3.gcp.databricks.com/",
    workspace_token=dbrToken,
    metadata_schema="carson_wilkins_catalog.validator_dev",
    metadata_table="harness_metadata",
    snapshot_schema="carson_wilkins_catalog.validator_dev",
    snapshot_table_post_fix="_gold",
    databricks_jdbc_host="e2-demo-field-eng.cloud.databricks.com",
    databricks_jdbc_user="carson.wilkins@databricks.com",
    databricks_jdbc_pat=dbrToken,
    databricks_jdbc_http_path="sql/protocolv1/o/1444828305810485/0624-233030-6hmhjtlc",
)

api = HarnessApi(env, spark)
print(api.resetEverything())
jobId = "b53fb562-e4f1-485b-9877-deb3c7c78d92"
hjm = api.getHarnessJobById(jobId)
if hjm is None:
    raise Exception(f"Could not find Harness job with id: {jobId}")
#     hjm = api.createHarnessJobFromCSV(
#         jobId, "firday_demo_6_30", "./test.csv", "databricks_jdbc"
#     )

# # hjm.snapshot()
# # hjm.snapshot()
# # hjm.setupTestData()
# # hjm.executeTestCase()
print(hjm.validateResults())
