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
    netezza_jdbc_url="jdbc:netezza://172.16.73.181:5480/",
    netezza_jdbc_user=username,
    netezza_jdbc_password=password,
    netezza_jdbc_driver="org.netezza.Driver",
    netezza_jdbc_num_part=9,
)
api = HarnessApi(env, spark)

job_id = "wf_Petshotel_Accrual"
hjm = api.getHarnessJobById(id=job_id)
# hjm = api.createHarnessJobFromCSV(id=job_id, name="wf_SVC_Service_Suspension_Log",path="tableList_wf_SVC_Service_Suspension_Log.csv",sourceType=SourceTypeEnum.netezza_jdbc)
hjm.updateTestSchema("PETSHOTEL_ACCRUAL", "qa_legacy")
hjm.updateTestSchema("SKU_PROFILE", "qa_legacy")
hjm.updateTestSchema("SITE_PROFILE", "qa_legacy")
hjm.updateTestSchema("TP_INVOICE", "qa_refine")
hjm.updateTestSchema("TP_INVOICE_SERVICE", "qa_refine")
hjm.updateSnapshotSchema("TP_EVENT", "data_harness_cust_sensitive")
hjm.updateTestSchema("TP_EVENT", "data_harness_cust_sensitive")
hjm.markInputsSnapshots(
    ["SKU_PROFILE", "SITE_PROFILE", "TP_INVOICE", "TP_INVOICE_SERVICE", "TP_EVENT"],
)
