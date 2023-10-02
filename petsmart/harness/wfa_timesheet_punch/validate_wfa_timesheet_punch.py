from io import StringIO
import sys
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
    netezza_jdbc_num_part=18,
)

job_id = "wf_WFA_TIME_SHEET_PUNCH"
api = HarnessApi(env, spark)
hjm = api.getHarnessJobById(job_id)

for snapshot in hjm.snapshoters.values():
    try:
        ret = snapshot.validateResults()
        if ret is not None:
            snapshot.config.snapshot_report = ret.summary
            print(snapshot.config.snapshot_report)
        else:
            print(f"Snapshot {snapshot.config.job_id}:{snapshot.config.name} has no validaiton report")
    except Exception as e:
        ret = f"Snapshot {snapshot.config.job_id}:{snapshot.config.name} failed validation. exception below\n{e}"
        print(ret)
        snapshot.config.snapshot_report = ret

hjm.updateTestSchema()

hjm.update()
