dbutils.library.restartPython()

from io import StringIO
import sys
from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi
from petsmart.validation.framework.validate_pets_with_pre_tables import (
    validate_pets_with_pre_table,
)

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

job_id = "01298d4f-934f-439a-b80d-251987f5422"
api = HarnessApi(env, spark)
hjm = api.getHarnessJobById(job_id)
for snapshot in hjm.snapshoters.values():
    ret = ""
    try:
        ret = validate_pets_with_pre_table(snapshot=snapshot, spark=spark)
        snapshot.config.snapshot_report = ret
        print(ret)
    except Exception as e:
        ret = f"Snapshot {snapshot.config.job_id}:{snapshot.config.name} failed validation. exception below\n{e}"
        print(ret)
        snapshot.config.snapshot_report = ret
hjm.update()