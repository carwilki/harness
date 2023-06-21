from harness.config.EnvConfig import EnvConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.ValidatorConfig import ValidatorConfig
from harness.manager.HarnessJobManager import HarnessJobManager
from harness.sources.JDBCSourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig
from pyspark.sql.session import SparkSession

username = dbutils.secrets.get(scope="netezza_petsmart_keys", key="username")
password = dbutils.secrets.get(scope="netezza_petsmart_keys", key="password")

env1 = EnvConfig(
    workspace_url="https://3986616729757273.3.gcp.databricks.com/",
    workspace_token="dapi5492460db39d145778c9d436bbbf1842",
    metadata_schema="nzmigration",
    metadata_table="harness_metadata",
    snapshot_schema="nzmigration",
    snapshot_table_post_fix="_gold",
    jdbc_url="jdbc:netezza://172.16.73.181:5480/EDW_PRD",
    jdbc_user=username,
    jdbc_password=password,
    jdbc_driver="org.netezza.Driver",
)
job_id = "01298d4f-934f-439a-b80d-251987f54415"

sc = JDBCSourceConfig(
    source_table="""WM_E_CONSOL_PERF_SMRY""",
    source_schema="""WMSMIS""",
    source_filter="""WM_CREATE_TSTMP >= date_trunc('day', now() - interval '60 days') OR update_tstmp >= date_trunc('day', now() - interval '60 days')""",
)

vc = ValidatorConfig(join_keys=[("WM_PERF_SMRY_TRAN_ID", "WM_PERF_SMRY_TRAN_ID")])

tc = TableTargetConfig(
    target_table="WM_E_CONSOL_PERF_SMRY", target_schema="hive_metastore.nzmigration"
)
join_keys = [(str("WM_PERF_SMRY_TRAN_ID"), str("WM_PERF_SMRY_TRAN_ID"))]


snc = SnapshotConfig(
    job_id=job_id, target=tc, source=sc, name="WM_E_CONSOL_PERF_SMRY", validator=vc
)

hjc = HarnessJobConfig(job_id=job_id, sources={"WM_E_CONSOL_P   ERF_SMRY": snc}, inputs={})
spark: SparkSession = SparkSession.getActiveSession()

hjm = HarnessJobManager(hjc, env1, spark)
print("snapshotting")
hjm.snapshot()
