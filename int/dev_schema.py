# Databricks notebook source
from harness.config.EnvConfig import EnvConfig
from harness.config.EnvironmentEnum import EnvironmentEnum
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.manager.HarnessApi import HarnessApi
#from harness.examples.demo.vars import job_id

username = dbutils.secrets.get(scope="HAATHEELOADER-mako8", key="username")
password = dbutils.secrets.get(scope="HAATHEELOADER-mako8", key="password")
token = dbutils.secrets.get(scope="netezza_petsmart_keys", key="workspace_token")

env = EnvConfig(
  workspace_url="https://3986616729757273.3.gcp.databricks.com/",
  workspace_token=token,
  metadata_schema="nzmigration",
  metadata_table="harness_metadata_v2",
  snapshot_schema="nzmigration",
  netezza_jdbc_url="jdbc:netezza://172.20.73.73:5480/",
  netezza_jdbc_user=username,
  netezza_jdbc_password=password,
  netezza_jdbc_driver="org.netezza.Driver",
  netezza_jdbc_num_part=9,
)
api = HarnessApi(env, spark)

job_id = 'wf_TimeSmart'
hjm = api.getHarnessJobById(job_id)
hjm.setupTestData(EnvironmentEnum.DEV)
# hjm = api.createHarnessJobFromCSV(id=job_id, name="wf_TimeSmart",path="tableList_wf_TimeSmart.csv",sourceType=SourceTypeEnum.netezza_jdbc)
# hjm.snapshot()

# COMMAND ----------

# job_id = "wf_SRC_Services_Reservation"
# api = HarnessApi(env, spark)
# hjm = api.getHarnessJobById(job_id)
# hjm.updateAllTargetSchema("qa_legacy")

# COMMAND ----------

# MAGIC %sql describe history dev_legacy.TS_ACTIVITY_XREF