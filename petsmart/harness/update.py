# Databricks notebook source
from harness.config.EnvConfig import EnvConfig
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
  netezza_jdbc_num_part=18,
)
api = HarnessApi(env, spark)

job_id = 'wf_workforce_annalyitcs'

# hjm = api.createHarnessJobFromCSV(id=job_id, name=job_id,path="tableList_workforce_annalytics.csv",sourceType=SourceTypeEnum.netezza_jdbc)
# hjm.snapshot()

hjm = api.getHarnessJobById(job_id)
snapshotter = hjm.snapshoters.get('WFA_TSCHD')
hjm