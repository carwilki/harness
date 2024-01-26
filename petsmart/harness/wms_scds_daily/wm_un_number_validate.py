dbutils.library.restartPython()
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max, min

from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi
from petsmart.validation.wms_scds_daily.validate_pets_with_pre_tables import (
    validate_pets_with_pre_table,
)

spark: SparkSession = spark
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
snapshot_name = "WM_UN_NUMBER"
hjm = api.getHarnessJobById("01298d4f-934f-439a-b80d-251987f5422")
snapshotter = hjm.snapshotters.get(snapshot_name)
pre_overide = ["LOCATION_ID", "UN_NUMBER_ID"]
ret = validate_pets_with_pre_table(
    snapshot=snapshotter, spark=spark, pre_keys_overide=pre_overide
)
snapshotter.config.snapshot_report = ret
print(ret)
hjm.update()
