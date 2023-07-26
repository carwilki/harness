from harness.config.EnvConfig import EnvConfig
from pyspark.sql import SparkSession
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.validator.DataFrameValidator import DataFrameValidator

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

HarnessJobManagerEnvironment.bindenv(env)
spark: SparkSession = spark
compare = spark.sql("SELECT * FROM dev_refine.WM_ORDER_LINE_ITEM")
base = spark.sql("SELECT * FROM nzmigration.WMS_TO_SCDS_DAILY_WM_ORDER_LINE_ITEM_V2")
validator = DataFrameValidator()
validator.validateDF(
    "WM_ORDER_LINE_ITEM",
    compare,
    base,
    ["LOCATION_ID", "WM_ORDER_ID", "WM_LINE_ITEM_ID"],
    spark,
)
