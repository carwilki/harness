from pyspark.sql import SparkSession
from harness.config.EnvConfig import EnvConfig
from harness.config.ValidatorConfig import ValidatorConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.validator.DataFrameValidator import DataFrameValidator


spark = SparkSession.builder.getOrCreate()

env1 = EnvConfig(
    workspace_url="https://3986616729757273.3.gcp.databricks.com/",
    workspace_token="dapi5492460db39d145778c9d436bbbf1842",
    metadata_schema="nzmigration",
    metadata_table="harness_metadata",
    snapshot_schema="nzmigration",
    snapshot_table_post_fix="_gold",
    jdbc_url="jdbc:netezza://172.16.73.181:5480/EDW_PRD",
    jdbc_user='test',
    jdbc_password='test',
    jdbc_driver="org.netezza.Driver",
)
HarnessJobManagerEnvironment.bindenv(env1)
3345104
3171142
df1 = spark.read.table("hive_metastore.nzmigration.WM_E_CONSOL_PERF_SMRY")
df2 = spark.read.table("hive_metastore.nzmigration.WM_E_CONSOL_PERF_SMRY")

validator = DataFrameValidator(
    ValidatorConfig(join_keys=["WM_PERF_SMRY_TRAN_ID", "LOCATION_ID"])
)

report = validator.validate(
    name="WM_E_CONSOL_PERF_SMRY", master=df1, canidate=df2, session=spark
)
