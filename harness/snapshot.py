from typing import Optional
from config import RaptorConfig, SnapshotConfig,
from pyspark.sql import SparkSession,DataFrame
from pyspark.dbutils import DBUtils
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

def create_snapshot_table(session:SparkSession):
    session.sql("create table if not exists \
                qa_work.snapshot_metadata (snapshot_id string,snapshot_name string,target_config string,pre_table_configs string)")

def insert_snapshot_metadata(session:SparkSession,config:SnapshotConfig):
    session.sql("insert into qa_work.snapshot_metadata values ('{}','{}','{}','{}')".format(config.snapshot_id,config.snapshot_name,config.target_config.json(),config.pre_table_configs()))