from struct import unpack
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from functools import reduce
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.sdk.service import JobsService
from harness.config.config import SnapshotConfig
from harness.snaphotter.rocky.rocky_config import RockyConfig


def insert_rocky_config(config: RockyConfig, spark: SparkSession):
    """This function inserts the rocky configuration into the rocky table
    for the rocky job
    Args:
        config (RockyConfig): the rocky configuration
        spark (SparkSession): requires a spark session to function
    """
    insert = f"""INSERT INTO qa_work.rocky_ingestion_metadata (
    table_group, table_group_desc, source_type, source_db, source_table, 
    table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
    target_db, target_schema, target_table_name, load_type, source_delta_column,
    primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
    expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
    job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql ,
    snowflake_post_sql )
    VALUES (
    {config.table_group},--table_group
    null,--table_group_desc
    {config.source_type},--source_type
    {config.source_db},--source_db
    {config.source_table},--source_table
    null,--table_desc
    {config.is_pii},--is_pii
    null,--pii_type
    {config.has_hard_deletes},--has_hard_deletes
    {config.target_sink},--target_sink
    {config.target_db},--target_db
    null,--target_schema
    {config.target_table_name},--target_table_name
    {config.load_type},--load_type
    null, --source_delta_column
    null, --primary_key
    null,--initial_load_filter
    {config.load_frequency}, --load_frequency
    {config.load_cron_expr}, --load_cron_expr
    null, --tidal_dependencies
    null, --expected_start_time
    array{reduce(lambda a,b: a +','+b,config.job_watchers)}, --job_watchers
    1, --max_retry
    '{"Department":"Netezza-Migration"}', --job_tag
    false, --is_scheduled
    null, --job_id
    null, --snowflake_ddl
    null, --tidal_trigger_condition
    true, --disable_no_record_failure
    null, --snowflake_pre_sql
    null --snowflake_post_sql
    );"""

    spark.sql(insert).collect()

def strip_rocky_metadata(config: RockyConfig, spark: SparkSession):
    """This function strips off the rocky speciffic metadata from the rocky table 
    the data is then written to the target table
    Args:
        config (RockyConfig): the rocky configuration
        spark (SparkSession): requires a spark session to function
    """
    spark.sql(f"select * from {config.rocky_target_db}.{config.rocky_target_table_name}")\
        .drop("rocky_ingestion_metadata")\
        .write.format("delta").mode("overwrite").saveAsTable(f"{config.target_db}.{config.target_table_name}")

def execute_rocky_job(config: RockyConfig):
    """This function executes the rocky job via service api call to retreive the data after
    the job has been configured

    Args:
        config (RockyConfig): the rocky configuration
    """
    dbutils = DBUtils(spark)
    api_client = ApiClient(host=dbutils.secrets.get(scope=config.scope, key=config.host_key),
                           token=dbutils.secrets.get(scope=config.scope, key=config.token_key))
    jobs_service = JobsService(api_client)
    jobs_service.run_now(config.job_id)