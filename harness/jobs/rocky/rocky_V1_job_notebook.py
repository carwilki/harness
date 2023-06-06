from struct import unpack
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark: SparkSession = spark
dbutils: DBUtils = dbutils

dbutils.widgets.text("table_group", "")
dbutils.widgets.text("source_type", "")
dbutils.widgets.text("source_db", "")
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("is_pii", "")
dbutils.widgets.text("has_hard_deletes", "")
dbutils.widgets.text("target_sink", "")
dbutils.widgets.text("target_db", "")
dbutils.widgets.text("target_schema", "")
dbutils.widgets.text("target_table_name", "")
dbutils.widgets.text("load_type", "")
dbutils.widgets.text("load_frequency", "")
dbutils.widgets.text("load_cron_expr", "")
dbutils.widgets.text("job_watchers", "")

table_group = dbutils.widgets.get("table_group")
source_type = dbutils.widgets.get("source_type")
source_db = dbutils.widgets.get("source_db")
source_table = dbutils.widgets.get("source_table")
is_pii = dbutils.widgets.get("is_pii")
has_hard_deletes = dbutils.widgets.get("has_hard_deletes")
target_sink = dbutils.widgets.get("target_sink")
target_db = dbutils.widgets.get("target_db")
target_schema = dbutils.widgets.get("target_schema")
target_table_name = dbutils.widgets.get("target_table_name")
load_type = dbutils.widgets.get("load_type")
load_frequency = dbutils.widgets.get("load_frequency")
load_cron_expr = dbutils.widgets.get("load_cron_expr")
job_watchers = dbutils.widgets.get("job_watchers")

insert = f"""INSERT INTO qa_work.rocky_ingestion_metadata (
table_group, table_group_desc, source_type, source_db, source_table, 
table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
target_db, target_schema, target_table_name, load_type, source_delta_column,
primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql ,
snowflake_post_sql )
VALUES (
{table_group},--table_group
null,--table_group_desc
{source_type},--source_type
{source_db},--source_db
{source_table},--source_table
null,--table_desc
{is_pii},--is_pii
null,--pii_type
{has_hard_deletes},--has_hard_deletes
{target_sink},--target_sink
{target_db},--target_db
null,--target_schema
{target_table_name},--target_table_name
{load_type},--load_type
null, --source_delta_column
null, --primary_key
null,--initial_load_filter
{load_frequency}, --load_frequency
{load_cron_expr}, --load_cron_expr
null, --tidal_dependencies
null, --expected_start_time
array{job_watchers}, --job_watchers
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
