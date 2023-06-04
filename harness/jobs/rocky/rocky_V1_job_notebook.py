from struct import unpack
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark:SparkSession = spark
dbutils:DBUtils = dbutils

dbutils.widgets.text("table_group","")
dbutils.widgets.text("table_group_desc","")
dbutils.widgets.text("source_type","")
dbutils.widgets.text("source_db","")
dbutils.widgets.text("source_table","")
dbutils.widgets.text("table_des","")
dbutils.widgets.text("is_pii","")
dbutils.widgets.text("pii_type","")
dbutils.widgets.text("has_hard_deletes","")
dbutils.widgets.text("target_sink","")
dbutils.widgets.text("target_db","")
dbutils.widgets.text("target_schema","")
dbutils.widgets.text("target_table_name","")
dbutils.widgets.text("load_type","")
dbutils.widgets.text("source_delta_colunm","")
dbutils.widgets.text("primary_key","")
dbutils.widgets.text("initial_load_filter","")
dbutils.widgets.text("load_frequency","")
dbutils.widgets.text("load_cron_expr","")

table_group = dbutils.widgets.get("table_group")
table_group_desc = dbutils.widgets.get("table_group_desc")
source_type = dbutils.widgets.get("source_type")
source_db = dbutils.widgets.get("source_db")
source_table = dbutils.widgets.get("source_table")
table_des = dbutils.widgets.get("table_des")
is_pii = dbutils.widgets.get("is_pii")
pii_type = dbutils.widgets.get("pii_type")
has_hard_deletes = dbutils.widgets.get("has_hard_deletes")
target_sink = dbutils.widgets.get("target_sink")
target_db = dbutils.widgets.get("target_db")
target_schema = dbutils.widgets.get("target_schema")
target_table_name = dbutils.widgets.get("target_table_name")
load_type = dbutils.widgets.get("load_type")
source_delta_colunm = dbutils.widgets.get("source_delta_colunm")
primary_key = dbutils.widgets.get("primary_key")
initial_load_filter = dbutils.widgets.get("initial_load_filter")
load_frequency = dbutils.widgets.get("load_frequency")
load_cron_expr = dbutils.widgets.get("load_cron_expr")

insert = f"""INSERT INTO qa_work.rocky_ingestion_metadata (table_group, table_group_desc, source_type, source_db, source_table, table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
target_db, target_schema, target_table_name, load_type, source_delta_column,primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql ,snowflake_post_sql )
VALUES (
{table_group},--table_group 
{table_group_desc or "null"},--table_group_desc
{source_type},--source_type 
{source_db},--source_db
{source_table},--source_table 
{table_des or "null"},--table_desc 
{is_pii},--is_pii 
{pii_type or "null"},--pii_type 
{has_hard_deletes},--has_hard_deletes 
{target_sink},--target_sink
{target_db},--target_db 
{target_schema or "null"},--target_schema 
{target_table_name},--target_table_name 
{load_type},--load_type
{source_delta_colunm or "null"}, --source_delta_column
{primary_key or "null"}, --primary_key 
{initial_load_filter},--initial_load_filter 
{load_frequency}, --load_frequency 
{load_cron_expr}, --load_cron_expr 
{unpack(tidal_dependencies) or "null"}, --tidal_dependencies
null, --expected_start_time 
array({unpack(job_watchers) or "null"}), --job_watchers
{max_retry}, --max_retry 
{job_tag}, --job_tag 
{is_scheduled}, --is_scheduled
null, --job_id 
{snowflake_ddl or "null"}, --snowflake_ddl 
{tidal_trigger_condition or "null"}, --tidal_trigger_condition
{disable_no_record_failure}, --disable_no_record_failure
{snowflake_pre_sql or "null"}, --snowflake_pre_sql
{snowflake_post_sql or "null"} --snowflake_post_sql"""
   
spark.sql(insert).collect()
