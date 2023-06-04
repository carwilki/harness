-- Databricks notebook source

INSERT INTO qa_work.rocky_ingestion_metadata (
table_group, table_group_desc, source_type, source_db, source_table, 
table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
target_db, target_schema, target_table_name, load_type, source_delta_column,
primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql ,
snowflake_post_sql 
)
VALUES (
"NZ_Migration",--table_group 
null,--table_group_desc
"NZ_Mako8",--source_type 
"EDW_PRD",--source_db
"SITE_PROFILE",--source_table 
null,--table_desc 
false,--is_pii 
null,--pii_type 
false,--has_hard_deletes 
"delta",--target_sink
"refine",--target_db 
null,--target_schema 
"site_profile",--target_table_name 
"full",--load_type
null, --source_delta_column
null, --primary_key 
null,--initial_load_filter 
"one-time", --load_frequency 
'0 0 6 ? * *', --load_cron_expr 
null, --tidal_dependencies
null, --expected_start_time 
array("rrajamani@petsmart.com", "pshekhar@petsmart.com"), --job_watchers
1, --max_retry 
'{"Department":"Netezza-Migration"}', --job_tag 
false, --is_scheduled
null, --job_id 
null, --snowflake_ddl 
null, --tidal_trigger_condition
true, --disable_no_record_failure
null, --snowflake_pre_sql
null --snowflake_post_sql
);

