
def insert_rocky_config(config:SnapshotConfig,session:SparkSession)->int:
    rocky = config.target_config
    insert = f"""INSERT INTO qa_work.rocky_ingestion_metadata (table_group, table_group_desc, source_type, source_db, source_table, table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
    target_db, target_schema, target_table_name, load_type, source_delta_column,primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
    job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql ,snowflake_post_sql )
    VALUES (
    {rocky.table_group},--table_group 
    {rocky.table_group_desc or "null"},--table_group_desc
    {rocky.source_type},--source_type 
    {rocky.source_db},--source_db
    {rocky.source_table},--source_table 
    {rocky.table_des or "null"},--table_desc 
    {rocky.is_pii},--is_pii 
    {rocky.pii_type or "null"},--pii_type 
    {rocky.has_hard_deletes},--has_hard_deletes 
    {rocky.target_sink},--target_sink
    {rocky.target_db},--target_db 
    {rocky.target_schema or "null"},--target_schema 
    {rocky.target_table_name},--target_table_name 
    {rocky.load_type},--load_type
    {rocky.source_delta_colunm or "null"}, --source_delta_column
    {rocky.primary_key or "null"}, --primary_key 
    {rocky.initial_load_filter},--initial_load_filter 
    {rocky.load_frequency}, --load_frequency 
    {rocky.load_cron_expr}, --load_cron_expr 
    {unpack(rocky.tidal_dependencies) or "null"}, --tidal_dependencies
    null, --expected_start_time 
    array({unpack(rocky.job_watchers) or "null"}), --job_watchers
    {rocky.max_retry}, --max_retry 
    {rocky.job_tag}, --job_tag 
    {rocky.is_scheduled}, --is_scheduled
    null, --job_id 
    {rocky.snowflake_ddl or "null"}, --snowflake_ddl 
    {rocky.tidal_trigger_condition or "null"}, --tidal_trigger_condition
    {rocky.disable_no_record_failure}, --disable_no_record_failure
    {rocky.snowflake_pre_sql or "null"}, --snowflake_pre_sql
    {rocky.snowflake_post_sql or "null"} --snowflake_post_sql"""
       
    session.sql(insert).collect()
        
    df:DataFrame = session.sql("select rocky_id from qa_work.rocky_ingestion_metadata where rocky_id in (select max(rocky_id) from qa_work.rocky_ingestion_metadata)")
    
    return  df.collect()[0][0]