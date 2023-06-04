# Databricks notebook source
from pyspark.dbutils import DBUtils
dbutils:DBUtils = dbutils
dbutils.widgets.text("email_address","")
dbutils.widgets.text("teams_cahnnel","")
dbutils.widgets.text("primary_key_list","")
dbutils.widgets.text("source_query","")
dbutils.widgets.text("source_system_type","")
dbutils.widgets.text("target_query","")
dbutils.widgets.text("target_system_type","")
dbutils.widgets.text("output_table_name_format","")
# COMMAND ----------

# MAGIC %run /Shared/DataRaptor/Method_Defintion_submit_raptor_request

# COMMAND ----------

email_address=dbutils.widgets.get("email_address")
teams_cahnnel=dbutils.widgets.get("teams_cahnnel")
primary_key_list=dbutils.widgets.get("primary_key_list")
source_query=dbutils.widgets.get("source_query")
source_system_type=dbutils.widgets.get("source_system_type")
target_query=dbutils.widgets.get("target_query")
target_system_type=dbutils.widgets.get("target_system_type")
output_table_name_format = dbutils.widgets.get("output_table_name_format")

if(email_address == "") or (teams_cahnnel == "") or (primary_key_list == "") or (source_query == "") \
or (source_system_type == "") or (target_query == "") or (target_system_type == "") or (output_table_name_format == ""):
  raise Exception("Please provide all the required parameters")

# COMMAND ----------

submit_dataRaptor_job(email_address, primary_key_list, source_query, source_system_type, output_table_name_format, target_query, target_system_type)
