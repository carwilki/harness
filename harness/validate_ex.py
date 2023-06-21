from datetime import datetime
from io import StringIO

from datacompy import SparkCompare
from pyspark.sql import SparkSession
from harness.config.ValidatorConfig import ValidatorConfig
from harness.validator.AbstractValidator import AbstractValidator
from harness.validator.DataFrameValidator import DataFrameValidator


spark = SparkSession.builder.getOrCreate()

df1 = spark.read.table("hive_metastore.nzmigration.WM_E_CONSOL_PERF_SMRY")
df2 = spark.read.table("hive_metastore.nzmigration.WM_E_CONSOL_PERF_SMRY")

comparison = SparkCompare(
    spark_session=spark,
    base_df=df1,
    compare_df=df2,
    join_columns=("WM_PERF_SMRY_TRAN_ID","WM_PERF_SMRY_TRAN_ID")
)

comparison.report()
