from typing import Optional
from config import RaptorConfig, SnapshotConfig
from pyspark.sql import SparkSession,DataFrame
from pyspark.dbutils import DBUtils
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

dbutils:DBUtils = dbutils



def unpack(list:list[str])->Optional[str]:
    ret = " ".join(str(x) for x in list)
    if(len(ret)<=0):
        return None
    else:
        return ret

def take_snapshot(version:int,config:SnapshotConfig,session:SparkSession):
    insert_rocky_config(config=config,session=session)


    
    
  

