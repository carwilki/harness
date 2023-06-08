from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from functools import reduce
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.sdk.service import JobsService
from harness.snaphotter.rocky.rocky_config import RockySnapshotConfig
from harness.config.env import PetSmartEnvConfig
from harness.snaphotter.snapshotter import Snapshotter


class RockySnapshotter(Snapshotter):
    def __init__(
        self,
        config: RockySnapshotConfig,
        env: PetSmartEnvConfig,
        session: SparkSession,
    ) -> None:
        self.config = config
        self.session = session
        self.env = env
        self.host = env.workspace_url
        self.token = env.tocken
        self.rocky_job_id = env.rocky_job_id
        self.api_client = ApiClient(host=self.host, token=self.token)
        self.jobs_service = JobsService(self.api_client)

    def insert_rocky_config(self):
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
        snowflake_post_sql)
        VALUES (
        {self.config.table_group},--table_group
        null,--table_group_desc
        {self.config.source_type},--source_type
        {self.config.source_db},--source_db
        {self.config.source_table},--source_table
        null,--table_desc
        {self.config.is_pii},--is_pii
        null,--pii_type
        {self.config.has_hard_deletes},--has_hard_deletes
        {self.config.target_sink},--target_sink
        {self.config.target_db},--target_db
        null,--target_schema
        {self.config.target_table_name},--target_table_name
        {self.config.load_type},--load_type
        null, --source_delta_column
        null, --primary_key
        null,--initial_load_filter
        {self.config.load_frequency}, --load_frequency
        {self.config.load_cron_expr}, --load_cron_expr
        null, --tidal_dependencies
        null, --expected_start_time
        array{reduce(lambda a,b: a +','+b,self.config.job_watchers)}, --job_watchers
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

        self.session.sql(insert).collect()

    def trigger_rocky_creation_job(self):
        """This function triggers the rocky job via service api call to retreive the data after
        the job has been configured

        Args:
            config (RockyConfig): the rocky configuration
        """
        dbutils = DBUtils(spark)

        host = dbutils.secrets.get(scope=config.scope, key=config.host_key)
        token = dbutils.secrets.get(scope=config.scope, key=config.token_key)
        jobiid = dbutils.secrets.get(scope=config.scope, key=config.job_id_key)
        if host is None or token is None:
            raise ValueError("host or token is missing from the secrets store")

        api_client = ApiClient(host=host, token=token)

        jobs_service = JobsService(api_client)

        jobs_service.run_now(job_id=jobiid)

    def execute_rocky_job(self):
        """This function executes the rocky job via service api call to retreive the data after
        the job has been configured

        Args:
            config (RockyConfig): the rocky configuration
        """

        rocky_run_id = jobs_service.run_now(config.job_id)

        return rocky_run_id

    def strip_rocky_metadata(self):
        """This function strips off the rocky speciffic metadata from the rocky table
        the data is then written to the target table
        Args:
            config (RockyConfig): the rocky configuration
            spark (SparkSession): requires a spark session to function
        """
        spark.sql(
            f"select * from {config.rocky_target_db}.{config.rocky_target_table_name}"
        ).drop("rocky_ingestion_metadata").write.format("delta").mode(
            "overwrite"
        ).saveAsTable(
            f"{config.target_db}.{config.target_table_name}"
        )

    def wait_for_run_compplete(self):
        pass

    def wait_for_rocky_create_complete(run_id: int):
        pass
