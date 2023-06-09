
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from harness.snaphotter.rocky.rocky_config import RockySnapshotConfig
from harness.config.env import PetSmartEnvConfig
from harness.snaphotter.snapshotter import AbstractSnapshotter
from harness.sources.source import AbstractSource
from harness.target.target import AbstractTarget


class RockySnapshotter(AbstractSnapshotter, AbstractSource, AbstractTarget):
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
        self.token = env.workspace_access_token
        self.rocky_job_id = env.rocky_job_id
        self.workspace_client = WorkspaceClient(host=self.host, token=self.token)
        self.workspace_client.jobs.run_now(job_id=self.rocky_job_id)

   
