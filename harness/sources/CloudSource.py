from pyspark.sql import DataFrame, SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.sources.AbstractSource import AbstractSource
from harness.sources.SourceConfig import DatabricksTableSourceConfig


class DatabricksTableSource(AbstractSource):
    """
    A class representing a source of data from a Databricks table.

    Args:
        harness_config (HarnessJobConfig): The configuration for the Harness job.
        snapshot_config (SnapshotConfig): The configuration for the snapshot.
        config (DatabricksTableSourceConfig): The configuration for the Databricks table source.
        session (SparkSession): The Spark session to use for reading data.

    Attributes:
        config (DatabricksTableSourceConfig): The configuration for the Databricks table source.

    Methods:
        read(): Reads data from the Databricks table source and returns a DataFrame.
    """

    def __init__(
        self,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        config: DatabricksTableSourceConfig,
        session: SparkSession,
    ):
        super().__init__(
            harness_config=harness_config,
            snapshot_config=snapshot_config,
            session=session,
        )
        self.config: DatabricksTableSourceConfig = config

    def read(self) -> DataFrame:
        """
        Reads data from the Databricks table source and returns a DataFrame.

        Returns:
            DataFrame: The DataFrame containing the data from the Databricks table source.
        """
        if self.config.source_catalog is None:
            SQL = f"""Select * from {self.config.source_schema}.{self.config.source_table}"""
        else:
            SQL = f"""Select * from {self.config.source_catalog}.{self.config.source_schema}.{self.config.source_table}"""

        if self.config.source_filter is not None:
            SQL = SQL + f""" WHERE {self.config.source_filter}"""

        df = self.session.sql(SQL)

        return df.repartition(50)
