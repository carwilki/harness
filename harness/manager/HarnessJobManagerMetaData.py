from re import compile, match, search
from typing import Optional

from pyspark.sql import SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.utils.logger import getLogger


class HarnessJobManagerMetaData:
    """
    Class to manage the metadata table
    """

    def __init__(self, session: SparkSession):
        self.session = session
        self._harness_metadata_schema = HarnessJobManagerEnvironment.metadata_schema()
        self._harness_metadata_table = HarnessJobManagerEnvironment.metadata_table()
        self._table = f"{self._harness_metadata_schema}.{self._harness_metadata_table}"
        self._logger = getLogger()

    @classmethod
    def getJobById(cls, id: str, spark: SparkSession) -> Optional[HarnessJobConfig]:
        """
        Static Classmethod to get the job by id
        Args:
            id (str): id of the job
            spark (SparkSession): the SparkSession

        Returns:
            Optional[HarnessJobConfig]: may return None if the job is not found
        """
        manager = HarnessJobManagerMetaData(spark)
        manager = HarnessJobManagerMetaData(spark)
        return manager.get(id)

    def create_metadata_table(self):
        self.session.sql(
            f"""Create table if not exists {self._table} (id string, value string)"""
        ).collect()

    def get(self, key) -> Optional[HarnessJobConfig]:
        """
        Get job by id
        Args:
            id (str): id of the job

        Returns:
            Optional[HarnessJobConfig]: may return None if the job is not found
        """
        try:
            bin = self.session.sql(
                f"""Select value from {self._table} where id == '{key}'"""
            ).collect()[0][0]
            if len(bin) == 0:
                return None
            else:
                return HarnessJobConfig.parse_raw(bin)
        except IndexError:
            return None

    def create(self, value: HarnessJobConfig):
        """
        Creates a new job in the metadata table
        Args:
            value (HarnessJobConfig): the config for the the new job
        """ """"""
        bin = value.json().encode("utf-8")
        bin = str(bin).removeprefix("b'").removesuffix("'")
        sql = f"""Insert into {self._table} values ('{value.job_id}', '{bin}')"""
        self.session.sql(sql).collect()

    def update(self, value: HarnessJobConfig):
        """
        Updates an existing job in the metadata table
        Args:
            value (HarnessJobConfig): the config for the the new job
        """
        bin = value.json().encode("utf-8")
        bin = str(bin).removeprefix("b'").removesuffix("'")
        sql = (
            f"""update {self._table} set value = '{bin}' where id == '{value.job_id}'"""
        )
        self.session.sql(sql).collect()

    def delete(self, key):
        """
        Deletes an existing job in the metadata table
        Args:
            key (str): id of the job
        """
        self._logger.debug(f"Deleting job {key}")
        self.session.sql(f"""Delete from {self._table} where id == '{key}'""").collect()
        self._logger.debug(f"Deleted job {key}")

    def resetEverything(self, dry_run: bool = True) -> str:
        """
        Resets the entire metadata table
        Args:
            dry_run (bool, optional): if True, don't actually delete the tables. Defaults to True.

        Returns:
            str: a string containing the SQL commands to execute
        """

        self._logger.debug("Generating Dry Run, not deleting tables\n")
        tables = self.session.catalog.listTables(self._harness_metadata_schema)

        if dry_run:
            msg = "Executing Dry Run, not deleting tables\n"
        else:
            msg = "Deleting tables:\n"

        for table in tables:
            msg += f"""Drop table if exists {self._harness_metadata_schema}.{table.name};\n"""

        return msg

    def cleanupValidationReports(self, job_name: str, dry_run: bool = True) -> str:
        """
        Cleans up the validation reports for a job
        Args:
            job_name (str): the name of the job
            dry_run (bool, optional): if True, don't actually delete the tables. Defaults to True.

        Returns:
            str: a string containing the SQL commands to execute
        """
        msg = ""
        self._logger.debug(f"Cleaning up validation reports for {job_name}")
        tables = self.session.catalog.listTables(f"{self._harness_metadata_schema}")
        for table in tables:
            if match(rf"(?:{job_name.lower()})_*\w*_validation_report_", table.name):
                self._logger.debug(f"Cleaning up {table.name}")
                msg += f"""Drop table if exists {self._harness_metadata_schema}.{table.name};\n"""
            else:
                self._logger.debug(f"Skipping cleanup of {table.name}")
        return msg
