from typing import Optional

from pyspark.sql import SparkSession
from re import search, compile, match
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.utils.logger import getLogger


class HarnessJobManagerMetaData:
    def __init__(self, session: SparkSession):
        self.session = session
        self._harness_metadata_schema = HarnessJobManagerEnvironment.metadata_schema()
        self._harness_metadata_table = HarnessJobManagerEnvironment.metadata_table()
        self._table = f"{self._harness_metadata_schema}.{self._harness_metadata_table}"
        self._logger = getLogger()

    @classmethod
    def getJobById(cls, id: str, spark: SparkSession) -> Optional[HarnessJobConfig]:
        manager = HarnessJobManagerMetaData(spark)
        return manager.get(id)

    def create_metadata_table(self):
        self.session.sql(
            f"""Create table if not exists {self._table} (id string, value string)"""
        ).collect()

    def get(self, key) -> Optional[HarnessJobConfig]:
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
        bin = value.json().encode("utf-8")
        bin = str(bin).removeprefix("b'").removesuffix("'")
        sql = f"""Insert into {self._table} values ('{value.job_id}', '{bin}')"""
        self.session.sql(sql).collect()

    def update(self, value: HarnessJobConfig):
        bin = value.json().encode("utf-8")
        bin = str(bin).removeprefix("b'").removesuffix("'")
        sql = (
            f"""update {self._table} set value = '{bin}' where id == '{value.job_id}'"""
        )
        self.session.sql(sql).collect()

    def delete(self, key):
        self.session.sql(f"""Delete from {self._table} where id == '{key}'""").collect()

    def resetEverything(self, dry_run: bool = True) -> str:
        tables = self.session.catalog.listTables(self._harness_metadata_schema)
        if dry_run:
            msg = "Executing Dry Run, not deleting tables\n"
        else:
            msg = "Deleting tables:\n"

        for table in tables:
            msg += f"""Drop table if exists {self._harness_metadata_schema}.{table.name};\n"""

        return msg

    def cleanupValidationReports(self, job_name: str, dry_run: bool = True) -> str:
        self._logger.debug(f"Cleaning up validation reports for {job_name}")
        msg = "Executing Dry Run, not deleting tables\n"
        tables = self.session.catalog.listTables(f"{self._harness_metadata_schema}")
        for table in tables:
            if match(rf"(?:{job_name.lower()})_*\w*_validation_report_", table.name):
                self._logger.debug(f"Cleaning up {table.name}")
                msg += f"""Drop table if exists {self._harness_metadata_schema}.{table.name};\n"""
            else:
                self._logger.debug(f"Skipping cleanup of {table.name}")
        return msg
