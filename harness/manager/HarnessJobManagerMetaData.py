from typing import Optional
from json import dumps, loads
from pyspark.sql import SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment


class HarnessJobManagerMetaData:
    def __init__(self, session: SparkSession):
        self.session = session
        self._harness_metadata_schema = HarnessJobManagerEnvironment.metadata_schema()
        self._harness_metadata_table = HarnessJobManagerEnvironment.metadata_table()
        self._table = f"{self._harness_metadata_schema}.{self._harness_metadata_table}"

    def create_metadata_table(self):
        self.session.sql(
            f"""Create table if not exists {self._table} (id string, value string)"""
        ).collect()

    def get(self, key) -> Optional[HarnessJobConfig]:
        self.session.conf.set("spark.sql.parser.escapedStringLiterals", "false")
        try:
            json: str = self.session.sql(
                f"""Select value from {self._table} where id == '{key}'"""
            ).collect()[0][0]
            if len(json) == 0:
                return None
            else:
                return HarnessJobConfig.parse_raw(loads(json))

        except IndexError:
            return None

    def create(self, value: HarnessJobConfig):
        self.session.conf.set("spark.sql.parser.escapedStringLiterals", "false")
        self.session.sql(
            f"""Insert into {self._table}(id,value) values ('{value.job_id}','{value.json()}'"""
        ).collect()

    def update(self, key, value: HarnessJobConfig):
        self.session.sql(
            f"""Update {self._table} set value = '{value.json()}' where id == '{key}'"""
        ).collect()

    def delete(self, key):
        self.session.sql(f"""Delete from {self._table} where id == '{key}'""").collect()
