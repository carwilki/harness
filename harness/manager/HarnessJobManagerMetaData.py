from typing import Optional
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

    def update(self, key, value: HarnessJobConfig):
        bin = value.json().encode("utf-8")
        bin = str(bin).removeprefix("b'").removesuffix("'")
        sql = f"""update {self._table} set value = '{bin}' where id == '{value.job_id}'"""
        self.session.sql(sql).collect()
        
    def delete(self, key):
        self.session.sql(f"""Delete from {self._table} where id == '{key}'""").collect()
