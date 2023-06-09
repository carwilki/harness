from typing import Optional
from pyspark.sql import SparkSession
from harness.config.config import HarnessJobConfig


class ManagerMetaData:
    def __init__(self, session: SparkSession):
        self.session = session

    def create_metadata_table(self, schema, table):
        self.session.sql(
            f"""Create table if not exists {schema}.{table} (id int, value string)"""
        ).collect()

    def get(self, key) -> Optional[HarnessJobConfig]:
        try:
            json: str = self.session.sql(
                f"""Select value from harnnes_metadata where id == {key}"""
            ).collect()[0][0]
            if len(json) == 0:
                return None
            return HarnessJobConfig.parse_raw(json)

        except IndexError:
            return None

    def create(self, value: HarnessJobConfig):
        self.session.sql(
            f"""Insert into harness_metadata values ({value.json()})"""
        ).collect()

    def update(self, key, value: HarnessJobConfig):
        self.session.sql(
            f"""Update harness_metadata set value = {value.json()} where id == {key}"""
        ).collect()

    def delete(self, key):
        self.session.sql(
            f"""Delete from harness_metadata where id == {key}"""
        ).collect()
