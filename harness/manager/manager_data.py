from pyspark.sql import SparkSession

from harness.config.config import HarnessJobConfig


class ManagerMetaData:
    def __init__(self, session: SparkSession):
        self.session = session

    def get(self, key):
        json = self.session.sql(
            f"""Select value from harnnes_metadata where id == {key}"""
        ).collect()
        return HarnessJobConfig.parse_raw(json)

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
