import os

from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture

from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.tests.utils.generator import generate_abstract_harness_job_config


class TestManagerMetaData:
    def configenv(self, mocker: MockFixture, faker: Faker) -> tuple[str, str]:
        schema = faker.pystr()
        table = faker.pystr()
        mocker.patch.dict(os.environ, {"__HARNESS_METADATA_SCHEMA": schema})
        mocker.patch.dict(os.environ, {"__HARNESS_METADATA_TABLE": table})
        return schema, table

    def test_constructor(self, mocker: MockFixture, faker: Faker):
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        assert metadata is not None
        assert metadata.session is session

    def test_can_create_metadata_table(self, mocker: MockFixture, faker: Faker):
        schema, table = self.configenv(mocker=mocker, faker=faker)
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        metadata.create_metadata_table()
        session.sql.assert_called_with(
            f"""Create table if not exists {schema}.{table} (id string, value string)"""
        )

    def test_can_get(self, mocker: MockFixture, faker: Faker):
        schema, table = self.configenv(mocker=mocker, faker=faker)
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        metadata.get(key=1)
        session.sql.assert_called_with(
            f"""Select value from {schema}.{table} where id == '1'"""
        )

    def test_can_delete(self, mocker: MockFixture, faker: Faker):
        schema, table = self.configenv(mocker=mocker, faker=faker)
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        metadata.delete(key=1)
        session.sql.assert_called_with(
            f"""Delete from {schema}.{table} where id == '1'"""
        )

    def test_can_create(self, mocker: MockFixture, faker: Faker):
        schema, table = self.configenv(mocker=mocker, faker=faker)
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        config = generate_abstract_harness_job_config(faker=faker)
        metadata.create(value=config)
        session.sql.assert_called_with(
            f"""Insert into {schema}.{table}(id,value) values ('{config.job_id}','{config.json()}')"""
        )

    def test_can_update(self, mocker: MockFixture, faker: Faker):
        schema, table = self.configenv(mocker=mocker, faker=faker)
        session: SparkSession = mocker.MagicMock()
        metadata = HarnessJobManagerMetaData(session=session)
        config = generate_abstract_harness_job_config(faker=faker)
        metadata.update(key=config.job_id, value=config)
        session.sql.assert_called_with(
            f"""Update {schema}.{table} set value = '{config.json()}' where id == '{config.job_id}'"""
        )
