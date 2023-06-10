from faker import Faker
from pytest_mock import MockFixture
from pyspark.sql import SparkSession
from harness.manager.ManagerMetaData import ManagerMetaData


class TestHarnessJobManagerEnvironment:
    def test_can_create(self, mocker: MockFixture, faker: Faker):
        session: SparkSession = mocker.MagicMock()
        metadata = ManagerMetaData(session=session)
        assert metadata is not None
        assert metadata.session is session
