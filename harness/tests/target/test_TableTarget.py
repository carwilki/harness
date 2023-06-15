from faker import Faker
from pytest_mock import MockFixture
from harness.target.TableTarget import TableTarget
from pyspark.sql import SparkSession
from harness.target.TableTargetConfig import TableTargetConfig


class TestTableTarget:
    def test_can_be_created(self, mocker: MockFixture, faker: Faker):
        session: SparkSession = mocker.MagicMock()

        config = TableTargetConfig(
            target_table=faker.pystr(), target_schema=faker.pystr()
        )

        target = TableTarget(config=config, session=session)
        
        assert target is not None
        assert target.config is not None
        assert target.session is not None
        assert target.config.target_table == config.target_table
        assert target.config.target_schema == config.target_schema