from faker import Faker
from pyspark.sql import SparkSession
from pytest_mock import MockFixture

from harness.target.TableTarget import TableTarget
from harness.target.TableTargetConfig import TableTargetConfig


class TestTableTarget:
    def test_can_be_created(self, mocker: MockFixture, faker: Faker):
        session: SparkSession = mocker.MagicMock()

        config = TableTargetConfig(
            snapshot_target_table=faker.pystr(), snapshot_target_schema=faker.pystr()
        )

        target = TableTarget(config=config, session=session)

        assert target is not None
        assert target.config is not None
        assert target.session is not None
        assert target.config.snapshot_target_table == config.snapshot_target_table
        assert target.config.snapshot_target_schema == config.snapshot_target_schema
