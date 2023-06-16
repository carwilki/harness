from faker import Faker
from pytest_mock import MockFixture
from harness.sources.JDBCSourceConfig import JDBCSourceConfig
from harness.sources.SourceFactory import SourceFactory


class TestSourceFactory:
    def test_can_create_source_from_jdbc_config(self, mocker: MockFixture, faker: Faker):
        session = mocker.MagicMock()
        sc = JDBCSourceConfig(source_table="E_CONSOL_PERF_SMRY", source_schema="WMSMIS")
        SourceFactory.create(source_config=sc, session=session)
