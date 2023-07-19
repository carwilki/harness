from faker import Faker
import pytest
from pyspark.sql import SparkSession
from pytest_mock import MockFixture
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.target.TableTargetConfig import TableTargetConfig
from harness.tests.utils.generator import (
    generate_env_config,
    generate_jdbc_source_config,
    generate_standard_harness_job_config,
    generate_standard_snapshot_config,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def bindenv(mocker: MockFixture, faker: Faker):
    envconfig = generate_env_config(faker)
    HarnessJobManagerEnvironment.bindenv(envconfig)
    return envconfig


@pytest.fixture(scope="function")
def snapshotConfig(mocker: MockFixture, faker: Faker):
    return generate_standard_snapshot_config(0, faker)


@pytest.fixture(scope="function")
def harnessConfig(mocker: MockFixture, faker: Faker):
    return generate_standard_harness_job_config(0, faker)


@pytest.fixture(scope="function")
def jdbcSourceConfig(mocker: MockFixture, faker: Faker):
    return generate_jdbc_source_config(faker)


@pytest.fixture(scope="function")
def tableTargetConfig(mocker: MockFixture, faker: Faker):
    return TableTargetConfig(
        snapshot_target_table=faker.pystr(),
        snapshot_target_schema=faker.pystr(),
        test_target_schema=faker.pystr(),
        test_target_table=faker.pystr(),
        primary_key=["test_target","test_target_2"]
    )
