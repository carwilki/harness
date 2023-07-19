from faker import Faker
import pytest
from pytest_mock import MockFixture
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment

from harness.tests.utils.generator import (
    generate_env_config,
    generate_jdbc_source_config,
    generate_standard_harness_job_config,
    generate_standard_snapshot_config,
)

