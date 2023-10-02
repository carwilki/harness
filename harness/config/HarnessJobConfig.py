from typing import Optional

from pydantic import BaseModel, validator

from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.TestRunnerConfig import TestRunnerConfig
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class HarnessJobConfig(BaseModel):
    """
    A configuration object representing a job in Harness.

    Attributes:
        job_id (str): The ID of the job.
        job_name (str): The name of the job.
        version (int, optional): The version of the job. Defaults to 0.
        testrunner (Optional[TestRunnerConfig], optional): The test runner configuration for the job. Defaults to None.
        snapshots (dict[str, SnapshotConfig | None], optional): A dictionary of snapshot configurations for the job. Defaults to {}.
        validation_reports (dict[str, DataFrameValidatorReport | None], optional): A dictionary of validation reports for the job. Defaults to {}.
    """

    job_id: str
    job_name: str
    version: int = 0
    testrunner: Optional[TestRunnerConfig] = None
    snapshots: dict[str, SnapshotConfig | None] = {}
    validation_reports: dict[str, DataFrameValidatorReport | None] = {}

    @classmethod
    @validator("job_id")
    def valid_job_id(cls, value):
        if value is None or isinstance(value, str) is False:
            raise ValueError("Job ID provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("sources")
    def valid_sources(cls, value):
        if value is None or isinstance(value, dict[str, SnapshotConfig]) is False:
            raise ValueError("sources provided is null or it's of incorrect datatype")
        return value
