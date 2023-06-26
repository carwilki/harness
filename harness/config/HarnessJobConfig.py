from typing import Optional

from pydantic import BaseModel, validator

from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.TestRunnerConfig import TestRunnerConfig


class HarnessJobConfig(BaseModel):
    job_id: str
    version: int = 0
    testrunner: Optional[TestRunnerConfig] = None
    snapshot_name: Optional[str] = None
    sources: dict[str, SnapshotConfig]
    inputs: dict[str, SnapshotConfig]

    @classmethod
    @validator("job_id")
    def valid_job_id(cls, value):
        if value is None or isinstance(value, str) == False:
            raise ValueError("Job ID provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("sources")
    def valid_sources(cls, value):
        if value is None or isinstance(value, dict[str, SnapshotConfig]) == False:
            raise ValueError("sources provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("inputs")
    def valid_inputs(cls, value):
        if value is None or isinstance(value, dict[str, SnapshotConfig]) == False:
            raise ValueError("inputs provided is null or it's of incorrect datatype")
        return value
