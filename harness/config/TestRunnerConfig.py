from pydantic import BaseModel

from harness.config.ValidatorConfig import ValidatorConfig


class TestRunnerConfig(BaseModel):
    test_job_id: str
    testValidators: dict[str, ValidatorConfig]
