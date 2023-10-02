from pydantic import BaseModel

from harness.config.ValidatorConfig import ValidatorConfig


class TestRunnerConfig(BaseModel):
    """
    Configuration for running tests.

    Attributes:
        test_job_id (str): The ID of the test job to run.
        testValidators (dict[str, ValidatorConfig]): A dictionary of validator configurations.
    """

    test_job_id: str
    testValidators: dict[str, ValidatorConfig]
