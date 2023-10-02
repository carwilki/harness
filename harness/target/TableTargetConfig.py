from typing import Optional

from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum


class TableTargetConfig(TargetConfig):
    """
    Configuration class for a table target.

    Attributes:
        snapshot_target_schema (str): The schema of the snapshot target.
        snapshot_target_table (str): The table name of the snapshot target.
        target_type (TargetTypeEnum): The type of the target.
        test_target_schema (str): The schema of the test target.
        test_target_table (str): The table name of the test target.
        dev_target_schema (Optional[str]): The schema of the dev target (optional).
        dev_target_table (Optional[str]): The table name of the dev target (optional).
        validation_filter (str): The validation filter to use.
        primary_key (list[str]): The primary key of the target.
    """

    snapshot_target_schema: str
    snapshot_target_table: str
    target_type: TargetTypeEnum = TargetTypeEnum.dbrtable
    # TODO: refactor this to be captured by a lit of envs
    test_target_schema: str
    test_target_table: str
    dev_target_schema: Optional[str]
    dev_target_table: Optional[str]

    validation_filter: str = "1=1"
    primary_key: list[str]
