from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum


class TableTargetConfig(TargetConfig):
    snapshot_target_schema: str
    snapshot_target_table: str
    target_type: TargetTypeEnum = TargetTypeEnum.dbrtable
    test_target_schema: str
    test_target_table: str
