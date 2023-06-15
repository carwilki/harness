from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum


class TableTargetConfig(TargetConfig):
    target_schema: str
    target_table: str
    target_type: TargetTypeEnum = TargetTypeEnum.dbrtable
