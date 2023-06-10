from harness.config.TargetConfig import TargetConfig


class TableTargetConfig(TargetConfig):
    target_schema: str
    target_table: str
