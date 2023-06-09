from harness.config.config import TargetConfig, TargetTypeEnum
from harness.target.databricks import TableTarget
from harness.target.target import AbstractTarget


class TargetFactory:
    @classmethod
    def create(cls, target_config: TargetConfig) -> AbstractTarget:
        match target_config.target_type:
            case TargetTypeEnum.dbrtable:
                return TableTarget(config=target_config)
            case _:
                raise ValueError(f"Unknown target type {target_config.target_type}")
