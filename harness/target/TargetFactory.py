from pyspark.sql import SparkSession

from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum
from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTarget import TableTarget


class TargetFactory:
    @classmethod
    def create(
        cls, target_config: TargetConfig, session: SparkSession
    ) -> AbstractTarget:
        match target_config.target_type:
            case TargetTypeEnum.dbrtable:
                return TableTarget(config=target_config, session=session)
            case _:
                raise ValueError(f"Unknown target type {target_config.target_type}")
