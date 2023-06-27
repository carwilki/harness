from pyspark.sql import SparkSession
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig

from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum
from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTarget import TableTarget


class TargetFactory:
    @classmethod
    def create(
        cls,
        harness_job_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        target_config: TargetConfig,
        session: SparkSession,
    ) -> AbstractTarget:
        match target_config.target_type:
            case TargetTypeEnum.dbrtable:
                return TableTarget(
                    harness_job_config=harness_job_config,
                    snapshot_config=snapshot_config,
                    config=target_config,
                    session=session,
                )
            case _:
                raise ValueError(f"Unknown target type {target_config.target_type}")
