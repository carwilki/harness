from pyspark.sql import SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum
from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTarget import DeltaTableTarget


class TargetFactory:
    """
    A factory class for creating instances of `AbstractTarget` subclasses based on the provided `TargetConfig`.

    :param harness_config: A `HarnessJobConfig` instance containing configuration information for the job.
    :param snapshot_config: A `SnapshotConfig` instance containing configuration information for the snapshot.
    :param target_config: A `TargetConfig` instance containing configuration information for the target.
    :param session: A `SparkSession` instance for interacting with Spark.
    :raises ValueError: If the `target_config.target_type` is not recognized.
    :return: An instance of a subclass of `AbstractTarget`.
    """

    @classmethod
    def create(
        cls,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        target_config: TargetConfig,
        session: SparkSession,
    ) -> AbstractTarget:
        """
        Creates an instance of a subclass of `AbstractTarget` based on the provided `TargetConfig`.

        :param harness_config: A `HarnessJobConfig` instance containing configuration information for the job.
        :param snapshot_config: A `SnapshotConfig` instance containing configuration information for the snapshot.
        :param target_config: A `TargetConfig` instance containing configuration information for the target.
        :param session: A `SparkSession` instance for interacting with Spark.
        :raises ValueError: If the `target_config.target_type` is not recognized.
        :return: An instance of a subclass of `AbstractTarget`.
        """
        match target_config.target_type:
            case TargetTypeEnum.dbrtable:
                return DeltaTableTarget(
                    harness_job_config=harness_config,
                    snapshot_config=snapshot_config,
                    table_config=target_config,
                    session=session,
                )
            case _:
                raise ValueError(f"Unknown target type {target_config.target_type}")
