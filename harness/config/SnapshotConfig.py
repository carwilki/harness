from datetime import datetime
from typing import Optional

from pydantic import BaseModel, validator

from harness.sources.SourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig


class SnapshotConfig(BaseModel):
    """
    Snapshot configuration model.

    :param name: Name of the snapshot.
    :type name: str, optional
    :param job_id: ID of the job.
    :type job_id: str, optional
    :param target: Target configuration.
    :type target: TableTargetConfig, optional
    :param source: Source configuration.
    :type source: JDBCSourceConfig, optional
    :param version: Version of the snapshot.
    :type version: int, optional
    :param isInput: Whether the snapshot is an input.
    :type isInput: bool, optional
    :param validated: Whether the snapshot is validated.
    :type validated: bool, optional
    :param validation_date: Date of validation.
    :type validation_date: datetime, optional
    :param enabled: Whether the snapshot is enabled.
    :type enabled: bool, optional
    :param snapshot_report: Snapshot report.
    :type snapshot_report: str, optional
    """

    name: Optional[str] = None
    job_id: Optional[str] = None
    target: TableTargetConfig = None
    source: JDBCSourceConfig = None
    version: int = 0
    # TODO: this is PetsMart specific implementation. Should be refactored to be generalized
    # and replaces with an enum for multiple types of tables.
    isInput: bool | None = False
    # TODO: this should be removed and put into a validation manager
    validated: bool = False
    validation_date: Optional[datetime] = None
    enabled: bool = True
    # TODO: this should be removed and put into a validation manager
    snapshot_report: Optional[str] = None

    @classmethod
    @validator("target")
    def valid_target(cls, value):
        """
        Validates the target configuration.

        :param value: Target configuration.
        :type value: TableTargetConfig
        :raises ValueError: If target is null or of incorrect datatype.
        :return: Target configuration.
        :rtype: TableTargetConfig
        """
        if value is None or isinstance(value, TableTargetConfig) is False:
            raise ValueError("target provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("source")
    def valid_source(cls, value):
        """
        Validates the source configuration.

        :param value: Source configuration.
        :type value: JDBCSourceConfig
        :raises ValueError: If source is null or of incorrect datatype.
        :return: Source configuration.
        :rtype: JDBCSourceConfig
        """
        if value is None or isinstance(value, JDBCSourceConfig) is False:
            raise ValueError("source provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("validated")
    def valid_validated(cls, value):
        """
        Validates the validated flag.

        :param value: Validated flag.
        :type value: bool
        :raises ValueError: If validated is null or of incorrect datatype.
        :return: Validated flag.
        :rtype: bool
        """
        if value is None or isinstance(value, bool) is False:
            raise ValueError("validated provided is null or it's of incorrect datatype")
        return value
