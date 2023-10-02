from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import validator as pydantic_validator

from harness.config.SourceConfig import SourceConfig
from harness.config.TargetConfig import TargetConfig
from harness.config.ValidatorConfig import ValidatorConfig
from harness.sources.SourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig


from datetime import datetime
from typing import Optional
from pydantic import BaseModel, validator
from harness.config.TargetConfig import TableTargetConfig
from harness.config.SourceConfig import JDBCSourceConfig
from harness.config.ValidatorConfig import ValidatorConfig


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
    :param validator: Validator configuration.
    :type validator: ValidatorConfig, optional
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
    isInput: bool | None = False
    validator: Optional[ValidatorConfig] = None
    validated: bool = False
    validation_date: Optional[datetime] = None
    enabled: bool = True
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
