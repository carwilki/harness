import abc

from pydantic import BaseModel, validator

from harness.config.TargetTypeEnum import TargetTypeEnum


class TargetConfig(BaseModel, abc.ABC):
    """
    Base class for all target configurations.
    """

    target_type: TargetTypeEnum

    @classmethod
    @validator("target_type")
    def valid_source(cls, value):
        """
        Validates that the target_type provided is not None and is of the correct datatype.

        Args:
            value (Any): The value to be validated.

        Raises:
            ValueError: If the target_type provided is None or is of incorrect datatype.

        Returns:
            Any: The validated value.
        """
        if value is None or isinstance(value, TargetTypeEnum) is False:
            raise ValueError(
                "target_type provided is null or it's of incorrect datatype"
            )
        return value
