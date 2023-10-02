import abc
from typing import Optional

from pydantic import BaseModel, validator

from harness.config.SourceTypeEnum import SourceTypeEnum


class SourceConfig(BaseModel, abc.ABC):
    """
    Represents a configuration for a source.

    Attributes:
        source_filter (Optional[str]): A filter to apply to the source.
        source_type (SourceTypeEnum): The type of the source.
    """

    source_filter: Optional[str]
    source_type: SourceTypeEnum

    @classmethod
    @validator("source_type")
    def valid_source(cls, value):
        """
        Validates that the source type is not null and is of the correct datatype.

        Args:
            value: The value to validate.

        Returns:
            The validated value.

        Raises:
            ValueError: If the source type is null or of incorrect datatype.
        """
        if value is None or isinstance(value, SourceTypeEnum) is False:
            raise ValueError(
                "source_type provided is null or it's of incorrect datatype"
            )
        return value

    @classmethod
    def parse_obj(cls, obj):
        """
        Parses the object and returns an instance of the class.

        Args:
            obj: The object to parse.

        Returns:
            An instance of the class.
        """
        return cls._convert_to_real_type_(obj)
