import abc

from pydantic import BaseModel, validator

from harness.config.SourceTypeEnum import SourceTypeEnum


class SourceConfig(BaseModel, abc.ABC):
    source_type: SourceTypeEnum

    @classmethod
    @validator("source_type")
    def valid_source(cls, value):
        if value is None or isinstance(value, SourceTypeEnum) == False:
            raise ValueError(
                "source_type provided is null or it's of incorrect datatype"
            )
        return value
    
    @classmethod
    def parse_obj(cls, obj):
        return cls._convert_to_real_type_(obj)
