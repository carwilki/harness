import abc

from pydantic import BaseModel

from harness.config.SourceTypeEnum import SourceTypeEnum


class SourceConfig(BaseModel, abc.ABC):
    source_type: SourceTypeEnum
