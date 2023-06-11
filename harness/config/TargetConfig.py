import abc
from typing import Optional

from pydantic import BaseModel

from harness.config.TargetTypeEnum import TargetTypeEnum
from harness.config.ValidatorTypeEnum import ValidatorTypeEnum


class TargetConfig(BaseModel, abc.ABC):
    target_type: TargetTypeEnum
