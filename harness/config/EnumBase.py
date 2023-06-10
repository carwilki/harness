from enum import Enum
from typing import Any


class EnumBase(str, Enum):
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Enum):
            return self.value == other.value
        return False
