from enum import Enum
from typing import Any


class EnumBase(str, Enum):
    """
    Base class for all enumeration classes in the Harness configuration.
    """
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Enum):
            return self.value == other.value
        return False
