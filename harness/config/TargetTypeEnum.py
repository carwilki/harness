from harness.config.EnumBase import EnumBase


class TargetTypeEnum(EnumBase):
    """
    An enumeration of the different types of targets that can be used in Harness.

    Attributes:
        dbrtable (str): A target type that represents a database table.
    """
    dbrtable = "dbrtable"
