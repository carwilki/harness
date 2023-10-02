from harness.config.EnumBase import EnumBase


class ValidatorTypeEnum(EnumBase):
    """
    Enum class representing the types of validators available in the system.
    """
    raptor = "raptor"
    dataframe = "dataframe"
