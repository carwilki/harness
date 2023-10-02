from harness.config.EnumBase import EnumBase


class SourceTypeEnum(EnumBase):
    """
    Enum class representing the different types of data sources that can be used in the application.
    """
    
    netezza_jdbc = "netezza_jdbc"
    databricks_jdbc = "databricks_jdbc"
    dbrx = "dbrx"
