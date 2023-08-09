from harness.config.EnumBase import EnumBase


class SourceTypeEnum(EnumBase):
    netezza_jdbc = "netezza_jdbc"
    databricks_jdbc = "databricks_jdbc"
    dbrx = "dbrx"
