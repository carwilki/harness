from harness.config.EnumBase import EnumBase


class EnvironmentEnum(EnumBase):
    """
    Enum representing the different environments in which the application can execute tests.
    """

    DEV = "dev"
    QA = "qa"
    PROD = "prod"
