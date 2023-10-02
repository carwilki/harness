from typing import Optional

from harness.config.EnvConfig import EnvConfig


class HarnessJobManagerEnvironment(object):
    """
    A class representing the environment configuration for the Harness Job Manager.

    Attributes:
        _config (Optional[EnvConfig]): The environment configuration object.
    """

    _config: Optional[EnvConfig] = None

    @classmethod
    def bindenv(cls, config: EnvConfig):
        """
        Binds the environment configuration object to the class.

        Args:
            config (EnvConfig): The environment configuration object.
        """
        cls._config = config

    @classmethod
    def workspace_url(cls) -> Optional[str]:
        """
        Returns the workspace URL from the environment configuration.

        Returns:
            Optional[str]: The workspace URL.
        """
        return cls._config.workspace_url

    @classmethod
    def metadata_schema(cls) -> Optional[str]:
        """
        Returns the metadata schema from the environment configuration.

        Returns:
            Optional[str]: The metadata schema.
        """
        return cls._config.metadata_schema

    @classmethod
    def metadata_table(cls) -> Optional[str]:
        """
        Returns the metadata table from the environment configuration.

        Returns:
            Optional[str]: The metadata table.
        """
        return cls._config.metadata_table

    @classmethod
    def snapshot_schema(cls) -> Optional[str]:
        """
        Returns the snapshot schema from the environment configuration.

        Returns:
            Optional[str]: The snapshot schema.
        """
        return cls._config.snapshot_schema

    @classmethod
    def getConfig(cls) -> dict:
        """
        Returns the environment configuration as a dictionary.

        Returns:
            dict: The environment configuration.
        """
        return dict(cls._config)

    @classmethod
    def get_config(cls) -> EnvConfig:
        """
        Returns the environment configuration object.

        Returns:
            EnvConfig: The environment configuration object.
        """
        return cls._config