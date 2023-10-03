from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.EnvironmentEnum import EnvironmentEnum
from harness.snaphotter.AbstractSnapshotter import AbstractSnapshotter
from harness.sources.AbstractSource import AbstractSource
from harness.target.AbstractTarget import AbstractTarget
from harness.utils.logger import getLogger


class Snapshotter(AbstractSnapshotter):
    """
    Default implementation of Snapshotter
    """

    def __init__(
        self, config: SnapshotConfig, source: AbstractSource, target: AbstractTarget
    ) -> None:
        super().__init__(config=config, source=source, target=target)
        self._logger = getLogger()

    def setupTestDataForEnv(self, env: EnvironmentEnum) -> None:
        """
            sets up the test data for the given environment.
        Args:
            env (EnvironmentEnum): environment that the data should be moved into.

        Raises:
            ValueError: Raises an exception if the version of the snapshot is not 2.
        """
        if self.config.enabled:
            if self.target.snapshot_config.version != 2: 
                raise ValueError("There Must be a version 2 of the snapshot")

            self._logger.debug(
                f"Setting up test data for snapshotter {self.config.name}"
            )
            self.target.setupDataForEnv(env)
        else:
            self._logger.debug(
                f"Skipping setupTestData for snapshotter {self.config.name}:Not Enabled"
            )

    def updateDevTargetSchema(self, schema: str):
        """
        Updates the development target schema for the Snapshotter's configuration.

        Args:
            schema (str): The new schema to set for the development target.

        Returns:
            None
        """
        self._logger.debug(f"Changing target schema for target {self.config.name}")
        self._logger.debug(f"initial value: {self.config.target.dev_target_schema}")
        self.config.target.dev_target_schema = schema
        self._logger.debug(f"new value: {self.config.target.dev_target_schema}")

    def updateDevTargetTable(self, table: str):
        """
        Update the development target table for the current Snapshotter instance.

        Args:
            table (str): The name of the new development target table.

        Returns:
            None
        """
        self._logger.debug(f"Changing target table for target {self.config.name}")
        self._logger.debug(f"initial value: {self.config.target.dev_target_schema}")
        self.config.target.dev_target_table = table
        self._logger.debug(f"new value: {self.config.target.dev_target_table}")

    def updateTestTargetSchema(self, schema: str):
        """
        Updates the test target schema for the Snapshotter's configuration.

        Args:
            schema (str): The new schema to set as the test target schema.

        Returns:
            None
        """
        self._logger.debug(f"Changing target schema for target {self.config.name}")
        self._logger.debug(f"initial value: {self.config.target.test_target_schema}")
        self.config.target.test_target_schema = schema
        self._logger.debug(f"new value: {self.config.target.test_target_schema}")

    def updateTestTargetTable(self, table: str):
        """
        Updates the test target table for the Snapshotter's configuration.

        Args:
            table (str): The name of the new test target table.

        Returns:
            None
        """
        self._logger.debug(f"Changing target table for target {self.config.name}")
        self._logger.debug(f"initial value: {self.config.target.test_target_schema}")
        self.config.target.test_target_table = table
        self._logger.debug(f"new value: {self.config.target.test_target_table}")

    def updateSnapshotSchema(self, schema: str):
        """
        Updates the snapshot schema for the Snapshotter's configuration.

        Args:
            schema (str): The new schema to set as the snapshot schema.

        Returns:
            None
        """
        self._logger.debug(f"Changing schema for snapshot {self.config.name}")
        self._logger.debug(
            f"initial value: {self.config.target.snapshot_target_schema}"
        )
        self.config.target.snapshot_target_schema = schema
        self._logger.debug(f"new value: {self.config.target.snapshot_target_schema}")

    def updateSnapshotTable(self, table: str):
        """
        Updates the snapshot table for the Snapshotter's configuration.

        Args:
            table (str): The name of the new snapshot table.

        Returns:
            None
        """
        self._logger.debug(f"Changing source table for snapshot {self.config.name}")
        self._logger.debug(f"initial value: {self.config.target.snapshot_target_table}")
        self.config.target.snapshot_target_table = table
        self._logger.debug(f"new value: {self.config.target.snapshot_target_table}")

    def disable(self):
        """
        Disables the snapshotter by setting the 'enabled' attribute of the config object to False.
        """
        self._logger.debug(f"Disabling snapshotter {self.config.name}")
        self.config.enabled = False

    def enable(self):
        """
        Enables the snapshotter by setting the 'enabled' attribute of the config object to True.
        """
        self._logger.debug(f"Enabling snapshotter {self.config.name}")
        self.config.enabled = True

    def snapshot(self):
        """
        Takes a snapshot of the source data and writes it to the target.
        """
        if self.config.enabled:
            if self.config.version < 0:
                self.config.version = 0
                self._logger.debug(
                    "invalid version number provided, setting version to 0"
                )

            if self.config.version < 1:
                self._logger.debug(f"Taking snapshot V1 of {self.config.name}...")
                self._snapshot()
            elif self.config.version == 1:
                self._logger.debug(f"Taking snapshot V2 of {self.config.name}...")
                self._snapshot()
            else:
                self._logger.debug(
                    f"V2 snapshot of {self.config.name} detected, skipping..."
                )
        else:
            self._logger.debug(
                f"Skipping snapshot for snapshotter {self.config.name}:Not Enabled"
            )

    def markAsInput(self):
        """
        Marks the snapshotter as input.
        """
        self._logger.debug(f"Marking {self.config.name} as input...")
        self.config.isInput = True

    def _snapshot(self):
        """
        Abstract method to be implemented by subclasses to take a snapshot of the data.
        """
        df = self.source.read()
        self.target.write(df)
        self.config.version += 1

    def destroy(self):
        """
        Destroys the snapshotter.
        """
        self._logger.debug(f"Destroying snapshotter {self.config.name}")
        self.target.destroy()
