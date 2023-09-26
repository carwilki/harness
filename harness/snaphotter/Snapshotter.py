from harness.config.SnapshotConfig import SnapshotConfig
from harness.snaphotter.AbstractSnapshotter import AbstractSnapshotter
from harness.sources.AbstractSource import AbstractSource
from harness.target.AbstractTarget import AbstractTarget
from harness.utils.logger import getLogger
from harness.validator.DataFrameValidator import DataFrameValidator
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class Snapshotter(AbstractSnapshotter):
    def __init__(
        self, config: SnapshotConfig, source: AbstractSource, target: AbstractTarget
    ) -> None:
        super().__init__(config=config, source=source, target=target)

        self._logger = getLogger()
        if self.config.validator is not None:
            self._validator = DataFrameValidator(self.config.validator)
        else:
            self._validator = None
            self._logger.debug(
                f"No validator configured for snapshotter {self.config.name}"
            )

    def setupTestData(self):
        if self.config.enabled:
            if self.target.snapshot_config.version != 2:
                raise ValueError("There Must be a version 2 of the snapshot")

            self._logger.debug(
                f"Setting up test data for snapshotter {self.config.name}"
            )
            self.target.setup_test_target()
        else:
            self._logger.debug(
                f"Skipping setupTestData for snapshotter {self.config.name}:Not Enabled"
            )

    def validateResults(self) -> DataFrameValidatorReport | None:
        if self.config.enabled and not self.config.isInput:
            return self.target.validate_results()

        self._logger.debug(
            f"Skipping validation for snapshotter {self.config.name}: Enabled:{self.config.enabled}, isInput:{self.config.isInput}"
        )
        return None

    def updateValidaitonFilter(self, filter: str):
        self._logger.debug(
            f"Changing validation filter for snapshotter {self.config.name}"
        )
        self._logger.debug(f"initial value: {self.config.target.validation_filter}")
        self.config.target.validation_filter = filter
        self._logger.debug(f"new value: {self.config.target.validation_filter}")

    def updateTargetSchema(self, schema: str):
        self._logger.debug(f"Changing target schema for target {self.config.name}")
        self._logger.debug(f"initial value: {self.config.target.test_target_schema}")
        self.config.target.test_target_schema = schema
        self._logger.debug(f"new value: {self.config.target.test_target_schema}")

    def updateTargetTable(self, table: str):
        self._logger.debug(f"Changing target table for target {self.config.name}")
        self._logger.debug(f"initial value: {self.config.target.test_target_schema}")
        self.config.target.test_target_table = table
        self._logger.debug(f"new value: {self.config.target.test_target_table}")

    def updateSnapshotSchema(self, schema: str):
        self._logger.debug(f"Changing source schema for snapshot {self.config.name}")
        self._logger.debug(
            f"initial value: {self.config.target.snapshot_target_schema}"
        )
        self.config.target.snapshot_target_schema = schema
        self._logger.debug(f"new value: {self.config.target.snapshot_target_schema}")

    def updateSnapshotTable(self, table: str):
        self._logger.debug(f"Changing source table for snapshot {self.config.name}")
        self._logger.debug(f"initial value: {self.config.target.snapshot_target_table}")
        self.config.target.snapshot_target_table = table
        self._logger.debug(f"new value: {self.config.target.snapshot_target_table}")

    def disable(self):
        self._logger.debug(f"Disabling snapshotter {self.config.name}")
        self.config.enabled = False

    def enable(self):
        self._logger.debug(f"Enabling snapshotter {self.config.name}")
        self.config.enabled = True

    def snapshot(self):
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
        self._logger.debug(f"Marking {self.config.name} as input...")
        self.config.isInput = True

    def _snapshot(self):
        df = self.source.read()
        self.target.write(df)
        self.config.version += 1

    def destroy(self):
        self._logger.debug(f"Destroying snapshotter {self.config.name}")
        self.target.destroy()
