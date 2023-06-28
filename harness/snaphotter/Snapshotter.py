from datetime import datetime

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
            self._logger.info(
                f"No validator configured for snapshotter {self.config.name}"
            )

    def setupTestData(self):
        if self.target.snapshot_config.version != 2:
            raise ValueError("There Must be a version 2 of the snapshot")

        self._logger.info(f"Setting up test data for snapshotter {self.config.name}")
        self.target.setup_test_target()

    def validateResults(self) -> DataFrameValidatorReport:
        return self.target.validate_results()

    def snapshot(self):
        if self.config.version < 0:
            self.config.version = 0
            self._logger.info("invalid version number provided, setting version to 0")

        if self.config.version < 1:
            self._logger.info(f"Taking snapshot V1 of {self.config.name}...")
            self._snapshot()
        elif self.config.version == 1:
            self._logger.info(f"Taking snapshot V2 of {self.config.name}...")
            self._snapshot()
        else:
            self._logger.info(
                f"V2 snapshot of {self.config.name} detected, skipping..."
            )

    def _snapshot(self):
        df = self.source.read()
        self.target.write(df)
        if self.config.validator is not None:
            date = datetime.now().strftime("%Y-%m-%d %H:%M")
            report = self._validator.validateDF(
                name=self.config.name,
                master=self.source.read(),
                canidate=df,
                session=self.source.session,
            )
            if self.config.validator.validator_reports is None:
                self.config.validator.validator_reports = {}

            self.config.validator.validator_reports[date] = report

        self.config.version += 1
