from logging import getLogger

from harness.config.SnapshotConfig import SnapshotConfig
from harness.snaphotter.AbstractSnapshotter import AbstractSnapshotter
from harness.sources.AbstractSource import AbstractSource
from harness.target.AbstractTarget import AbstractTarget
from harness.validator.DataFrameValidator import DataFrameValidator


class Snapshotter(AbstractSnapshotter):
    def __init__(
        self, config: SnapshotConfig, source: AbstractSource, target: AbstractTarget
    ) -> None:
        super().__init__(config=config, source=source, target=target)
        self._logger = getLogger()
        if self.config.validator is not None:
            self._validator = DataFrameValidator(self.config.validator)

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
            self._validator.validate(df, self.source.read, self.source.session)

        self.config.version += 1
