from harness.snaphotter.snapshotter import Snapshotter
from harness.tests.utils.generator import generate_snapshot_config

from pyspark.sql import SparkSession


class TestSnapshotter:
    def test_config(self):
        config = generate_snapshot_config()
        snapshotter = Snapshotter(config=config)
        assert snapshotter.config == config

    def test_take_snapshot(self, mocker):
        session: SparkSession = mocker.MagicMock()
        config = generate_snapshot_config()
        snapshotter = Snapshotter(config=config)
        snapshotter.take_snapshot(version="1", session=session)
