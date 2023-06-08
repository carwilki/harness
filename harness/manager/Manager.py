from harness.config.config import HarnessJobConfig
from harness.manager.manager_data import ManagerMetaData


class HarnessJobManager:
    """
    Harness Manager is responsible for orchestrating the snapshotting process.
    """

    def __init__(self, config: HarnessJobConfig):
        self.config = config
        self.metadata = ManagerMetaData()

    def snapshot(self):
        pass

    def validate():
        pass
