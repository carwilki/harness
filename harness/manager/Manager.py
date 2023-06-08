from harness.config.config import JobConfig
from harness.manager.manager_data import ManagerMetaData


class HarnessManager:
    """
    Harness Manager is responsible for orchestrating the snapshotting process.
    """

    def __init__(self, config: JobConfig):
        self.config = config
        self.metadata = ManagerMetaData()
    
    def snapshot(self):
        
    
    def validate():
        pass
