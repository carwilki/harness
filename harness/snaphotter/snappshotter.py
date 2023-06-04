from abc import abstractclassmethod
from typing import Any


class Snapshotter:
    type:str
    config:dict[str,Any]
    
    @abstractclassmethod()
    def take_snapshot(version:int,config:SnapshotConfig,session:SparkSession):
        pass