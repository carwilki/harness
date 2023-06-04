from abc import abstractmethod

from validator.ValidatorConfig import ValidatorConfig

class Validatator:
    def __init__(self, config:ValidatorConfig):
        self.type = config.type
        self.config = config.config

    @abstractmethod()
    def validate(self):
        pass