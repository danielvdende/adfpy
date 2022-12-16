import abc


class AdfDataset:
    def __init__(self, name: str, linked_service_name:str):
        self.name = name
        self.linked_service_name = linked_service_name

    @abc.abstractmethod
    def to_adf(self):
        pass
