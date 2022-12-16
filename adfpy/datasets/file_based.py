import abc
from pathlib import Path

from azure.mgmt.datafactory.models import BinaryDataset, LinkedServiceReference, AzureBlobStorageLocation, DatasetResource

from adfpy.datasets.main import AdfDataset


class FileStorageType:
    # for each storage type a custom class?
    # Http is quite different from azure blob / s3. Might need to differentiate here.
    # There is overlap between many of these, but also some differentiation. Probably can do something with kwargs
    # to get this to work
    pass


class AdfFileBasedDataset(AdfDataset):
    def __init__(self, name: str, linked_service_name: str, storage_type: str):
        super(AdfFileBasedDataset, self).__init__(name, linked_service_name)
        self.storage_type = storage_type

    @abc.abstractmethod
    def to_adf(self):
        pass


class AdfAvroDataset(AdfFileBasedDataset):
    pass


class AdfBinaryDataset(AdfFileBasedDataset):
    def __init__(self, name: str, linked_service_name: str, storage_type: str, container_name: str, path: str):
        super(AdfBinaryDataset, self).__init__(name, linked_service_name, storage_type)
        self.container_name = container_name
        self.path = path  #TODO: need to ensure the first char is not a `/`

    def to_adf(self):
        parsed_path = Path(self.path)
        return DatasetResource(properties=BinaryDataset(linked_service_name=LinkedServiceReference(reference_name=self.linked_service_name),
                               location=AzureBlobStorageLocation(
                                   container=self.container_name,
                                   folder_path=parsed_path.parent if str(parsed_path.parent) != "." else None,
                                   file_name=parsed_path.name)
                             ))


class AdfJsonDataset(AdfFileBasedDataset):
    pass


class AdfDelimitedTextDataset(AdfFileBasedDataset):
    pass


class AdfOrcDataset(AdfFileBasedDataset):
    pass


class AdfParquetDataset(AdfFileBasedDataset):
    pass


class AdfXmlDataset(AdfFileBasedDataset):
    pass


class AdfExcelDataset(AdfFileBasedDataset):
    pass
