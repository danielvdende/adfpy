from enum import Enum

class SupportedFileTypes(Enum):
    Avro = "Avro"

class AdfFileBasedDataset:
    def __init__(self, linked_service_name: str, type: ):
        self.linked_service_name = linked_service_name

class AdfAvroDataset(AdfFileBasedDataset):
    pass
class AdfBinaryDataset(AdfFileBasedDataset):
    pass
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
