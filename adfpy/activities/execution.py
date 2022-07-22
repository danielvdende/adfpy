from typing import Any, List, Optional

from azure.mgmt.datafactory.models import (  # type: ignore
    ActivityDependency,
    AzureBlobStorageReadSettings,
    CopySource,
    CopySink,
    CopyActivity,
    DatabricksSparkPythonActivity,
    DatasetReference,
    DeleteActivity,
    LinkedServiceReference,
    LookupActivity,
    SqlServerStoredProcedureActivity,
)

from adfpy.activity import AdfActivity
from adfpy.pipeline import AdfPipeline


class AdfCopyActivity(AdfActivity):
    def __init__(
            self,
            name: str,
            input_dataset_name: str,
            output_dataset_name: str,
            source_type: CopySource,
            sink_type: CopySink,
            pipeline: AdfPipeline = None
    ):
        super(AdfCopyActivity, self).__init__(name, pipeline)
        self.input_dataset = DatasetReference(reference_name=input_dataset_name)
        self.output_dataset = DatasetReference(reference_name=output_dataset_name)
        self.source_type = source_type
        self.sink_type = sink_type

    def to_adf(self) -> CopyActivity:
        return CopyActivity(
            name=self.name,
            inputs=[self.input_dataset],
            outputs=[self.output_dataset],
            source=self.source_type,
            sink=self.sink_type,
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfDeleteActivity(AdfActivity):
    def __init__(
        self, name: str, dataset_name: str, recursive: bool = False, wildcard: str = None, pipeline: AdfPipeline = None
    ):
        super(AdfDeleteActivity, self).__init__(name, pipeline)
        # TODO: this is an arbitrary subselection of options. We should bring this in line with what the SDK exposes.
        # this can be achieved in several ways (e.g. getting all parameters in here, using a dict, re-using the ADF SDK
        # objects, etc.)
        self.dataset_name = dataset_name
        self.recursive = recursive
        self.wildcard = wildcard

    def to_adf(self) -> DeleteActivity:
        return DeleteActivity(
            name=self.name,
            dataset=DatasetReference(reference_name=self.dataset_name),
            store_settings=AzureBlobStorageReadSettings(
                wildcard_file_name=self.wildcard, recursive=self.recursive
            ),
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfDatabricksSparkPythonActivity(AdfActivity):
    def __init__(self, name: str,
                 python_file: str,
                 parameters: Optional[List[Any]] = None,
                 pipeline: AdfPipeline = None):
        super(AdfDatabricksSparkPythonActivity, self).__init__(name, pipeline)
        self.python_file = python_file
        self.parameters = parameters

    def to_adf(self) -> DatabricksSparkPythonActivity:
        return DatabricksSparkPythonActivity(
            name=self.name,
            python_file=self.python_file,
            parameters=self.parameters,
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfLookupActivity(AdfActivity):
    def __init__(self, name: str, dataset: str, source: CopySource, pipeline: AdfPipeline = None):
        super(AdfLookupActivity, self).__init__(name, pipeline)
        self.dataset = DatasetReference(reference_name=dataset)
        self.source = source

    def to_adf(self) -> LookupActivity:
        return LookupActivity(
            name=self.name,
            dataset=self.dataset,
            source=self.source,
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfSqlServerStoredProcedureActivity(AdfActivity):
    def __init__(self, name: str, stored_procedure_name: str, linked_service: str, pipeline: AdfPipeline = None):
        super(AdfSqlServerStoredProcedureActivity, self).__init__(name, pipeline)
        self.stored_procedure_name = stored_procedure_name
        self.linked_service = LinkedServiceReference(reference_name=linked_service)

    def to_adf(self) -> SqlServerStoredProcedureActivity:
        return SqlServerStoredProcedureActivity(
            name=self.name,
            stored_procedure_name=self.stored_procedure_name,
            linked_service_name=self.linked_service,
        )
