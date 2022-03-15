from azure.mgmt.datafactory.models import (
    ActivityDependency,
    AzureBlobStorageReadSettings,
    CopyActivity,
    DatabricksSparkPythonActivity,
    DatasetReference,
    DeleteActivity,
    LinkedServiceReference,
    LookupActivity,
    SqlServerStoredProcedureActivity,
)

from adfpy.activity import AdfActivity


class AdfCopyActivity(AdfActivity):
    def __init__(self, name, input_dataset_name, output_dataset_name, source_type, sink_type, pipeline=None):
        super(AdfCopyActivity, self).__init__(name, pipeline)
        self.input_dataset = DatasetReference(reference_name=input_dataset_name)
        self.output_dataset = DatasetReference(reference_name=output_dataset_name)
        self.source_type = source_type
        self.sink_type = sink_type

    def to_adf(self):
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
        self, name: str, dataset_name: str, recursive: bool = False, wildcard: str = None, pipeline=None
    ):
        super(AdfDeleteActivity, self).__init__(name, pipeline)
        # TODO: this is an arbitrary subselection of options. We should bring this in line with what the SDK exposes.
        # this can be achieved in several ways (e.g. getting all parameters in here, using a dict, re-using the ADF SDK
        # objects, etc.)
        self.dataset_name = dataset_name
        self.recursive = recursive
        self.wildcard = wildcard

    def to_adf(self):
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
    def __init__(self, name, python_file, pipeline=None):
        super(AdfDatabricksSparkPythonActivity, self).__init__(name, pipeline)
        self.python_file = python_file

    def to_adf(self):
        return DatabricksSparkPythonActivity(
            name=self.name,
            python_file=self.python_file,
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfLookupActivity(AdfActivity):
    def __init__(self, name: str, dataset: str, source, pipeline=None):
        super(AdfLookupActivity, self).__init__(name, pipeline)
        self.dataset = DatasetReference(reference_name=dataset)
        self.source = source

    def to_adf(self):
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
    def __init__(self, name: str, stored_procedure_name: str, linked_service: str, pipeline=None):
        super(AdfSqlServerStoredProcedureActivity, self).__init__(name, pipeline)
        self.stored_procedure_name = stored_procedure_name
        self.linked_service = LinkedServiceReference(reference_name=linked_service)

    def to_adf(self):
        return SqlServerStoredProcedureActivity(
            name=self.name,
            stored_procedure_name=self.stored_procedure_name,
            linked_service_name=self.linked_service,
        )
