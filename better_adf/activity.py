import abc

from azure.mgmt.datafactory.models import CopyActivity, CopySink, DatasetReference, DatabricksSparkPythonActivity, \
    ActivityDependency


class AdfActivity:
    depends_on = set()

    def __init__(self, name):
        self.name = name

    @abc.abstractmethod
    def to_adf(self):
        pass


class AdfCopyActivity(AdfActivity):
    def __init__(self, name, input_dataset_name, output_dataset_name, source_type, sink_type):
        super(AdfCopyActivity, self).__init__(name)
        self.input_dataset = DatasetReference(reference_name=input_dataset_name)
        self.output_dataset = DatasetReference(reference_name=output_dataset_name)
        self.source_type = source_type
        self.sink_type = sink_type

    def __rshift__(self, other: AdfActivity):
        other.depends_on.add(self.name)

    def to_adf(self):
        return CopyActivity(name=self.name,
                            inputs=[self.input_dataset],
                            outputs=[self.output_dataset],
                            source=self.source_type,
                            sink=self.sink_type)


class AdfDatabricksSparkPythonActivity(AdfActivity):
    def __init__(self, name, python_file):
        super(AdfDatabricksSparkPythonActivity, self).__init__(name)
        self.python_file = python_file

    # def __lshift__(self, other: AdfActivity):
    #     self.depends_on.add(other.name)

    def to_adf(self):
        return DatabricksSparkPythonActivity(name=self.name,
                                             python_file=self.python_file,
                                             depends_on=[ActivityDependency(activity=dep, dependency_conditions=['Succeeded']) for dep in self.depends_on])
