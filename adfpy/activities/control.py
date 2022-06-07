from typing import List

from azure.mgmt.datafactory.models import (  # type: ignore
    ActivityDependency,
    ExecutePipelineActivity,
    Expression,
    ForEachActivity,
    IfConditionActivity,
    PipelineReference,
    SetVariableActivity,
)

from adfpy.activity import AdfActivity
from adfpy.pipeline import AdfPipeline


class AdfExecutePipelineActivity(AdfActivity):
    def __init__(self, name: str, pipeline_name: str, pipeline=None):
        super(AdfExecutePipelineActivity, self).__init__(name, pipeline)
        self.pipeline_name = pipeline_name

    def to_adf(self) -> ExecutePipelineActivity:
        return ExecutePipelineActivity(
            name=self.name,
            pipeline=PipelineReference(reference_name=self.pipeline_name),
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfIfConditionActivity(AdfActivity):
    def __init__(
        self,
        name: str,
        expression: str,
        if_false_activities: List[AdfActivity],
        if_true_activities: List[AdfActivity],
        pipeline=None,
    ):
        super(AdfIfConditionActivity, self).__init__(name, pipeline)
        self.expression = expression
        self.if_false_activities = if_false_activities
        self.if_true_activities = if_true_activities

    def to_adf(self) -> IfConditionActivity:
        return IfConditionActivity(
            name=self.name,
            expression=Expression(value=self.expression),
            if_true_activities=[activity.to_adf() for activity in self.if_true_activities],
            if_false_activities=[activity.to_adf() for activity in self.if_false_activities],
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfForEachActivity(AdfActivity):
    def __init__(
        self,
        name: str,
        items: str,
        activities: List[AdfActivity],
        pipeline: AdfPipeline = None,
    ):
        super(AdfForEachActivity, self).__init__(name, pipeline)
        self.items = items  # TODO: this now has to be an ADF expression. Probably want to revisit this
        self.activities = activities

        # WARNING This is a workaround that means activities within a foreachactivity may only have a single linear
        # line of dependency
        for i in range(1, len(self.activities)):
            self.activities[i].add_dependency(self.activities[i - 1].name)

    def to_adf(self) -> ForEachActivity:
        return ForEachActivity(
            name=self.name,
            items=Expression(value=self.items),
            activities=[activity.to_adf() for activity in self.activities],
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfSetVariableActivity(AdfActivity):
    def __init__(self, name: str, value: str, pipeline: AdfPipeline = None):
        super(AdfSetVariableActivity, self).__init__(name, pipeline)
        self.value = value

    def to_adf(self) -> SetVariableActivity:
        return SetVariableActivity(
            name=self.name,
            value=self.value,
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )
