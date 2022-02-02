from typing import List

from azure.mgmt.datafactory.models import (
    ExecutePipelineActivity,
    PipelineReference,
    ActivityDependency,
    IfConditionActivity,
    Expression,
    ForEachActivity,
)

from better_adf.activity import AdfActivity


class AdfExecutePipelineActivity(AdfActivity):
    def __init__(self, name: str, pipeline_name: str):
        super(AdfExecutePipelineActivity, self).__init__(name)
        self.pipeline_name = pipeline_name

    def to_adf(self):
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
    ):
        super(AdfIfConditionActivity, self).__init__(name)
        self.expression = expression
        self.if_false_activities = if_false_activities
        self.if_true_activities = if_true_activities

    def to_adf(self):
        return IfConditionActivity(
            name=self.name,
            expression=Expression(value=self.expression),
            if_true_activities=[activity.to_adf() for activity in self.if_true_activities],
            if_false_activities=[activity.to_adf() for activity in self.if_false_activities],
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep-dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )


class AdfForEachActivity(AdfActivity):
    def __init__(self, name, items: str, activities: List[AdfActivity]):
        super(AdfForEachActivity, self).__init__(name)
        self.items = items  # TODO: this now has to be an ADF expression. Probably want to revisit this
        self.activities = activities

    def to_adf(self):
        return ForEachActivity(
            name=self.name,
            items=Expression(value=self.items),
            activities=[activity.to_adf() for activity in self.activities],
            depends_on=[
                ActivityDependency(activity=dep_name, dependency_conditions=dep_conditions)
                for dep_name, dep_conditions in self.depends_on.items()
            ],
        )
