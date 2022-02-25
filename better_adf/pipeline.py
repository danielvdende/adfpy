from typing import List

from azure.mgmt.datafactory.models import PipelineResource

from better_adf.activity import AdfActivity


class AdfPipeline:
    def __init__(self, name, activities: List[AdfActivity] = None, depends_on_pipelines = {}):
        self.name = name
        self.activities = activities
        if not activities:
            self.activities = []

        self.depends_on_pipelines = depends_on_pipelines

    def to_adf(self):
        return PipelineResource(activities=[act.to_adf() for act in self.activities])
