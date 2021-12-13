from typing import List

from azure.mgmt.datafactory.models import PipelineResource

from better_adf.activity import AdfActivity


class AdfPipeline:
    def __init__(self, name, activities: List[AdfActivity]):
        self.name = name
        self.activities = activities

    def to_adf(self):
        return PipelineResource(activities=[act.to_adf() for act in self.activities])
