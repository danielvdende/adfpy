from datetime import datetime, timezone
from typing import List

from azure.mgmt.datafactory.models import PipelineResource  # type: ignore

from adfpy.activity import AdfActivity
from adfpy.trigger import AdfScheduleTrigger


class AdfPipeline:
    def __init__(self,
                 name: str,
                 activities: List[AdfActivity] = None,
                 depends_on_pipelines={},
                 schedule=None,
                 start_time=None):
        self.name = name
        self.activities = activities
        self.schedule = schedule
        self.start_time = start_time
        if not activities:
            self.activities = []

        self.depends_on_pipelines = depends_on_pipelines

        if schedule:
            if not self.start_time:
                self.start_time = datetime.now(tz=timezone.utc)
            self.schedule = AdfScheduleTrigger(name=f"{self.name}-trigger",
                                               schedule=self.schedule,
                                               start_time=self.start_time,
                                               pipelines=[self.name]
                                               )

    def to_adf(self):
        return PipelineResource(activities=[act.to_adf() for act in self.activities])

    def __eq__(self, other):
        # This is debatable
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"<AdfPipeline: {self.name}>"
