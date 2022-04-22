from typing import List

from azure.mgmt.datafactory.models import (
    ScheduleTrigger,
    ScheduleTriggerRecurrence,
    PipelineReference,
    TriggerPipelineReference,
    TriggerResource,
    RecurrenceSchedule,
    RecurrenceFrequency
)


class AdfScheduleTrigger:
    def __init__(self, name: str, schedule: str, pipelines: List[str]):
        self.name = name
        self.schedule = schedule
        self.pipelines = [TriggerPipelineReference(pipeline_reference=PipelineReference(reference_name=p)) for p in pipelines]

    def to_adf(self):
        from datetime import datetime
        scheduler_recurrence = ScheduleTriggerRecurrence(frequency='Minute', interval="15",start_time="2022-04-01T19:08:00Z", time_zone='UTC')
        pipeline_reference = PipelineReference(reference_name='copyPipeline')
        # pipelines_to_run.append(TriggerPipelineReference(pipeline_reference=pipeline_reference, parameters=pipeline_parameters))
        tr_properties = TriggerResource(properties=ScheduleTrigger(recurrence=scheduler_recurrence, pipelines=[], annotations=[], additional_properties={}))
        # adf_schedule = ScheduleTriggerRecurrence(frequency=RecurrenceFrequency.DAY, interval=5)
        return tr_properties
