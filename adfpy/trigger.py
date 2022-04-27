from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

from azure.mgmt.datafactory.models import (
    ScheduleTrigger,
    ScheduleTriggerRecurrence,
    PipelineReference,
    TriggerPipelineReference,
    TriggerResource,
    RecurrenceSchedule,
    RecurrenceFrequency
)


@dataclass
class AdfCronExpression:
    # TODO: add validation for valid cron (e.g. max hours, max minutes)
    minute: Union[str, int]
    hour: Union[str, int]
    day_of_month: Union[str, int]
    month: Union[str, int]
    day_of_week: Union[str, int]

    days_of_week_adf_map = {
        0: "Sunday",
        1: "Monday",
        2: "Tuesday",
        3: "Wednesday",
        4: "Thursday",
        5: "Friday",
        6: "Saturday"
    }

    def __post_init__(self):
        if not self.day_of_week == "*":
            self.day_of_week_adf = self.days_of_week_adf_map[self.day_of_week]


class NotSupportedError(Exception):
    pass


class AdfScheduleTrigger:
    def __init__(self, name: str, schedule: str, start_time: datetime, pipelines: List[str], time_zone: str = "UTC"):
        self.name = name
        self.schedule = schedule
        self.start_time = start_time
        self.time_zone = time_zone
        self.pipelines = [TriggerPipelineReference(pipeline_reference=PipelineReference(reference_name=p)) for p in pipelines]

        self.preset_expressions_mapping = {
            "@hourly": "Hour",
            "@daily": "Day",
            "@weekly": "Week",
            "@monthly": "Month",
            "@yearly": None
        }

    def to_adf(self):
        scheduler_recurrence = self._convert_cron_to_adf()
        tr_properties = TriggerResource(properties=ScheduleTrigger(recurrence=scheduler_recurrence, pipelines=self.pipelines, annotations=[], additional_properties={}))
        return tr_properties

    def convert_preset_expression_to_adf(self, schedule):
        # assumption: schedule is in the mapping
        mapped_frequency = self.preset_expressions_mapping[schedule]
        if mapped_frequency:
            return ScheduleTriggerRecurrence(frequency=mapped_frequency,
                                             interval=1,
                                             start_time=self.start_time,
                                             time_zone=self.time_zone)
        elif schedule == "@yearly":
            return ScheduleTriggerRecurrence(frequency="Month",
                                             interval=12,
                                             start_time=self.start_time,
                                             time_zone=self.time_zone)

    def _convert_cron_to_adf(self):
        """
        Basic recurrence options: minutes/hours
        Advanced recurrence options: days/weeks  -> basic support for this kind of interval is fine. But the more advanced things like
        running every 50 days is harder. This isn't something cron can do by default it seems. Would need further investigation.

            Convert a cron-like string to a set of objects understood by adf

        @hourly
        @daily
        @weekly
        @monthly
        @yearly

        Returns:

        """
        if self.schedule in self.preset_expressions_mapping:
            return self.convert_preset_expression_to_adf(self.schedule)
        else:
            cron_components = self.schedule.split(" ")
            assert len(cron_components) == 5
            cron_components = AdfCronExpression(*cron_components)

            if cron_components.day_of_week == "*":
                if cron_components.day_of_month == "*":
                    if cron_components.hour == "*":
                        if cron_components.minute == "*":
                            # * * * * *
                            return ScheduleTriggerRecurrence(frequency="Day",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone)
                        else:
                            # 5 * * * *
                            return ScheduleTriggerRecurrence(frequency="Hour",
                                                             interval=cron_components.minute,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone)
                    else:
                        if cron_components.minute == "*":
                            # * 5 * * *
                            return ScheduleTriggerRecurrence(frequency="Day",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                hours=[cron_components.hour],
                                                                minutes=[i for i in range(60)]
                                                             ))
                        else:
                            # 5 5 * * *
                            return ScheduleTriggerRecurrence(frequency="Day",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[cron_components.hour],
                                                                 minutes=[cron_components.minute]
                                                             ))
                else:
                    if cron_components.hour == "*":
                        if cron_components.minute == "*":
                            # * * 5 * *
                            return ScheduleTriggerRecurrence(frequency="Month",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[i for i in range(24)],
                                                                 minutes=[i for i in range(60)],
                                                                 month_days=[cron_components.day_of_month]
                                                             ))
                        else:
                            # 5 * 5 * *
                            return ScheduleTriggerRecurrence(frequency="Day",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[i for i in range(24)],
                                                                 minutes=[5],
                                                                 month_days=[cron_components.day_of_month]
                                                             ))
                    else:
                        if cron_components.minute == "*":
                            # * 5 5 * *
                            return ScheduleTriggerRecurrence(frequency="Month",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[cron_components.hour],
                                                                 minutes=[i for i in range(60)],
                                                                 month_days=[cron_components.day_of_month]
                                                             ))
                        else:
                            # 5 5 5 * *
                            return ScheduleTriggerRecurrence(frequency="Month",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[cron_components.hour],
                                                                 minutes=[cron_components.minute],
                                                                 month_days=[cron_components.day_of_month]
                                                             ))
            else:
                if cron_components.day_of_month == "*":
                    if cron_components.hour == "*":
                        if cron_components.minute == "*":
                            # * * * * 5
                            return ScheduleTriggerRecurrence(frequency="Week",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[i for i in range(24)],
                                                                 minutes=[i for i in range(60)],
                                                                 week_days=[cron_components.day_of_week_adf]
                                                             ))
                        else:
                            # 5 * * * 5
                            return ScheduleTriggerRecurrence(frequency="Week",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[i for i in range(24)],
                                                                 minutes=[cron_components.minute],
                                                                 week_days=[cron_components.day_of_week_adf]
                                                             ))
                    else:
                        if cron_components.minute == "*":
                            # * 5 * * 5
                            return ScheduleTriggerRecurrence(frequency="Week",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[cron_components.hour],
                                                                 minutes=[i for i in range(60)],
                                                                 week_days=[cron_components.day_of_week_adf]
                                                             ))
                        else:
                            # 5 5 * * 5
                            return ScheduleTriggerRecurrence(frequency="Week",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[cron_components.hour],
                                                                 minutes=[cron_components.minute],
                                                                 week_days=[cron_components.day_of_week_adf]
                                                             ))
                else:
                    raise NotSupportedError("""
                        There are a number of Cron expressions that cannot be expressed properly with ADF's scheduling
                        logic. These are: """
                    )
                    # if self.hour == "*":
                    #     if self.minute == "*":
                    #         # Not possible
                    #         # * * 5 * 5
                    #     else:
                    #         # Not possible
                    #         # 5 * 5 * 5
                    # else:
                    #     if self.minute == "*":
                    #         # Not possible
                    #         # * 5 5 * 5
                    #     else:
                    #         # Not possible
                    #         # 5 5 5 * 5



