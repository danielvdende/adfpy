from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

from azure.mgmt.datafactory.models import (  # type: ignore
    ScheduleTrigger,
    ScheduleTriggerRecurrence,
    PipelineReference,
    TriggerPipelineReference,
    TriggerResource,
    RecurrenceSchedule,
)

from adfpy.error import NotSupportedError, InvalidCronExpressionError


@dataclass
class AdfCronExpression:
    minute: Union[str, int]
    hour: Union[str, int]
    day_of_month: Union[str, int]
    month: Union[str, int]
    day_of_week: Union[str, int]

    days_of_week_adf_map = {
        "0": "Sunday",
        "1": "Monday",
        "2": "Tuesday",
        "3": "Wednesday",
        "4": "Thursday",
        "5": "Friday",
        "6": "Saturday"
    }

    def __post_init__(self):
        # For now, we implement our own validation logic here. TODO: consider using pydantic
        # TODO: how about pattern matching for this?
        if self.minute != "*":
            if not 0 <= int(self.minute) <= 59:
                raise InvalidCronExpressionError(f"Invalid minutes specified. Value entered: {self.minute}. "
                                                 f"Please set minutes in the range 0-59 or *")
        if self.hour != "*":
            if not 0 <= int(self.hour) <= 23:
                raise InvalidCronExpressionError(f"Invalid hours specified. Value entered: {self.hour}. "
                                                 f"Please set hours in the range 0-23 or *")
        if self.day_of_month != "*":
            if not 0 <= int(self.day_of_month) <= 31:
                raise InvalidCronExpressionError(f"Invalid day_of_month specified. Value entered: {self.day_of_month}. "
                                                 f"Please set day_of_month in the range 1-31 or *")
        if self.month != "*":
            if not 1 <= int(self.month) <= 12:
                raise InvalidCronExpressionError(f"Invalid month specified. Value entered: {self.month}. "
                                                 f"Please set month in the range 1-12 or *")
        if self.day_of_week != "*":
            if not 0 <= int(self.day_of_week) <= 6:
                raise InvalidCronExpressionError(f"Invalid day_of_week specified. Value entered: {self.day_of_week}. "
                                                 f"Please set day_of_week in the range 0-6 or *")
            else:
                self.day_of_week_adf = self.days_of_week_adf_map[self.day_of_week]


class AdfScheduleTrigger:
    def __init__(self, name: str, schedule: str, start_time: datetime, pipelines: List[str], time_zone: str = "UTC"):
        self.name = name
        self.schedule = schedule
        self.start_time = start_time
        self.time_zone = time_zone
        self.pipelines = [
            TriggerPipelineReference(pipeline_reference=PipelineReference(reference_name=p)) for p in pipelines
        ]

        self.preset_expressions_mapping = {
            "@hourly": "Hour",
            "@daily": "Day",
            "@weekly": "Week",
            "@monthly": "Month",
            "@yearly": None
        }

    def to_adf(self):
        scheduler_recurrence = self._convert_cron_to_adf()
        tr_properties = TriggerResource(
            properties=ScheduleTrigger(recurrence=scheduler_recurrence,
                                       pipelines=self.pipelines,
                                       annotations=[],
                                       additional_properties={}
                                       )
        )
        return tr_properties

    def convert_preset_expression_to_adf(self, schedule: str) -> ScheduleTriggerRecurrence:
        if schedule not in self.preset_expressions_mapping:
            raise ValueError(f"Expression {schedule} is not in the predefined expressions mapping")
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

    def _convert_cron_to_adf(self) -> ScheduleTriggerRecurrence:
        """
        Basic recurrence options: minutes/hours
        Advanced recurrence options: days/weeks
        -> basic support for this kind of interval is fine. But the more advanced things like
        running every 50 days is harder. This isn't something cron can do by default it seems.
        Would need further investigation.

            Convert a cron-like string to a set of objects understood by adf


        Returns:

        """
        if self.schedule in self.preset_expressions_mapping:
            return self.convert_preset_expression_to_adf(self.schedule)
        else:
            raw_cron_components = self.schedule.split(" ")
            if len(raw_cron_components) != 5:
                raise InvalidCronExpressionError(f"The provided cron expression: {self.schedule} has the wrong number "
                                                 f"of components. There should be 5")
            cron_components = AdfCronExpression(*raw_cron_components)

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
                                                             interval=int(cron_components.minute),
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
                                                                hours=[int(cron_components.hour)],
                                                                minutes=[i for i in range(60)]
                                                             ))
                        else:
                            # 5 5 * * *
                            return ScheduleTriggerRecurrence(frequency="Day",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[int(cron_components.hour)],
                                                                 minutes=[int(cron_components.minute)]
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
                                                                 month_days=[int(cron_components.day_of_month)]
                                                             ))
                        else:
                            # 5 * 5 * *
                            return ScheduleTriggerRecurrence(frequency="Day",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[i for i in range(24)],
                                                                 minutes=[int(cron_components.minute)],
                                                                 month_days=[int(cron_components.day_of_month)]
                                                             ))
                    else:
                        if cron_components.minute == "*":
                            # * 5 5 * *
                            return ScheduleTriggerRecurrence(frequency="Month",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[int(cron_components.hour)],
                                                                 minutes=[i for i in range(60)],
                                                                 month_days=[int(cron_components.day_of_month)]
                                                             ))
                        else:
                            # 5 5 5 * *
                            return ScheduleTriggerRecurrence(frequency="Month",
                                                             interval=1,
                                                             start_time=self.start_time,
                                                             time_zone=self.time_zone,
                                                             schedule=RecurrenceSchedule(
                                                                 hours=[int(cron_components.hour)],
                                                                 minutes=[int(cron_components.minute)],
                                                                 month_days=[int(cron_components.day_of_month)]
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
                                                                 minutes=[int(cron_components.minute)],
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
                                                                 hours=[int(cron_components.hour)],
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
                                                                 hours=[int(cron_components.hour)],
                                                                 minutes=[int(cron_components.minute)],
                                                                 week_days=[cron_components.day_of_week_adf]
                                                             ))
                else:
                    unsupported_expressions = [
                        {"* * 5 * 5": "At every minute on day-of-month 5 and on Friday."},
                        {"5 * 5 * 5": "At minute 5 on day-of-month 5 and on Friday."},
                        {"* 5 5 * 5": "At every minute past hour 5 on day-of-month 5 and on Friday."},
                        {"5 5 5 * 5": "At 05:05 on day-of-month 5 and on Friday."}
                    ]
                    raise NotSupportedError(f"""
                        There are a number of Cron expressions that cannot be expressed properly with ADF's scheduling
                        logic. These are: {unsupported_expressions}""")
