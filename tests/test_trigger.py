import pytest

from datetime import datetime
from azure.mgmt.datafactory.models import ScheduleTriggerRecurrence, RecurrenceSchedule

from adfpy import trigger as victim
from adfpy.error import InvalidCronExpressionError, NotSupportedError


class TestAdfScheduleTrigger:
    # TODO:
    # - hit notsupportederror crons
    # - check validity of schedule (e.g. hours capped at 23, minutes at 59 etc.)

    start_time = datetime(2022, 4, 27, 21, 18)
    cron_test_data = [
        (
            "* * * * *",
            start_time,
            ScheduleTriggerRecurrence(frequency="Day", interval=1, start_time=start_time, time_zone="UTC"),
        ),
        (
            "25 * * * *",
            start_time,
            ScheduleTriggerRecurrence(frequency="Hour", interval=25, start_time=start_time, time_zone="UTC"),
        ),
        (
            "* 8 * * *",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Day",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(hours=[8], minutes=[i for i in range(60)]),
            ),
        ),
        (
            "5 5 * * *",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Day",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(hours=[5], minutes=[5]),
            ),
        ),
        (
            "* * 15 * *",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Month",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(
                    hours=[i for i in range(24)], minutes=[i for i in range(60)], month_days=[15]
                ),
            ),
        ),
        (
            "5 * 25 * *",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Day",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(hours=[i for i in range(24)], minutes=[5], month_days=[25]),
            ),
        ),
        (
            "* 12 7 * *",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Month",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(hours=[12], minutes=[i for i in range(60)], month_days=[7]),
            ),
        ),
        (
            "5 5 5 * *",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Month",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(hours=[5], minutes=[5], month_days=[5]),
            ),
        ),
        (
            "* * * * 2",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Week",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(
                    hours=[i for i in range(24)], minutes=[i for i in range(60)], week_days=["Tuesday"]
                ),
            ),
        ),
        (
            "5 * * * 5",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Week",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(hours=[i for i in range(24)], minutes=[5], week_days=["Friday"]),
            ),
        ),
        (
            "* 15 * * 5",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Week",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(hours=[15], minutes=[i for i in range(60)], week_days=["Friday"]),
            ),
        ),
        (
            "5 5 * * 5",
            start_time,
            ScheduleTriggerRecurrence(
                frequency="Week",
                interval=1,
                start_time=start_time,
                time_zone="UTC",
                schedule=RecurrenceSchedule(hours=[5], minutes=[5], week_days=["Friday"]),
            ),
        ),
    ]

    @pytest.mark.parametrize("schedule,start_time,expected", cron_test_data)
    def test_cron_schedule_conversion(self, schedule, start_time, expected):
        target = victim.AdfScheduleTrigger("Foo", schedule, start_time, [])

        result = target._convert_cron_to_adf()
        assert result == expected

    unsupported_expressions_data = [
        (
            "* * 5 * 5",
            start_time
        ),
        (
            "5 * 5 * 5",
            start_time
        ),
        (
            "* 5 5 * 5",
            start_time
        ),
        (
            "5 5 5 * 5",
            start_time
        ),
    ]

    @pytest.mark.parametrize("schedule,start_time", unsupported_expressions_data)
    def test_unsupported_expressions(self, schedule, start_time):
        target = victim.AdfScheduleTrigger("Foo", schedule, start_time, [])
        with pytest.raises(NotSupportedError):
            target._convert_cron_to_adf()


class TestAdfCronExpression:

    # minute: Union[str, int]
    # hour: Union[str, int]
    # day_of_month: Union[str, int]
    # month: Union[str, int]
    # day_of_week: Union[str, int]

    def test_invalid_minutes_cron_expression(self):
        with pytest.raises(InvalidCronExpressionError):
            victim.AdfCronExpression(minute=66, hour=12, day_of_month=5, month=5, day_of_week=3)

    def test_invalid_hours_cron_expression(self):
        with pytest.raises(InvalidCronExpressionError):
            victim.AdfCronExpression(minute=33, hour=32, day_of_month=5, month=5, day_of_week=3)

    def test_invalid_day_of_month_cron_expression(self):
        with pytest.raises(InvalidCronExpressionError):
            victim.AdfCronExpression(minute=22, hour=12, day_of_month=45, month=5, day_of_week=3)

    def test_invalid_month_cron_expression(self):
        with pytest.raises(InvalidCronExpressionError):
            victim.AdfCronExpression(minute=22, hour=12, day_of_month=5, month=15, day_of_week=3)

    def test_invalid_day_of_week_cron_expression(self):
        with pytest.raises(InvalidCronExpressionError):
            victim.AdfCronExpression(minute=22, hour=12, day_of_month=5, month=5, day_of_week=13)
