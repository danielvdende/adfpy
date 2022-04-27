import pytest

from datetime import datetime
from azure.mgmt.datafactory.models import ScheduleTriggerRecurrence, RecurrenceSchedule

from adfpy import trigger as victim


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

    @pytest.mark.parametrize("schedule,start_time,,expected", cron_test_data)
    def test_cron_schedule_conversion(self, schedule, start_time, expected):
        target = victim.AdfScheduleTrigger("Foo", schedule, start_time, [])

        result = target._convert_cron_to_adf()
        print(result)
        print(expected)
        assert result == expected
