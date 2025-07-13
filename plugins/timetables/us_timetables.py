import logging
from datetime import timedelta, datetime as dt
from typing import Dict, Optional, Any, Union
import sys

from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from pendulum import UTC, Date, DateTime, Time, today, instance

from calendar.holidays import (
    nyse_holidays,
)
from calendar.utils import (
    is_workday,
    next_business_day,
    next_acceptable_business_day,
    previous_acceptable_business_day,
)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(filename)s] [%(asctime)s] [%(message)s]",
)

def _is_dst(zonename: str):
    import pytz
    from datetime import datetime

    tz = pytz.timezone(zonename)
    now = pytz.utc.localize(datetime.utcnow())
    return now.astimezone(tz).dst() != timedelta(0)

def _get_rollover_schedule(tz : str):
    # This will only check for 8:00EDT/7:00EST
    if _is_dst(tz):
        rollover_time = 20
    else:
        rollover_time = 19
    rollover_schedule_at = Time(rollover_time)
    return rollover_schedule_at

def _convert_datetime_to_pendulum(date, tz):
    if isinstance(date, dt):
        return instance(date, tz)
    else:
        return date.in_tz(tz)


class NyseTimetable(Timetable):
    """
    Maintained by: MAM OPS US
    Contact: COGTechMAMOperationsPlatformUS@macquarie.com
    """

    def __init__(self, schedule_at: Time, tz: str = "America/Toronto") -> None:
        self._schedule_at = schedule_at
        self.description = (
            f"Runs at {self._schedule_at} on weekdays excluding NYSE holidays"
        )
        self.holiday_list = nyse_holidays
        self.tz = tz

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """
        Tells scheduler if a DAG run is manually triggered (from the web UI, for example) and how to reverse-infer out-of-schedule run’s data interval.
        """
        delta = timedelta(days=1)
        start = DateTime.combine((run_after - delta).date(), Time.min).replace(
            tzinfo=self.tz
        )
        return DataInterval(start=start, end=(start + timedelta(days=1)))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:
            logging.info("There was a previous run on the regular schedule.")
            last_start = _convert_datetime_to_pendulum(
                last_automated_data_interval.start, self.tz
            )
            logging.info(f"Last Start: {last_start}")
            last_start_weekday = last_start.weekday()

            # JOBMIGRA-1635 if the time is past 8:00PM EDT/7:00PM EST or 0:00 UTC set the delta to days=0
            logging.info(self.tz)
            rollover_schedule_at = _get_rollover_schedule(self.tz)

            if 0 <= last_start_weekday < 4:

                logging.info("Last run on Monday through Thursday -- next is tomorrow.")
                logging.info(f"rollover_schedule_at: {rollover_schedule_at}")
                logging.info(f"_schedule_at: {self._schedule_at}")

                if self._schedule_at >= rollover_schedule_at:
                    delta = timedelta(days=0)
                else:
                    delta = timedelta(days=1)

            else:

                logging.info("Last run on Friday -- skip to next Monday. Except if start time >= 20:00")
                
                if last_start_weekday == 4 and self._schedule_at >= rollover_schedule_at:
                    delta = timedelta(days=0)
                else:
                    delta = timedelta(days=(7 - last_start_weekday))

            logging.info(f"delta: {delta}")

            next_start = DateTime.combine(
                (last_start + delta).date(), Time.min
            ).replace(tzinfo=UTC)
        else:
            logging.info("This is the first ever run on the regular schedule.")
            next_start = _convert_datetime_to_pendulum(restriction.earliest, self.tz)
            if next_start is None:
                logging.info("No start_date, don't schedule")
                return None
            next_start = next_start.replace(tzinfo=UTC)
            if not restriction.catchup:
                logging.info(
                    "If the DAG has catchup=False, today is the earliest to consider."
                )
                logging.info("Getting the latest date from today and next_start")
                next_start = max(
                    next_start.replace(tzinfo=UTC),
                    DateTime.combine(today(self.tz), Time.min).replace(tzinfo=UTC),
                )
            elif next_start.time() != Time.min:
                logging.info(
                    "If earliest does not fall on midnight, skip to the next day."
                )
                next_day = next_start.date() + timedelta(days=1)
                next_start = DateTime.combine(next_day, Time.min).replace(tzinfo=UTC)
            next_start_weekday = next_start.weekday()
            if next_start_weekday in (5, 6):
                logging.info("If next start is in the weekend, go to next Monday.")
                delta = timedelta(days=(7 - next_start_weekday))
                next_start = next_start + delta
        logging.info("Check if next_start falls on a holiday")
        holidays = self.holiday_list.dates(next_start, next_start)
        if not is_workday(next_start, holidays):
            logging.info("If next start is not a workday move to next workday.")
            next_start = next_business_day(next_start, holidays)
        else:
            logging.info("Next start is a workday!")
        if restriction.latest is not None and next_start > restriction.latest:
            logging.info("Over the DAG's scheduled end; don't schedule.")
            return None
        next_start = DateTime.combine(next_start.date(), self._schedule_at)
        logging.info(f"Setting next start={next_start}")
        return DagRunInfo.interval(
            start=next_start.replace(tzinfo=self.tz),
            end=next_start.replace(tzinfo=self.tz),
        )

    def serialize(self) -> Dict[str, Any]:
        return {"schedule_at": self._schedule_at.isoformat()}

    @classmethod
    def deserialize(cls, value: Dict[str, Any]) -> Timetable:
        return cls(Time.fromisoformat(value["schedule_at"]))


class NyseSelectBusinessDays(Timetable):
    """
    Maintained by: MAM OPS US
    Contact: COGTechMAMOperationsPlatformUS@macquarie.com
    -------------

    schedule_at: time of day to run the dag

    Days: Can be a single day or a list of days
    use negative numbers for working backwards from the end of the month

        i.e. -1 would be the last business day of the month

        NOTE: it is not recommended to use any day > 19 as there are only
        so many business dates in a month

    tz: Defaults to EDT, please review the pendulum tzs if overriding

    interval: Defaults to monthly, quarterly is a future enhancement
    """

    def _input_validation(self):
        """
        Prevent bad days from being passed
        """
        if self.days == []:
            raise ValueError("Days cannot be blank!")
        if 0 in self.days:
            raise ValueError("0 is not a valid day")
        days = set(self.days)
        numbers_not_in_range = [
            day for day in days if day not in range(-22, 22) or day == 0
        ]
        if len(numbers_not_in_range) > 0:
            raise AirflowTimetableInvalid(
                f"{numbers_not_in_range} are not valid business dates"
            )

    def _generate_description(self):
        """
        Depending on the days given, return a human readable description
        """
        if len(self.days) == 1:
            days = self.days[0]
            if days == 1:
                return f"Runs at {self._schedule_at} on the first business day of the {self.interval}"
            elif days == -1:
                return f"Runs at {self._schedule_at} on the last business day of the {self.interval}"
            else:
                return f"Runs at {self._schedule_at} on the {str(self.days)} business day of the {self.interval}"
        else:
            return f"Runs at {self._schedule_at} on the {','.join(map(str, self.days))} business days of the {self.interval}"

    def __init__(
        self,
        schedule_at: Union[Time, list],
        days: Union[list, int],
        tz="America/Toronto",
        interval="month",
    ) -> None:
        if not isinstance(days, list):
            self.days = [days]
        else:
            self.days = days
        self.interval = interval
        self._schedule_at = schedule_at
        self.description = self._generate_description()
        self._input_validation()
        self.holiday_list = nyse_holidays
        self.tz = tz

    def _move_to_next_business_date(self, next_start: DateTime):
        """
        Determines if the next start date is fits the criteria of a business date based on the holidays
        """
        holidays = self.holiday_list.dates(next_start, next_start)
        next_start = next_acceptable_business_day(
            next_start, holidays, self.days, self.interval
        )
        return next_start

    def _get_prev(self, next_start, last_automated_data_interval):
        """
        Get the previous end date
        """
        logging.info("Determine previous run")
        if last_automated_data_interval is not None:
            previous_end = last_automated_data_interval.end.in_tz(self.tz)
        else:
            # If we do not have an interval we can reverse infer
            logging.info("No interval, inferring previous end date")
            holidays = self.holiday_list.dates(next_start, next_start)
            previous_end = previous_acceptable_business_day(
                next_start, holidays, self.days
            )
        return previous_end

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """
        Tells scheduler if a DAG run is manually triggered (from the web UI, for example) and
        how to reverse-infer out-of-schedule run's data interval.

        This timetable is not designed to reverse-infer out-of-schedule runs.
        """
        return DataInterval(start=run_after, end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:
            logging.info(
                f"There was a previous run on the regular schedule.\n\tStarting: {last_automated_data_interval.start.in_tz(self.tz)}\n\tEnding: {last_automated_data_interval.end.in_tz(self.tz)}"
            )
            next_start = last_automated_data_interval.end.in_tz(self.tz)
        else:
            logging.info("This is the first ever run on the regular schedule.")
            next_start = _convert_datetime_to_pendulum(restriction.earliest, self.tz)
            logging.info(f"The earliest date allowed = {restriction.earliest}")
            if next_start is None:
                logging.info("No start_date, don't schedule")
                return None

            if not restriction.catchup:
                next_start = DateTime.combine(
                    today(self.tz).subtract(days=1), Time.min
                ).replace(tzinfo=UTC)
                logging.info(
                    f"If the DAG has catchup=False, yesterday ({next_start}) is the earliest to consider."
                )
            elif next_start.time() > self._schedule_at:
                logging.info(
                    "If earliest time is after the schedule_at time, find next acceptable date."
                )

        # Determine the next run date and previous run
        next_start = self._move_to_next_business_date(next_start)
        previous_end = DateTime.combine(
            self._get_prev(next_start, last_automated_data_interval).date(),
            self._schedule_at,
        )
        next_start = DateTime.combine(next_start.date(), self._schedule_at)

        # DAG Restrictions
        if restriction.latest is not None and next_start.replace(
            tzinfo="UTC"
        ) > restriction.latest.replace(tzinfo="UTC"):
            logging.info("Over the DAG's scheduled end; don't schedule.")
            return None

        # interval MUST have tz awareness
        interval_start = previous_end.replace(tzinfo=self.tz)
        interval_end = next_start.replace(tzinfo=self.tz)
        logging.info(
            f"Setting data interval\n\tStarting: {interval_start}\n\tEnding: {interval_end}"
        )
        return DagRunInfo.interval(
            start=interval_start,
            end=interval_end,
        )

    # TODO how to serialize/deserialize multiple times?
    # I don't think it's possible to have multiple times with a timetable currently
    # the underlying class only supports deserializing one
    def serialize(self) -> Dict[str, Any]:
        """
        Stores timetable info in the database
        """
        return {"schedule_at": self._schedule_at.isoformat(), "days": self.days}

    @classmethod
    def deserialize(cls, value: Dict[str, Any]) -> Timetable:
        """
        This method converts the "schedule at" values into a usable format
        """
        return cls(Time.fromisoformat(value["schedule_at"]), value["days"])


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "mam_ops_timetable_plugin"
    timetables = [
        NyseTimetable,
        NyseSelectBusinessDays,
    ]
