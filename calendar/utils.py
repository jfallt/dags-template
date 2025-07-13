import logging
from datetime import datetime as dt

from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from pendulum import (
    MONDAY,
    TUESDAY,
    SATURDAY,
    SUNDAY,
    UTC,
    DateTime,
    naive,
)


# ---------------------------------------------------------------------
# Observance Functions
# ---------------------------------------------------------------------
def convert_to_naive(dt):
    return naive(dt.year, dt.month, dt.day)


def next_monday(dt: DateTime) -> DateTime:
    """
    If holiday falls on Saturday, use following Monday instead;
    if holiday falls on Sunday, use Monday instead
    """
    if dt.day_of_week == SATURDAY:
        return dt.add(days=2)
    elif dt.day_of_week == SUNDAY:
        return dt.add(days=2)
    return dt


def next_monday_or_tuesday(dt: DateTime) -> DateTime:
    """
    For second holiday of two adjacent ones!
    If holiday falls on Saturday, use following Monday instead;
    if holiday falls on Sunday or Monday, use following Tuesday instead
    (because Monday is already taken by adjacent holiday on the day before)
    """
    dow = dt.day_of_week
    if dow == 6 or dow == 0:
        return dt.add(days=2)
    elif dow == 1:
        return dt.add(days=1)
    return dt


def previous_friday(dt: DateTime) -> DateTime:
    """
    If holiday falls on Saturday or Sunday, use previous Friday instead.
    """
    if dt.day_of_week == 6:
        return dt.days(-1)
    elif dt.day_of_week == 0:
        return dt.days(-2)
    return dt


def sunday_to_monday(dt: DateTime) -> DateTime:
    """
    If holiday falls on Sunday, use day thereafter (Monday) instead.
    """
    if dt.day_of_week == 0:
        return dt.add(days=1)
    return dt


def weekend_to_monday(dt: DateTime) -> DateTime:
    """
    If holiday falls on Sunday or Saturday,
    use day thereafter (Monday) instead.
    Needed for holidays such as Christmas observation in Europe
    """
    if dt.day_of_week == 0:
        return dt.add(days=1)
    elif dt.day_of_week == 6:
        return dt.add(days=2)
    return dt


def nearest_workday_nyse(dt: DateTime) -> DateTime:
    """
    If New Year's day falls on Saturday do not change the date since it will not be observed per NYSE rules.
    If holiday falls on Saturday, use day before (Friday) instead;
    if holiday falls on Sunday, use day thereafter (Monday) instead.
    """
    if dt.day_of_week == SATURDAY and dt.day == 1 and dt.month == 1:
        return dt
    elif dt.day_of_week == SATURDAY:
        return dt.add(days=-1)
    elif dt.day_of_week == SUNDAY:
        return dt.add(days=1)
    return dt

def nearest_workday_sydney(dt: DateTime) -> DateTime:
    """   
    if holiday falls on weekend, use day thereafter instead.
    """
    if dt.day == 25 and dt.month == 4:
        return dt
    if dt.day_of_week == SATURDAY:
        return dt.add(days=2)
    elif dt.day_of_week == SUNDAY:
        return dt.add(days=1)
    return dt

def nearest_workday_adjacent_holiday_sydney(dt: DateTime) -> DateTime:
    """   
    For second holiday of two adjacent ones!
    If holiday falls on Saturday, use following Monday instead;
    if holiday falls on Sunday or Monday, use following Tuesday instead
    (because Monday is already taken by adjacent holiday on the day before)
    """
    if dt.day_of_week == SUNDAY or dt.day_of_week == MONDAY:
        return dt.next(TUESDAY)
    elif dt.day_of_week == SATURDAY:
        return dt.add(days=2)
    return dt


def nearest_workday(dt: DateTime) -> DateTime:
    """
    If holiday falls on Saturday, use day before (Friday) instead;
    if holiday falls on Sunday, use day thereafter (Monday) instead.
    """
    if dt.day_of_week == SATURDAY:
        return dt.add(days=-1)
    elif dt.day_of_week == SUNDAY:
        return dt.add(days=1)
    return dt


def next_workday(dt: DateTime) -> DateTime:
    """
    returns next weekday used for observances
    """
    dt = dt.add(days=1)
    while dt.weekday() > 4:
        # Mon-Fri are 0-4
        dt = dt.add(days=1)
    return dt


def previous_workday(dt: DateTime) -> DateTime:
    """
    returns previous weekday used for observances
    """
    dt = dt.add(days=-1)
    while dt.weekday() > 4:
        # Mon-Fri are 0-4
        dt = dt.add(days=-1)
    return dt


def before_nearest_workday(dt: DateTime) -> DateTime:
    """
    returns previous workday after nearest workday
    """
    return previous_workday(nearest_workday(dt))


def after_nearest_workday(dt: DateTime) -> DateTime:
    """
    returns next workday after nearest workday
    needed for Boxing day or multiple holidays in a series
    """
    return next_workday(nearest_workday(dt))


def is_tuesday_to_saturday(dt: DateTime, calendar) -> bool:
    """
    returns if date is Tuesday-Saturday
    """
    holidays = calendar.dates(dt, dt)
    dt = convert_to_naive(dt)
    if dt in holidays:
        logging.info("Date is a holiday.")
        return False
    elif dt.day_of_week in (SUNDAY, MONDAY):
        logging.info("Date is a Sunday or Monday.")
        return False
    else:
        return True

    
# ---------------------------------------------------------------------
# Workday Functions
# ---------------------------------------------------------------------
def is_workday(dt: DateTime, holidays: list) -> bool:
    dt = convert_to_naive(dt)
    if dt in holidays:
        return False
    elif dt.day_of_week in (SATURDAY, SUNDAY):
        return False
    else:
        return True


def next_business_day(dt: DateTime, holidays: list) -> DateTime:
    """
    Moves date to next business day if current day is not a biz date
    """
    dt = convert_to_naive(dt)
    while dt in holidays or dt.day_of_week in (SATURDAY, SUNDAY):
        dt = dt.add(days=1)
    return dt


def previous_business_day(dt: DateTime, holidays: list) -> DateTime:
    dt = convert_to_naive(dt)
    dt = dt.add(days=-1)
    while dt in holidays or dt.day_of_week in (SATURDAY, SUNDAY):
        dt = dt.add(days=-1)
    return dt


def get_specific_business_day(
    business_day: int, dt: DateTime, holidays: list, interval: str = "month"
) -> DateTime:
    """
    Returns n business date from the interval

    For months with less than n business date,
    returns the last business date in the month
    """
    dt = convert_to_naive(dt)
    month = dt.month

    if business_day > 0:
        # Check if the first day of the month is a biz date
        dt = dt.first_of(interval)
        if is_workday(dt, holidays):
            n = 1
        else:
            n = 0

        while n < business_day and dt.month == month:
            dt = dt.add(days=1)
            dt = next_business_day(dt, holidays)
            n += 1
    elif business_day < 0:
        # Support for reverse
        dt = dt.last_of(interval)
        if is_workday(dt, holidays):
            n = -1
        else:
            n = 0
        while n > business_day and dt.month == month:
            dt = previous_business_day(dt, holidays)
            n -= 1
    else:
        raise Exception("Cannot use 0 as a business day")
    return dt


def get_business_day_number(dt: DateTime, calendar) -> int | None:
    """
    Returns the business day count for the month of the date passed in, or None if the current day is not a business day.
    """
    holidays = calendar.dates(dt, dt)
    if not is_workday(dt, holidays):
        return None

    dt = convert_to_naive(dt)
    check_date = convert_to_naive(dt)
    check_date = check_date.first_of("month")
    month = check_date.month
    # Check if the first day of the month is a biz date
    if is_workday(check_date, holidays):
        n = 1
    else:
        n = 0
    while check_date < dt and dt.month == month:
        check_date = check_date.add(days=1)
        check_date = next_business_day(check_date, holidays)
        n += 1
    return n


def next_acceptable_business_day(
    dt: DateTime, holidays: list, days: list, interval="month"
):
    """
    Returns the next business date within a restriction of days
    """
    logging.info(f"Determining which dates are allowed in the month")
    acceptable_dates = [
        get_specific_business_day(day, dt, holidays, interval) for day in days
    ]
    acceptable_dates = list(
        filter(
            lambda date: date.replace(tzinfo=UTC) > dt.replace(tzinfo=UTC),
            acceptable_dates,
        )
    )

    if len(acceptable_dates) == 0:
        logging.info(
            "No dates left in current month, moving to first acceptable date in next month"
        )
        dt = dt.first_of("month").add(months=1).replace(tzinfo=UTC)
        acceptable_dates = [
            get_specific_business_day(day, dt, holidays) for day in days
        ]
    return min(acceptable_dates)


def previous_acceptable_business_day(dt: DateTime, holidays: list, days: list):
    """
    Returns the previous business date within a restriction of days
    """
    acceptable_dates = [get_specific_business_day(day, dt, holidays) for day in days]
    acceptable_dates = list(
        filter(
            lambda date: date.replace(tzinfo=UTC) < dt.replace(tzinfo=UTC),
            acceptable_dates,
        )
    )

    if len(acceptable_dates) == 0:
        logging.info(
            "No dates left in current month, moving to first acceptable date in previous month"
        )
        dt = dt.first_of("month").add(months=-1).replace(tzinfo=UTC)
        acceptable_dates = [
            get_specific_business_day(day, dt, holidays) for day in days
        ]
    return max(acceptable_dates)


def previous_acceptable_interval_list(dt: DateTime, holidays: list, days: list):
    """
    Returns the previous business date interval
    """
    acceptable_dates = [get_specific_business_day(day, ty, holidays) for day in days 
                        for ty in [dt.add(months=-1), dt, dt.add(months=-2)]]
    acceptable_dates = list(
        filter(
            lambda date: date.replace(tzinfo=UTC) < dt.replace(tzinfo=UTC),
            acceptable_dates,
        )
    )

    return sorted(acceptable_dates, reverse=True)

