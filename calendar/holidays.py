from datetime import timedelta

from dateutil.easter import easter
try:
    from pendulum import (
    MONDAY,
    THURSDAY,
    DateTime,
    naive,
    period,
)
except ImportError:
    from pendulum import (
    MONDAY,
    THURSDAY,
    DateTime,
    naive,
    interval as period, # this function was renamed in 3.0.0
)

from calendar.utils import (
    convert_to_naive,
    nearest_workday_nyse,
    nearest_workday,
    after_nearest_workday,
    nearest_workday_sydney,
    nearest_workday_adjacent_holiday_sydney,
)


# ---------------------------------------------------------------------
# Holiday Classes
# ---------------------------------------------------------------------
class Holiday:
    """
    Class that defines a holiday with start/end dates and rules
    for observance.
    """

    def __init__(
        self,
        name,
        year: int = None,
        month: int = None,
        day: int = None,
        offset=None,
        offset_length: int = None,
        observance=None,
        start_date: DateTime = None,
        end_date: DateTime = None,
    ) -> None:
        """
        Parameters
        ----------
        name : str
            Name of the holiday , defaults to class name
        year (Optional): only use year when holiday is a one off
        month: Month of holiday
        day: Numbered day of holiday
        offset : Pendulum day or any predefined functions like Easter
        offset_length: Distance from offset (i.e. 4th thursday would be offset = THURSDAY and offset_length = 4)
        observance: function
            computes when holiday is given (nearest workday is the most common)
        tz: defaults to EDT
        start_date: when the holiday starts (i.e. Juneteeth)
        end_date: when holiday ends
        """
        if offset is not None and observance is not None:
            raise NotImplementedError("Cannot use both offset and observance.")
        self.name = name
        self.year = year
        self.month = month
        self.day = day
        self.offset = offset
        self.offset_length = offset_length
        self.tz = "America/Toronto"
        self.start_date = (
            start_date
            if start_date is not None
            else DateTime.now(tz=self.tz).add(years=-1)
        )
        self.end_date = (
            end_date if end_date is not None else DateTime.now(tz=self.tz).add(years=1)
        )
        try:
            assert type(self.start_date) == DateTime
        except:
            raise (Exception("Start date must be DateTime!"))
        try:
            assert type(self.end_date) == DateTime
        except:
            raise (Exception("End date must be DateTime!"))
        self.start_date = convert_to_naive(self.start_date)
        self.end_date = convert_to_naive(self.end_date)
        self.observance = observance

    def dates(self, start_date, end_date):
        """
        Calculate holidays observed between start date and end date
        Parameters
        ----------
        start_date : starting date, datetime-like, optional
        end_date : ending date, datetime-like, optional
        """

        if self.year is not None:
            dt = DateTime(self.year, self.month, self.day)
            return [dt]
        dates = self._reference_dates(start_date, end_date)
        holidays = self._apply_rule(dates)
        return holidays

    def _reference_dates(self, start_date, end_date):
        """
        Get reference dates for the holiday.
        Return reference dates for the holiday also returning the year
        prior to the start_date and year following the end_date.  This ensures
        that any offsets to be applied will yield the holidays within
        the passed in dates.
        """
        reference_start_date = DateTime(start_date.year, self.month, self.day).add(
            years=-1
        )
        reference_end_date = DateTime(end_date.year, self.month, self.day).add(years=1)
        # Don't process unnecessary holidays
        date_period = period(start=reference_start_date, end=reference_end_date)
        return [reference_start_date.add(years=i) for i in range(date_period.years + 1)]

    def _apply_rule(self, dates):
        """
        Apply the given offset/observance to a list of dates.
        Parameters
        ----------
        dates : list
            Dates to apply the given offset/observance rule
        Returns
        -------
        list of dates with rules applied
        """
        if self.observance is not None:
            observed = [
                self.observance(dt)
                for dt in dates
                if dt >= self.start_date and dt <= self.end_date
            ]
            return observed
        # when using timedelta with offsets we have to convert back to pendulum datetimes        
        if self.offset is not None:            
            offset_dates = []
            if self.offset == "Easter":  # for good friday and easter monday
                for date in dates:
                    easter_date = easter(date.year, 3)
                    date = easter_date + timedelta(self.offset_length)
                    offset_dates.append(convert_to_naive(date))
            else:
                for date in dates:
                    if self.offset_length < 0:
                        offset_length = self.offset_length
                        if date.day_of_week != self.offset:
                            date = date.previous(self.offset)
                        offset_length += 1
                        date -= timedelta(weeks=offset_length)
                    else:
                        offset_length = self.offset_length
                        if date.day_of_week != self.offset:
                            date = date.next(self.offset)
                        offset_length -= 1
                        date += timedelta(weeks=offset_length)
                    offset_dates.append(convert_to_naive(date))
        return offset_dates


class HolidayCollection:
    """
    Collects holidays required to determine if a date is a workday
    """

    def __init__(
        self,
        holidays: list = [],
    ) -> list:
        self.holidays = holidays
        self._cache = None
        self._min_date = None
        self._max_date = None

    def dates(self, start_date=None, end_date=None):
        """
        Returns holiday dates
        """
        # If this is not the first time running this we want to return
        # the cached value if it falls within the date range
        if self._min_date is not None:
            if (
                convert_to_naive(start_date) >= self._min_date
                and convert_to_naive(end_date) <= self._max_date
            ):
                return self._cache
        if start_date is None:
            start_date = naive(1970, 1, 1)
        if end_date is None:
            end_date = naive(2200, 12, 31)
        self._cache = sorted(
            list(
                set(
                    [
                        item
                        for holiday in self.holidays
                        for item in holiday.dates(start_date, end_date)
                    ]
                )
            )
        )
        self._min_date = self._cache[0]
        self._max_date = self._cache[-1]
        return self._cache


# ---------------------------------------------------------------------
# Holidays with offset rules
# ---------------------------------------------------------------------
USMartinLutherKingJr = Holiday(
    "Birthday of Martin Luther King, Jr.",
    start_date=naive(1986, 1, 1),
    month=1,
    day=1,
    offset=MONDAY,
    offset_length=3,
)

USPresidentsDay = Holiday(
    "Washington's Birthday", month=2, day=1, offset=MONDAY, offset_length=3
)
GoodFriday = Holiday("Good Friday", month=1, day=1, offset="Easter", offset_length=-2)
EasterMonday = Holiday("Good Friday", month=1, day=1, offset="Easter", offset_length=1)
USMemorialDay = Holiday(
    "Memorial Day", month=5, day=31, offset=MONDAY, offset_length=-1
)
USLaborDay = Holiday("Labor Day", month=9, day=1, offset=MONDAY, offset_length=1)
USColumbusDay = Holiday("Columbus Day", month=10, day=1, offset=MONDAY, offset_length=2)
USThanksgivingDay = Holiday(
    "Thanksgiving Day", month=11, day=1, offset=THURSDAY, offset_length=4
)

# ---------------------------------------------------------------------
# One-off Holidays
# Note: The year field must be included for one-off holidays.
# ---------------------------------------------------------------------
DayOfMourning = Holiday(
    "Day of Mourning",
    year=2025,
    month=1,
    day=9,
    observance=nearest_workday
)


nyse_holidays = HolidayCollection(
    holidays=[
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday_nyse),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        USLaborDay,
        Holiday("July 4th", month=7, day=4, observance=nearest_workday),
        Holiday(
            "Juneteenth",
            month=6,
            day=19,
            observance=nearest_workday,
            start_date=naive(2022, 6, 18),
        ),
        USThanksgivingDay,
        Holiday("Christmas Day", month=12, day=25, observance=nearest_workday),
        DayOfMourning,  # For Late President Jimmy Carter
    ]
)

cboe_holidays = HolidayCollection(
    holidays=[
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday),
        Holiday("Australia Day", month=1, day=26, observance=nearest_workday),
        GoodFriday,
        EasterMonday,
        Holiday("ANZAC Day", month=1, day=26, observance=nearest_workday),
        Holiday("Queen's Birthday", month=4, day=25, observance=nearest_workday),
        Holiday("Boxing Day", month=12, day=26, observance=after_nearest_workday),
        Holiday("Christmas Day", month=12, day=25, observance=after_nearest_workday),
    ]
)

nsw_holidays = HolidayCollection(
    holidays=[
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday_sydney),
        Holiday("Australia Day", month=1, day=26, observance=nearest_workday_sydney),
        GoodFriday,
        EasterMonday,
        Holiday("ANZAC Day", month=4, day=25, observance=nearest_workday_sydney),
        Holiday("King's Day", month=6, day=1, offset=MONDAY, offset_length=2),
        Holiday("Bank Holiday", month=8, day=1, offset=MONDAY, offset_length=1),
        Holiday("Labour Day", month=10, day=1, offset=MONDAY, offset_length=1),
        Holiday("Boxing Day", month=12, day=26, observance=nearest_workday_adjacent_holiday_sydney),
        Holiday("Christmas Day", month=12, day=25, observance=nearest_workday_sydney),
    ]
)
