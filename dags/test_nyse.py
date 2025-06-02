from datetime import datetime, timedelta
import pendulum

from airflow.decorators import task, dag
from pendulum import Time
from us_timetables import NyseTimetable

local_tz = pendulum.timezone("America/New_York")

@task
def helloWorld():
    print("Hello World")


@dag(
    timetable=NyseTimetable(Time(20)),
    description="Test dag for Nyse Timetable",
    catchup=False,
    default_args=default_args,
)
def test_nyse(env):
    helloWorld()

test_nyse()
