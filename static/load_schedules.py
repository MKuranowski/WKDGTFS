# © Copyright 2024-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable
from xml.etree import ElementTree as ET

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors
from impuls.model import Date, TimePoint
from impuls.tools import polish_calendar_exceptions
from impuls.tools.temporal import BoundedDateRange


class LoadSchedules(Task):
    NS = {"sitkol": "http://model.km.sitkol.tktelekom.pl"}
    STOP_ID_LOOKUP = {
        # cspell: disable
        "1": "gmrad",
        "2": "gmjor",
        "3": "gmpia",
        "4": "gmokr",
        "5": "brzoz",
        "6": "kazim",
        "7": "plzac",
        "8": "plglo",
        "9": "plwsc",
        "10": "otreb",
        "11": "kanie",
        "12": "nwwar",
        "13": "komor",
        "14": "prusz",
        "15": "twork",
        "16": "malic",
        "17": "regul",
        "18": "micha",
        "19": "opacz",
        "20": "wsalo",
        "21": "wrako",
        "22": "walje",
        "23": "wreor",
        "24": "wzach",
        "25": "wocho",
        "26": "wsrod",
        "27": "poles",
        "28": "milgr",
        "102": "prpkp",
        "103": "prusc",
        # cspell: enable
    }

    def __init__(self) -> None:
        super().__init__()
        self.holidays = set[Date]()
        self.missing_stations = dict[str, str]()
        self.inserted_stations = set[str]()

    def execute(self, r: TaskRuntime) -> None:
        self.retrieve_holidays(r)
        self.missing_stations.clear()
        self.inserted_stations.clear()

        with r.resources["wkd.xml"].open_text(encoding="utf-8") as f:
            tt = ET.parse(f).getroot()

        with r.db.transaction():
            r.db.raw_execute("UPDATE feed_info SET version = ?", (tt.attrib["created"],))
            for train in tt.findall("sitkol:train", self.NS):
                self.parse_train(r.db, train)

        if self.missing_stations:
            raise MultipleDataErrors(
                "stop lookup",
                [DataError(f"{id}: {name}") for id, name in self.missing_stations.items()],
            )

    def retrieve_holidays(self, r: TaskRuntime) -> None:
        self.holidays.clear()
        resource = r.resources["calendar_exceptions.csv"]
        region = polish_calendar_exceptions.PolishRegion.MAZOWIECKIE
        for date, details in polish_calendar_exceptions.load_exceptions(resource, region).items():
            if polish_calendar_exceptions.CalendarExceptionType.HOLIDAY in details.typ:
                self.holidays.add(date)

    def parse_train(self, db: DBConnection, train: ET.Element) -> None:
        symbol = train.findtext("sitkol:information/sitkol:symbol", "", self.NS).strip()
        if not symbol:
            raise DataError("<train> without a <symbol>")
        route_id = "Z" if "ZKA" in symbol else "A"

        number = train.findtext("sitkol:information/sitkol:number", "", self.NS).strip()
        if not number:
            raise DataError("<train> without a <number>")

        version = train.findtext("sitkol:information/sitkol:version", "", self.NS).strip()
        if not version:
            raise DataError("<train> without a <version>")

        id = f"{number}_{version}"

        db.raw_execute("INSERT INTO calendars (calendar_id) VALUES (?)", (id,))
        db.raw_execute(
            (
                "INSERT INTO trips (trip_id, route_id, calendar_id, short_name, "
                "wheelchair_accessible, bikes_allowed) VALUES (?, ?, ?, ?, 1, ?)"
            ),
            (id, route_id, id, number, 1 if route_id == "A" else 0),
        )

        self.parse_days(id, db, train.find("sitkol:information/sitkol:days", self.NS))
        self.parse_route(id, db, train.findall("sitkol:route/sitkol:station", self.NS))

    def parse_days(self, id: str, db: DBConnection, days: ET.Element | None) -> None:
        if days is None:
            raise DataError(f"train {id!r} has no <days>")

        # Parse days->dayOperationCode
        match days.attrib["dayOperationCode"]:
            case "C":
                weekdays = 0b1100000
                runs_on_holidays = True
            case "D":
                weekdays = 0b0011111
                runs_on_holidays = False
            case "1234567":
                weekdays = 0b1111111
                runs_on_holidays = True
            case "23456":
                weekdays = 0b0111110
                runs_on_holidays = False
            case "17":
                weekdays = 0b1000001
                runs_on_holidays = True
            case unrecognized:
                raise DataError(f"Unrecognized dayOperationCode: {unrecognized!r}")

        # Parse days->start and days->end, taking dayOperationCode and holidays into account
        active_days = set[Date]()
        for day in self.parse_date_range(days):
            is_holiday = day in self.holidays
            is_base_active = bool(weekdays & (1 << day.weekday()))

            # A day is active in either of the 3 cases:
            # 1. the weekday matches AND don't care about holidays
            # 2. the weekday matches AND it's not a holiday
            # 3. the services run on holidays AND it's a holiday
            if (
                (is_base_active and runs_on_holidays is None)  # type: ignore
                or (is_base_active and not is_holiday)
                or (runs_on_holidays and is_holiday)
            ):
                active_days.add(day)

        # Parse days->include
        for included_range in days.findall("sitkol:include/sitkol:days", self.NS):
            active_days.update(self.parse_date_range(included_range))

        # Parse days->exclude
        for included_range in days.findall("sitkol:exclude/sitkol:days", self.NS):
            active_days.difference_update(self.parse_date_range(included_range))

        # Warn if there are no days
        if not active_days:
            raise DataError(f"train {id!r} has an empty dates set")

        # Insert the dates to the DB
        db.raw_execute_many(
            "INSERT INTO calendar_exceptions (calendar_id,date,exception_type) VALUES (?,?,?)",
            ((id, i.isoformat(), 1) for i in active_days),
        )

    def parse_route(self, id: str, db: DBConnection, stations: Iterable[ET.Element]) -> None:
        previous_dep = TimePoint()

        for idx, station in enumerate(stations):
            stop_wkd_id = station.attrib["id"]
            stop_wkd_name = station.attrib["name"]
            try:
                stop_id = self.STOP_ID_LOOKUP[stop_wkd_id]
            except KeyError:
                self.missing_stations[stop_wkd_id] = stop_wkd_name
                continue

            if stop_id not in self.inserted_stations:
                self.inserted_stations.add(stop_id)
                db.raw_execute(
                    "INSERT INTO stops (stop_id, name, lat, lon) VALUES (?, ?, 0, 0)",
                    (stop_id, stop_wkd_name),
                )

            arr = self.parse_time(station.attrib["arr"] or station.attrib["dep"])
            dep = self.parse_time(station.attrib["dep"] or station.attrib["arr"])
            is_bus = station.attrib.get("serviceType") == "BUS"

            if arr < previous_dep or (idx == 0 and dep < TimePoint(hours=2, minutes=30)):
                arr = self.add_24h(arr)
            if dep < arr:
                dep = self.add_24h(dep)

            db.raw_execute(
                (
                    "INSERT INTO stop_times (trip_id, stop_id, stop_sequence, arrival_time, "
                    "departure_time, platform) VALUES (?, ?, ?, ?, ?, ?)"
                ),
                (
                    id,
                    stop_id,
                    idx,
                    round(arr.total_seconds()),
                    round(dep.total_seconds()),
                    "BUS" if is_bus else "",
                ),
            )
            previous_dep = dep

    @staticmethod
    def parse_time(x: str) -> TimePoint:
        h, m = map(int, x.split(":"))
        return TimePoint(hours=h, minutes=m)

    @staticmethod
    def parse_date_range(e: ET.Element) -> BoundedDateRange:
        return BoundedDateRange(
            start=Date.from_ymd_str(e.attrib["start"]),
            end=Date.from_ymd_str(e.attrib["end"]),
        )

    @staticmethod
    def add_24h(x: TimePoint) -> TimePoint:
        return TimePoint(seconds=x.total_seconds() + 86_400)
