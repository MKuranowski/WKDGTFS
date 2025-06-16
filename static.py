# © Copyright 2024-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import os
from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from itertools import chain, groupby, pairwise
from pathlib import Path
from typing import Any, Final, Iterable, Sequence, TypeVar, cast
from xml.etree import ElementTree as ET

import pyroutelib3
from impuls import App, DBConnection, Pipeline, PipelineOptions, Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors
from impuls.extern import load_gtfs
from impuls.model import Date, FeedInfo, TimePoint, Trip
from impuls.resource import HTTPResource, LocalResource, ZippedResource
from impuls.tasks import AddEntity, GenerateTripHeadsign, SaveGTFS, SplitTripLegs
from impuls.tools import polish_calendar_exceptions
from impuls.tools.temporal import BoundedDateRange

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_lang",
        "agency_timezone",
    ),
    "calendar_dates.txt": ("date", "service_id", "exception_type"),
    "fare_attributes.txt": (
        "agency_id",
        "fare_id",
        "price",
        "currency_type",
        "payment_method",
        "transfers",
        "transfer_duration",
    ),
    "feed_info.txt": (
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_version",
    ),
    "routes.txt": (
        "agency_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "stops.txt": (
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "wheelchair_boarding",
    ),
    "trips.txt": (
        "route_id",
        "service_id",
        "trip_id",
        "trip_headsign",
        "trip_short_name",
        "direction_id",
        "shape_id",
        "wheelchair_accessible",
        "bikes_allowed",
    ),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
    ),
    "shapes.txt": ("shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"),
}

STATIC_FILES_DIR = Path(__file__).with_name("static_files")
T = TypeVar("T")


class AssignDirectionIds(Task):
    # List of pairs of stops in the outbound direction -
    # if a trip stops at the first one before the second one,
    # the direction_id should be "0", or "1" if it stops
    # on the second one before the first.
    OUTBOUND_DIRECTION: Final[Sequence[tuple[str, str]]] = [
        ("wzach", "walje"),
        ("prusz", "komor"),
        ("prusp", "komor"),
        ("komor", "regul"),
        ("komor", "plglo"),
        ("plglo", "milgr"),
        ("plzac", "milgr"),
        ("plzac", "gmrad"),
        ("plglo", "gmjor"),
        ("gmjor", "gmrad"),
    ]

    def execute(self, r: TaskRuntime) -> None:
        self.db = r.db
        trip_ids = [cast(str, i[0]) for i in r.db.raw_execute("SELECT trip_id FROM trips")]
        with r.db.transaction():
            r.db.raw_execute_many(
                "UPDATE trips SET direction = ? WHERE trip_id = ?",
                MultipleDataErrors.catch_all(
                    "AssignDirectionIds",
                    map(self.assign_direction_id, trip_ids),
                ),
            )

    def assign_direction_id(self, trip_id: str) -> tuple[int, str]:
        stop_id_to_idx = {
            cast(str, i[0]): cast(int, i[1])
            for i in self.db.raw_execute(
                "SELECT stop_id, stop_sequence FROM stop_times WHERE trip_id = ?", (trip_id,)
            )
        }

        for stop_a, stop_b in self.OUTBOUND_DIRECTION:
            a_idx = stop_id_to_idx.get(stop_a)
            b_idx = stop_id_to_idx.get(stop_b)
            if a_idx is not None and b_idx is not None:
                return (0 if a_idx < b_idx else 1), trip_id

        stops = sorted(stop_id_to_idx, key=lambda stop_id: stop_id_to_idx[stop_id])
        raise DataError(f"can't detect direction of trip {trip_id} (stops: {stops})")


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
        # cspell: enable
    }

    def __init__(self) -> None:
        super().__init__()
        self.holidays = set[Date]()

    def execute(self, r: TaskRuntime) -> None:
        self.retrieve_holidays(r)
        with r.resources["wkd.xml"].open_text(encoding="utf-8") as f:
            tt = ET.parse(f).getroot()

        with r.db.transaction():
            r.db.raw_execute("UPDATE feed_info SET version = ?", (tt.attrib["created"],))
            for train in tt.findall("sitkol:train", self.NS):
                self.parse_train(r.db, train)

    def retrieve_holidays(self, r: TaskRuntime) -> None:
        self.holidays.clear()
        resource = r.resources["calendar_exceptions.csv"]
        region = polish_calendar_exceptions.PolishRegion.MAZOWIECKIE
        for date, details in polish_calendar_exceptions.load_exceptions(resource, region).items():
            if polish_calendar_exceptions.CalendarExceptionType.HOLIDAY in details.typ:
                self.holidays.add(date)

    def parse_train(self, db: DBConnection, train: ET.Element) -> None:
        route_id = train.findtext("sitkol:information/sitkol:symbol", "", self.NS).strip()
        if not route_id:
            raise DataError("<train> without a <symbol>")

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
                "wheelchair_accessible, bikes_allowed) VALUES (?, ?, ?, ?, 1, 1)"
            ),
            (id, route_id, id, number),
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
            stop_id = self.STOP_ID_LOOKUP[station.attrib["id"]]
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


class LoadStaticFiles(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.released() as db_path:
            load_gtfs(db_path, STATIC_FILES_DIR)


class GenerateShapes(Task):
    def __init__(self, osm_resource: str) -> None:
        super().__init__()
        self.osm_resource = osm_resource

        self._stop_id_to_node = dict[str, int]()
        self._stops_to_shape = dict[tuple[str, ...], int]()
        self._shape_id_counter = 0

    def clear(self) -> None:
        self._stop_id_to_node.clear()
        self._stops_to_shape.clear()
        self._shape_id_counter = 0

    def execute(self, r: TaskRuntime) -> None:
        self.clear()

        self.logger.debug("Loading OSM graph")
        with r.resources[self.osm_resource].open_binary() as f:
            graph = pyroutelib3.osm.Graph.from_file(pyroutelib3.osm.RailwayProfile(), f)

        self.logger.debug("Mapping stops to nodes")
        self._map_stops_to_nodes(graph, r.db)

        trip_ids = [cast(str, i[0]) for i in r.db.raw_execute("SELECT trip_id FROM trips")]
        with r.db.transaction():
            for trip_id in trip_ids:
                self._assign_shape(trip_id, r.db, graph)

    def _map_stops_to_nodes(self, graph: pyroutelib3.osm.Graph, db: DBConnection) -> None:
        kd = pyroutelib3.KDTree[pyroutelib3.osm.GraphNode].build(graph.nodes.values())
        assert kd is not None

        for stop_id, lat, lon in db.raw_execute("SELECT stop_id, lat, lon FROM stops"):
            stop_id = cast(str, stop_id)
            lat = cast(float, lat)
            lon = cast(float, lon)
            node = kd.find_nearest_neighbor((lat, lon))
            self._stop_id_to_node[stop_id] = node.id

    def _assign_shape(self, trip_id: str, db: DBConnection, graph: pyroutelib3.osm.Graph) -> None:
        stops = self._trip_stops(trip_id, db)
        shape_id = self._stops_to_shape.get(stops)
        if shape_id is None:
            shape_id = self._generate_shape(stops, db, graph)
        db.raw_execute("UPDATE trips SET shape_id = ? WHERE trip_id = ?", (shape_id, trip_id))

    def _generate_shape(
        self,
        stops: tuple[str, ...],
        db: DBConnection,
        graph: pyroutelib3.osm.Graph,
    ) -> int:
        shape_id = self._shape_id_counter
        self._shape_id_counter += 1
        self.logger.debug("Generating shape %d (%s)", shape_id, stops)

        nodes = (self._stop_id_to_node[stop_id] for stop_id in stops)
        node_pairs = pairwise(nodes)
        legs = (pyroutelib3.find_route(graph, a, b) for a, b in node_pairs)
        shape_nodes = self.remove_consecutive_runs(chain.from_iterable(legs))
        shape_pts = (graph.get_node(i).position for i in shape_nodes)

        db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (shape_id,))
        db.raw_execute_many(
            "INSERT INTO shape_points (shape_id, sequence, lat, lon) VALUES (?, ?, ?, ?)",
            ((shape_id, idx, lat, lon) for idx, (lat, lon) in enumerate(shape_pts)),
        )
        self._stops_to_shape[stops] = shape_id
        return shape_id

    @staticmethod
    def remove_consecutive_runs(it: Iterable[T]) -> Iterable[T]:
        return (k for k, _ in groupby(it))

    @staticmethod
    def _trip_stops(trip_id: str, db: DBConnection) -> tuple[str, ...]:
        return tuple(
            cast(str, i[0])
            for i in db.raw_execute(
                "SELECT stop_id FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence ASC",
                (trip_id,),
            )
        )


class SplitBusLegs(SplitTripLegs):
    def update_trip(self, trip: Trip, data: Any, db: DBConnection) -> None:
        if data:
            new_route_id = f"Z{trip.route_id}"
            trip.route_id = new_route_id
            trip.bikes_allowed = False


class WKDGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("-k", "--apikey", help="kolej-wkd.pl apikey")

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        apikey = self.resolve_apikey(args.apikey)
        return Pipeline(
            tasks=[
                LoadStaticFiles(),
                AddEntity(
                    entity=FeedInfo(
                        publisher_name="Mikołaj Kuranowski",
                        publisher_url="https://mkuran.pl/gtfs/",
                        lang="pl",
                        version="",
                    )
                ),
                LoadSchedules(),
                GenerateTripHeadsign(),
                AssignDirectionIds(),
                SplitBusLegs(),
                GenerateShapes("shapes.osm"),
                SaveGTFS(GTFS_HEADERS, "wkd.zip", ensure_order=True),
            ],
            resources={
                "wkd.xml": ZippedResource(
                    HTTPResource.get(f"http://www.kolej-wkd.pl/pliki/{apikey}/2024-2025/zip/")
                ),
                "shapes.osm": LocalResource("shapes.osm"),
                "calendar_exceptions.csv": polish_calendar_exceptions.RESOURCE,
            },
            options=options,
        )

    @staticmethod
    def resolve_apikey(arg: str | None) -> str:
        if arg:
            return arg
        elif env := os.getenv("WKD_APIKEY"):
            return env
        elif env_file := os.getenv("WKD_APIKEY_FILE"):
            with open(env_file, "r", encoding="ascii") as f:
                return f.read().strip()
        else:
            raise ValueError(
                "Missing kolej-wkd.pl apikey. Use the `-k` command line argument, "
                "`WKD_APIKEY` or `WKD_APIKEY_FILE` environment variables."
            )


if __name__ == "__main__":
    WKDGTFS().run()
