# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import re
from argparse import ArgumentParser, Namespace
from glob import glob
from itertools import chain, groupby, pairwise
from os.path import basename
from pathlib import Path
from typing import Final, Iterable, Sequence, TypeVar, cast

import pyroutelib3
from impuls import (
    App,
    DBConnection,
    LocalResource,
    Pipeline,
    PipelineOptions,
    Task,
    TaskRuntime,
)
from impuls.errors import DataError, MultipleDataErrors
from impuls.extern import load_gtfs
from impuls.model import FeedInfo, StopTime, TimePoint
from impuls.tasks import AddEntity, GenerateTripHeadsign, SaveGTFS

GTFS_HEADERS = {
    "agency": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_lang",
        "agency_timezone",
    ),
    "calendar": (
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ),
    "fare_attributes": (
        "agency_id",
        "fare_id",
        "price",
        "currency_type",
        "payment_method",
        "transfers",
        "transfer_duration",
    ),
    "feed_info": (
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_version",
    ),
    "routes": (
        "agency_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "stops": (
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "wheelchair_boarding",
    ),
    "trips": (
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
    "stop_times": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
    ),
    "shapes": ("shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"),
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
    def __init__(self, schedule_csv_resources: Iterable[str]) -> None:
        super().__init__()
        self.schedule_csv_resources = schedule_csv_resources

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            for resource in self.schedule_csv_resources:
                with r.resources[resource].open_text(encoding="utf-8-sig", newline="") as f:
                    self.load_trips_from_file(f, r.db)

    def load_trips_from_file(self, f: Iterable[str], db: DBConnection) -> None:
        columns = list(self.transpose(csv.reader(f)))
        stop_ids = columns[0][3:]
        for column in columns[1:]:
            self.load_trip(column, stop_ids, db)

    def load_trip(self, column: Sequence[str], stop_ids: Sequence[str], db: DBConnection) -> None:
        short_name = column[0]
        route_id = column[1]
        calendar_id = column[2]
        trip_id = f"{calendar_id}-{short_name}"
        db.raw_execute(
            "INSERT INTO trips (trip_id, route_id, calendar_id, short_name, wheelchair_accessible, "
            "bikes_allowed) VALUES (?,?,?,?,1,?)",
            (trip_id, route_id, calendar_id, short_name, 0 if route_id.startswith("Z") else 1),
        )
        db.create_many(StopTime, self.parse_stop_times(column[3:], stop_ids, trip_id))

    def parse_stop_times(
        self,
        times: Sequence[str],
        stop_ids: Sequence[str],
        trip_id: str,
    ) -> list[StopTime]:
        stop_times = list[StopTime]()

        for idx, (stop_id, time_str) in enumerate(zip(stop_ids, times, strict=True)):
            # Try to parse the time
            time = self.parse_time(time_str)
            if time is None:
                continue

            # Ensure no time travel
            if stop_times and time < stop_times[-1].departure_time:
                time = TimePoint(days=1, seconds=time.total_seconds())

            # Special handling for arrival and departure times spread across multiple rows
            if stop_id == "" and idx > 0:
                stop_id = stop_ids[idx - 1]

            # Ensure we have a stop_id
            if not stop_id:
                raise ValueError(f"missing stop_id for row: {idx + 4}")

            # If the latests StopTime has the same stop_id, only update its departure time
            if stop_times and stop_times[-1].stop_id == stop_id:
                stop_times[-1].departure_time = time
            else:
                stop_times.append(StopTime(trip_id, stop_id, idx, time, time))

        return stop_times

    @staticmethod
    def transpose(it: Iterable[Iterable[T]]) -> Iterable[tuple[T, ...]]:
        return zip(*it)

    @staticmethod
    def parse_time(x: str) -> TimePoint | None:
        x = x.strip()
        if x in {"", "|", "<", ">"}:
            return None

        regex_match = re.match(r"([0-9]{1,2}):([0-9]{2})(?::([0-9]{2}))?", x)
        if not regex_match:
            raise ValueError(f"invalid time string: {x!r}")

        hours = int(regex_match[1])
        minutes = int(regex_match[2])
        seconds = int(regex_match[3]) if regex_match[3] else 0
        return TimePoint(hours=hours, minutes=minutes, seconds=seconds)


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


class WKDGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("feed_version")

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        schedule_resources = {
            basename(filename): LocalResource(filename) for filename in glob("schedules/*.csv")
        }

        return Pipeline(
            tasks=[
                LoadStaticFiles(),
                AddEntity(
                    entity=FeedInfo(
                        publisher_name="Mikołaj Kuranowski",
                        publisher_url="https://mkuran.pl/gtfs/",
                        lang="pl",
                        version=args.feed_version,
                    )
                ),
                LoadSchedules(schedule_resources.keys()),
                GenerateTripHeadsign(),
                AssignDirectionIds(),
                GenerateShapes("shapes.osm"),
                SaveGTFS(GTFS_HEADERS, f"wkd-{args.feed_version.replace("-", "")}.zip"),
            ],
            resources={
                **schedule_resources,
                "shapes.osm": LocalResource("shapes.osm"),
            },
            options=options,
        )


if __name__ == "__main__":
    WKDGTFS().run()
