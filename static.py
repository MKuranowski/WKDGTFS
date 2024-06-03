# Copyright (c) 2023 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import re
from contextlib import closing
from dataclasses import dataclass
from itertools import chain, groupby, pairwise, starmap
from pathlib import Path
from typing import Iterable, Sequence, TypeVar
from warnings import warn

import pyroutelib3

T = TypeVar("T")
Pt = tuple[float, float]


# Utility functions


def remove_consecutive_runs(it: Iterable[T]) -> Iterable[T]:
    """remove_consecutive_runs replaces consecutive runs of equal elements
    just by the first elements.

    >>> list(remove_consecutive_runs([1, 1, 2, 3, 3, 4, 1]))
    [1, 2, 3, 4, 1]
    """
    return (k for k, _ in groupby(it))


def transpose(it: Iterable[Iterable[T]]) -> Iterable[tuple[T, ...]]:
    """transpose returns a 2D iterable "flipped diagonally", aka. with rows swapped with columns.

    >>> list(transpose([[1, 2, 3], [4, 5, 6], [7, 8, 9]]))
    [(1, 4, 7), (2, 5, 8), (3, 6, 9)]
    """
    return zip(*it)


def time_from_str(x: str) -> int:
    """time_from_str tries to parse a HH:MM[:SS] string into a
    seconds-since-midnight time value.

    >>> time_from_str("9:15")
    33300
    >>> time_from_str("05:05:30")
    18330
    >>> time_from_str("27:46:40")
    100000
    """
    regex_match = re.match(r"([0-9]{1,2}):([0-9]{2})(?::([0-9]{2}))?", x)
    if not regex_match:
        raise ValueError(f"invalid time string: {x!r}")

    hours = int(regex_match[1])
    minutes = int(regex_match[2])
    seconds = int(regex_match[3]) if regex_match[3] else 0
    return hours * 3600 + minutes * 60 + seconds


def time_to_str(s: int) -> str:
    """time_to_str converts a seconds-since-midnight time value and
    converts it into a GTFS-compliant string.

    >>> time_to_str(33300)
    '09:15:00'
    >>> time_to_str(18330)
    '05:05:30'
    >>> time_to_str(100_000)
    '27:46:40'
    """
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h:0>2}:{m:0>2}:{s:0>2}"


# Schedule reading


@dataclass
class StopTime:
    stop: str
    arrival: int
    departure: int


@dataclass
class Trip:
    number: str
    route: str
    service: str
    times: list[StopTime]


def extract_trips_from_csv(f: Iterable[str]) -> Iterable[Trip]:
    columns = list(transpose(csv.reader(f)))
    stop_ids = columns[0][3:]

    for trip_column in columns[1:]:
        yield Trip(
            number=trip_column[0],
            route=trip_column[1],
            service=trip_column[2],
            times=extract_stop_times(stop_ids, trip_column[3:]),
        )


def extract_stop_times(stop_ids: Sequence[str], csv_times: Sequence[str]) -> list[StopTime]:
    stop_times: list[StopTime] = []

    for idx, (stop_id, csv_time) in enumerate(zip(stop_ids, csv_times, strict=True)):
        # Skip "times" which indicate that a vehicle does not stop
        csv_time = csv_time.strip()
        if csv_time in {"", "|", "<", ">"}:
            continue

        # Try to parse the time
        time = time_from_str(csv_time)

        # Ensure no time travel
        if stop_times and time < stop_times[-1].departure:
            time += 86400

        # Special handling for arrival and departure times spread across multiple rows
        if stop_id == "" and idx > 0:
            stop_id = stop_ids[idx - 1]

        # Ensure we have a stop_id
        if not stop_id:
            raise ValueError(f"missing stop_id for row: {idx + 4}")

        # If the latests StopTime has the same stop_id, only update its departure time
        if stop_times and stop_times[-1].stop == stop_id:
            stop_times[-1].departure = time
        else:
            stop_times.append(StopTime(stop_id, time, time))

    return stop_times


# GTFS generation

# List of pairs of stops in the outbound direction -
# if a trip stops at the first one before the second one,
# the direction_id should be "0", or "1" if it stops
# on the second one before the first.
OUTBOUND_DIRECTION: list[tuple[str, str]] = [
    ("wzach", "walje"),
    ("prusz", "komor"),
    ("prusp", "komor"),
    ("komor", "regul"),
    ("plglo", "milgr"),
    ("plzac", "milgr"),
    ("plglo", "gmjor"),
    ("gmjor", "gmrad"),
]


def detect_direction_id(stops: Sequence[str]) -> str:
    stop_indices = {stop: i for i, stop in enumerate(stops)}
    for left, right in OUTBOUND_DIRECTION:
        left_idx = stop_indices.get(left)
        right_idx = stop_indices.get(right)
        if left_idx is not None and right_idx is not None:
            return "0" if left_idx < right_idx else "1"
    raise ValueError(f"can't detect direction of stops:\n\t{stops}")


class Shaper:
    def __init__(self) -> None:
        self.router = pyroutelib3.Router("train", "shapes.osm")
        self.stop_to_node = {}

        self.cache: dict[tuple[str, ...], str] = {}
        self.shape_id_counter = 0

        self.file = open("gtfs/shapes.txt", mode="w", encoding="utf-8", newline="")
        self.writer = csv.writer(self.file)
        self.writer.writerow(("shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"))

    def close(self) -> None:
        self.file.close()

    def get_shape(self, stops: Sequence[str]) -> str:
        # Check if a shape for this sequence of stops was already generated
        stops_hash = tuple(stops)
        cached_shape_id = self.cache.get(stops_hash)
        if cached_shape_id is not None:
            return cached_shape_id

        # Generate a new shape under a new shape_id
        shape_id = str(self.shape_id_counter)
        self.shape_id_counter += 1

        shape_points = self.generate_shape(stops)

        # Save the shape to shapes.txt and save the shape_id in cached
        self.write_shape(shape_id, shape_points)
        self.cache[stops_hash] = shape_id
        return shape_id

    def write_shape(self, shape_id: str, points: Iterable[Pt]) -> None:
        for idx, (lat, lon) in enumerate(points):
            self.writer.writerow((shape_id, idx, lat, lon))

    def generate_shape(self, stops: Sequence[str]) -> Iterable[Pt]:
        stop_pairs = pairwise(stops)
        legs = starmap(self.generate_shape_for_pair, stop_pairs)
        all_nodes = chain.from_iterable(legs)
        unique_nodes = remove_consecutive_runs(all_nodes)
        positions = map(self.router.nodeLatLon, unique_nodes)
        return positions

    def generate_shape_for_pair(self, from_id: str, to_id: str) -> list[int]:
        from_node = self.stop_to_node[from_id]
        to_node = self.stop_to_node[to_id]
        status, nodes = self.router.doRoute(from_node, to_node)

        if status != "success":
            warn(f"No shape between {from_id} and {to_id}: {status}")
            nodes = [from_node, to_node]

        return nodes


class GTFSGenerator:
    def __init__(self) -> None:
        self.f_trips = open("gtfs/trips.txt", mode="w", encoding="utf-8", newline="")
        self.w_trips = csv.writer(self.f_trips)
        self.w_trips.writerow(
            (
                "route_id",
                "service_id",
                "trip_id",
                "trip_headsign",
                "trip_short_name",
                "direction_id",
                "shape_id",
                "wheelchair_accessible",
                "bikes_allowed",
            )
        )

        self.f_times = open("gtfs/stop_times.txt", mode="w", encoding="utf-8", newline="")
        self.w_times = csv.writer(self.f_times)
        self.w_times.writerow(
            (
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
            )
        )

        self.shaper = Shaper()
        self.seen_trip_ids: set[str] = set()
        self.stop_names: dict[str, str] = {}

    def load_stop_data(self) -> None:
        with open("gtfs/stops.txt", mode="r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                id = row["stop_id"]
                name = row["stop_name"]
                lat = float(row["stop_lat"])
                lon = float(row["stop_lon"])

                self.stop_names[id] = name
                self.shaper.stop_to_node[id] = self.shaper.router.findNode(lat, lon)

    def close(self) -> None:
        self.f_trips.close()
        self.f_times.close()
        self.shaper.close()

    def write_trip(self, t: Trip) -> None:
        trip_id = f"{t.service}-{t.number}"
        if trip_id in self.seen_trip_ids:
            raise ValueError(f"generated duplicate trip_id: {trip_id!r}")
        self.seen_trip_ids.add(trip_id)

        trip_stops = [i.stop for i in t.times]
        direction_id = detect_direction_id(trip_stops)
        shape_id = self.shaper.get_shape(trip_stops)
        headsign = self.stop_names[t.times[-1].stop]
        wheelchairs = "1"  # all trips are wheelchair-accessible
        bikes = "2" if t.route[0] == "Z" else "1"  # no bikes in rail repl. buses

        self.w_trips.writerow(
            (
                t.route,
                t.service,
                trip_id,
                headsign,
                t.number,
                direction_id,
                shape_id,
                wheelchairs,
                bikes,
            )
        )

        for seq, st in enumerate(t.times):
            self.w_times.writerow(
                (
                    trip_id,
                    time_to_str(st.arrival),
                    time_to_str(st.departure),
                    st.stop,
                    seq,
                )
            )

    def process_trips_from_file(self, f: Iterable[str]) -> None:
        for trip in extract_trips_from_csv(f):
            self.write_trip(trip)


if __name__ == "__main__":
    with closing(GTFSGenerator()) as generator:
        generator.load_stop_data()

        for timetable_file_path in Path("schedules").iterdir():
            # Skip non-csv files
            if not timetable_file_path.suffix == ".csv":
                continue

            print(timetable_file_path.name)
            with timetable_file_path.open(mode="r", encoding="utf-8-sig", newline="") as f:
                generator.process_trips_from_file(f)
