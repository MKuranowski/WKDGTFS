# © Copyright 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import IO, NamedTuple, cast
from xml.sax import parse as sax_parse
from xml.sax.handler import ContentHandler as SAXContentHandler
from xml.sax.xmlreader import AttributesImpl as SAXAttributes

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors
from impuls.model import Route, Stop, Trip
from impuls.tools.types import StrPath


class StopKey(NamedTuple):
    station_id: str
    mode: Route.Type
    direction: Trip.Direction | None = None


class StopExtractor(SAXContentHandler):
    def __init__(self) -> None:
        super().__init__()
        self.stops = dict[StopKey, Stop]()
        self.in_node = False
        self.lat = 0.0
        self.lon = 0.0
        self.tags = dict[str, str]()

    def startElement(self, name: str, attrs: SAXAttributes[str]) -> None:
        if name == "node":
            self.in_node = True
            self.lat = round(float(attrs["lat"]), 7)
            self.lon = round(float(attrs["lon"]), 7)
        elif name == "tag" and self.in_node:
            self.tags[attrs["k"]] = attrs["v"]

    def endElement(self, name: str) -> None:
        if name == "node":
            if self.tags.get("railway") == "station":
                if "direction" in self.tags:
                    raise ValueError(f"direction tags not supported on railway=station nodes")

                id = self.tags["ref"]
                key = StopKey(id, Route.Type.RAIL)
                self.stops[key] = Stop(
                    id=id,
                    name=self.tags["name"],
                    lat=self.lat,
                    lon=self.lon,
                )

            elif self.tags.get("highway") == "bus_stop":
                base_id = self.tags["ref"]
                if d_str := self.tags.get("direction"):
                    direction = Trip.Direction(int(d_str))
                    id = f"{base_id}_bus_d{d_str}"
                else:
                    direction = None
                    id = f"{base_id}_bus"

                key = StopKey(base_id, Route.Type.BUS, direction)
                self.stops[key] = Stop(
                    id=id,
                    name=self.tags["name"],
                    code=self.tags.get("local_ref", ""),
                    lat=self.lat,
                    lon=self.lon,
                )

            self.in_node = False
            self.tags.clear()

    @classmethod
    def parse(cls, file: StrPath | IO[bytes]) -> dict[StopKey, Stop]:
        self = cls()
        sax_parse(file, self)
        return self.stops


class LoadStops(Task):
    def __init__(self, osm_resource: str) -> None:
        super().__init__()
        self.osm_resource = osm_resource

    def execute(self, r: TaskRuntime) -> None:
        # Load curated stops
        with r.resources[self.osm_resource].open_binary() as f:
            stops = StopExtractor.parse(f)

        # Curate stops
        to_curate = self.load_to_curate(r.db)
        with r.db.transaction():
            missing = self.apply_curation(r.db, to_curate, stops)

        # Raise an error on missing stops
        self.fail_on_missing(missing)

    def load_to_curate(self, db: DBConnection) -> defaultdict[StopKey, list[str]]:
        trips_by_stop = defaultdict[StopKey, list[str]](list)

        with db.raw_execute(
            "SELECT DISTINCT trip_id, stop_id, routes.type, trips.direction "
            "FROM stop_times "
            "INNER JOIN trips USING (trip_id) "
            "INNER JOIN routes USING (route_id)"
        ) as q:
            for r in q:
                trip_id = cast(str, r[0])
                stop_base_id = cast(str, r[1])
                mode = Route.Type(r[2])
                direction = Trip.Direction(r[3]) if r[3] is not None else None

                key = StopKey(stop_base_id, mode, direction)
                trips_by_stop[key].append(trip_id)

        return trips_by_stop

    def apply_curation(
        self,
        db: DBConnection,
        to_curate: Mapping[StopKey, Iterable[str]],
        stops: Mapping[StopKey, Stop],
    ) -> set[StopKey]:
        missing = set[StopKey]()
        existing_stops = {cast(str, i[0]) for i in db.raw_execute("SELECT stop_id FROM stops")}
        curated = set[str]()

        for key, trips in to_curate.items():
            stop = self.match_stop(key, stops)
            if not stop:
                missing.add(key)
                continue

            # Upsert stop data
            if stop.id in curated:
                pass
            elif stop.id in existing_stops:
                curated.add(stop.id)
                db.update(stop)
            else:
                curated.add(stop.id)
                db.create(stop)

            # Update stop_times when necessary
            if stop.id != key.station_id:
                db.raw_execute_many(
                    "UPDATE stop_times SET stop_id = ? WHERE trip_id = ? AND stop_id = ?",
                    ((stop.id, trip_id, key.station_id) for trip_id in trips),
                )

        return missing

    def match_stop(self, key: StopKey, stops: Mapping[StopKey, Stop]) -> Stop | None:
        # 1. Try direct match
        if s := stops.get(key):
            return s

        # 2. Try without direction
        if s := stops.get(key._replace(direction=None)):
            return s

        # 3. No match :^(
        return None

    def fail_on_missing(self, missing: Iterable[StopKey]) -> None:
        missing = sorted(missing)
        if missing:
            raise MultipleDataErrors(
                "LoadStops",
                [DataError(f"no data for {i}") for i in missing],
            )
