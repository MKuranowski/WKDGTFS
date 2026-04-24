# © Copyright 2024-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable
from itertools import chain, groupby, pairwise
from typing import TypeVar, cast

import routx
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Route

T = TypeVar("T")

PROFILES = {
    Route.Type.RAIL: routx.OsmProfile.RAILWAY,
    Route.Type.BUS: routx.OsmProfile.BUS,
}


class GenerateShapes(Task):
    def __init__(self, osm_resource: str) -> None:
        super().__init__()
        self.osm_resource = osm_resource

        self._stop_id_to_node = dict[str, int]()
        self._stops_to_shape = dict[tuple[str, ...], int]()
        self._shape_id_counter = 0

    def execute(self, r: TaskRuntime) -> None:
        self._shape_id_counter = 0

        for mode, profile in PROFILES.items():
            self.logger.info("Generating shapes for %s", mode)

            self.logger.debug("Loading OSM graph")
            graph_file = r.resources[self.osm_resource].stored_at
            graph = routx.Graph()
            graph.add_from_osm_file(graph_file, profile)

            self.logger.debug("Mapping stops to nodes")
            self._map_stops_to_nodes(graph, r.db)

            trip_ids = [
                cast(str, i[0])
                for i in r.db.raw_execute(
                    "SELECT trip_id FROM trips JOIN routes USING (route_id) WHERE routes.type = ?",
                    (mode.value,),
                )
            ]
            with r.db.transaction():
                self._stops_to_shape.clear()
                for trip_id in trip_ids:
                    self._assign_shape(trip_id, r.db, graph)

    def _map_stops_to_nodes(self, graph: routx.Graph, db: DBConnection) -> None:
        self._stop_id_to_node.clear()
        kd = routx.KDTree.build(graph)

        for stop_id, lat, lon in db.raw_execute("SELECT stop_id, lat, lon FROM stops"):
            stop_id = cast(str, stop_id)
            lat = cast(float, lat)
            lon = cast(float, lon)
            node = kd.find_nearest_node(lat, lon)
            self._stop_id_to_node[stop_id] = node.id

    def _assign_shape(self, trip_id: str, db: DBConnection, graph: routx.Graph) -> None:
        stops = self._trip_stops(trip_id, db)
        shape_id = self._stops_to_shape.get(stops)
        if shape_id is None:
            shape_id = self._generate_shape(stops, db, graph)
        db.raw_execute("UPDATE trips SET shape_id = ? WHERE trip_id = ?", (shape_id, trip_id))

    def _generate_shape(
        self,
        stops: tuple[str, ...],
        db: DBConnection,
        graph: routx.Graph,
    ) -> int:
        shape_id = self._shape_id_counter
        self._shape_id_counter += 1
        self.logger.debug("Generating shape %d (%s)", shape_id, stops)

        nodes = (self._stop_id_to_node[stop_id] for stop_id in stops)
        node_pairs = pairwise(nodes)
        legs = (graph.find_route(a, b, without_turn_around=False) for a, b in node_pairs)
        shape_nodes = self.remove_consecutive_runs(chain.from_iterable(legs))
        shape_pts = (((n := graph[i]).lat, n.lon) for i in shape_nodes)

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
