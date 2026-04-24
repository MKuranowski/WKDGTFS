# © Copyright 2024-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable
from itertools import chain, groupby, pairwise
from typing import TypeVar, cast

import pyroutelib3
from impuls import DBConnection, Task, TaskRuntime

T = TypeVar("T")


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
