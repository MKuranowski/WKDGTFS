# © Copyright 2024-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Sequence
from typing import Final, cast

from impuls import Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors


class AssignDirectionIds(Task):
    # List of pairs of stops in the outbound direction -
    # if a trip stops at the first one before the second one,
    # the direction_id should be "0", or "1" if it stops
    # on the second one before the first.
    OUTBOUND_DIRECTION: Final[Sequence[tuple[str, str]]] = [
        ("wzach", "walje"),
        ("prusz", "komor"),
        ("prpkp", "komor"),
        ("komor", "regul"),
        ("komor", "plglo"),
        ("plglo", "plzac"),
        ("plglo", "milgr"),
        ("plzac", "milgr"),
        ("plzac", "gmrad"),
        ("gmjor", "gmrad"),
        ("prpkp", "nwwar"),
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
