# © Copyright 2024-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from typing import Any

from impuls import DBConnection
from impuls.model import Trip
from impuls.tasks import SplitTripLegs


class SplitBusLegs(SplitTripLegs):
    def update_trip(self, trip: Trip, data: Any, db: DBConnection) -> None:
        if data:
            new_route_id = "Z"
            trip.route_id = new_route_id
            trip.bikes_allowed = False
