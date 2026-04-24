# © Copyright 2024-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import os
from argparse import ArgumentParser, Namespace

from impuls import App, Pipeline, PipelineOptions
from impuls.model import FeedInfo
from impuls.resource import HTTPResource, LocalResource, ZippedResource
from impuls.tasks import AddEntity, ExecuteSQL, GenerateTripHeadsign, SaveGTFS
from impuls.tools import polish_calendar_exceptions
from impuls.tools.temporal import get_european_railway_schedule_revision

from .assign_direction_ids import AssignDirectionIds
from .generate_shapes import GenerateShapes
from .gtfs import GTFS_HEADERS
from .load_schedules import LoadSchedules
from .load_static_files import LoadStaticFiles
from .split_bus_legs import SplitBusLegs


class WKDGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("-k", "--apikey", help="kolej-wkd.pl apikey")

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        apikey = self.resolve_apikey(args.apikey)
        revision = get_european_railway_schedule_revision()
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
                ExecuteSQL(
                    task_name="RemoveFakeBusStopTimes",
                    statement=(
                        "DELETE FROM stop_times WHERE stop_id = 'malic' "
                        "AND (SELECT routes.type FROM trips JOIN routes USING (route_id) "
                        "     WHERE trips.trip_id = stop_times.trip_id) = 3"
                    ),
                ),
                GenerateShapes("shapes.osm"),
                SaveGTFS(GTFS_HEADERS, "wkd.zip", ensure_order=True),
            ],
            resources={
                "wkd.xml": ZippedResource(
                    HTTPResource.get(f"http://www.kolej-wkd.pl/pliki/{apikey}/{revision}/zip/")
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
