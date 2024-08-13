import re
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import cast

from impuls import App, LocalResource, PipelineOptions, Task
from impuls.errors import DataError, MultipleDataErrors
from impuls.model import Date
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, MultiFile
from impuls.tasks import LoadGTFS, SaveGTFS

DATE_RE = re.compile(r"(20[0-9][0-9])\W?([01][0-9])\W?([0-3][0-9])")

GTFS_HEADERS = {
    "agency": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_lang",
        "agency_timezone",
    ),
    "calendar_dates": ("service_id", "date", "exception_type"),
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


class StaticIntermediateFeedProvider(IntermediateFeedProvider[LocalResource]):
    def __init__(self, feeds: list[IntermediateFeed[LocalResource]]) -> None:
        self.feeds = feeds

    def needed(self) -> list[IntermediateFeed[LocalResource]]:
        return self.feeds


class Merger(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("-o", "--output", type=Path, help="target GTFS filename")
        parser.add_argument("files", nargs="+", type=Path, help="GTFS files to merge")

    def prepare(self, args: Namespace, options: PipelineOptions) -> MultiFile[LocalResource]:
        return MultiFile(
            options=options,
            intermediate_provider=StaticIntermediateFeedProvider(self._input_feeds(args.files)),
            intermediate_pipeline_tasks_factory=lambda feed: [
                cast(Task, LoadGTFS(feed.resource_name)),
            ],
            final_pipeline_tasks_factory=lambda _: [
                cast(Task, SaveGTFS(GTFS_HEADERS, self._output_path(args.files, args.output)))
            ],
        )

    def _input_feeds(self, inputs: list[Path]) -> list[IntermediateFeed[LocalResource]]:
        return MultipleDataErrors.catch_all(
            "parse command line arguments",
            map(self._input_feed, inputs),
        )

    def _input_feed(self, input: Path) -> IntermediateFeed[LocalResource]:
        date_match = self._file_version(input.name)
        if not date_match:
            raise DataError(f"failed to extract date from filename {input}")
        date = Date(int(date_match[1]), int(date_match[2]), int(date_match[3]))
        return IntermediateFeed(
            resource=LocalResource(input),
            resource_name=input.name,
            version=date_match[0],
            start_date=date,
        )

    def _output_path(self, inputs: list[Path], output: Path | None = None) -> Path:
        if output:
            return output

        versions = [self._file_version(i.stem) for i in inputs]
        dir = inputs[0].parent
        prefix = inputs[0].stem[: versions[0].start()]
        suffix = inputs[0].stem[versions[0].end() :]
        return dir / f"{prefix}{'-'.join(i[0] for i in versions)}{suffix}.zip"

    @staticmethod
    def _file_version(x: str) -> re.Match[str]:
        date_match = DATE_RE.search(x)
        if not date_match:
            raise DataError(f"failed to extract date from filename {x}")
        return date_match


if __name__ == "__main__":
    Merger().run()
