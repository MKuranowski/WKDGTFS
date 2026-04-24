# © Copyright 2024-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

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
        "stop_code",
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
