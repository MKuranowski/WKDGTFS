# © Copyright 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from impuls import Task, TaskRuntime
from impuls.model import Agency, FareAttribute, FeedInfo, Route

ENTITIES = [
    Agency(
        id="0",
        name="Warszawska Kolej Dojazdowa",
        url="https://wkd.com.pl/",
        lang="pl",
        timezone="Europe/Warsaw",
    ),
    FeedInfo(
        publisher_name="Mikołaj Kuranowski",
        publisher_url="https://mkuran.pl/gtfs/",
        lang="pl",
        version="",
    ),
    Route(
        id="A",
        agency_id="0",
        short_name="WKD",
        long_name="Warszawska Kolej Dojazdowa",
        type=Route.Type.RAIL,
        color="990099",
        text_color="FFFFFF",
    ),
    Route(
        id="Z",
        agency_id="0",
        short_name="WKD ZKA",
        long_name="Warszawska Kolej Dojazdowa - Zastępcza Komunikacja Autobusowa",
        type=Route.Type.BUS,
        color="990000",
        text_color="FFFFFF",
    ),
    FareAttribute(
        id="1",
        agency_id="0",
        price=4.1,
        currency_type="PLN",
        payment_method=FareAttribute.PaymentMethod.BEFORE_BOARDING,
        transfers=0,
        transfer_duration=19 * 60,
    ),
    FareAttribute(
        id="2",
        agency_id="0",
        price=6.3,
        currency_type="PLN",
        payment_method=FareAttribute.PaymentMethod.BEFORE_BOARDING,
        transfers=0,
        transfer_duration=38 * 60,
    ),
    FareAttribute(
        id="3",
        agency_id="0",
        price=9.2,
        currency_type="PLN",
        payment_method=FareAttribute.PaymentMethod.BEFORE_BOARDING,
        transfers=0,
        transfer_duration=None,
    ),
]


class AddStaticEntities(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            for entity in ENTITIES:
                r.db.create(entity)
