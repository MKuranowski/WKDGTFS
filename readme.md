# WKD GTFS

This project contains files used to generate [Warszawska Kolej Dojazdowa](https://wkd.com.pl)
GTFS and GTFS-Realtime files, as used on <https://mkuran.pl/gtfs/>.

## Static schedules

The feed is prepared semi-manually from the PDF schedules available on <https://wkd.com.pl>.

First, [Tabula](https://tabula.technology/) is used to extract tables those PDFs
into the CSV files. The extracted CSV files need to be cleaned in a spreadsheet editor
to the same format as current files in the schedules directory.

Note that trips which start after midnight need to have the hours set to `24:__`.
Thanks to automatic time-travel fixing this is only necessary for the very first stop-time of a trip.

Shapes are automatically generated using [pyroutelib3](https://github.com/MKuranowski/pyroutelib3/).

Every other file needs to be manually edited if that is required.

Then, the `static.py` script can be run and the trips.txt, stop_times.txt and shapes.txt
are automatically generated.

Also, as a side note, the `main` branch only contains the newest version of the GTFS feed,
which may only be valid from some time in the future.
**The full (possibly merged) dataset is only available on <https://mkuran.pl/gtfs/wkd.zip>**.
See the commit history for previous feed versions.

## Realtime data

The realtime script (rt/wkd_rt.go) scrapes the embedded realtime map
from <https://wkd.com.pl> into a GTFS-Realtime feed with trip updates.

It requires the static GTFS feed to run to provide accurate trip_ids.

## License

The following files in this repository are released into the public domain
under the [CC0 1.0 license](http://creativecommons.org/publicdomain/zero/1.0/):
- shapes.osm
- every file in the `gtfs` subdirectory
- every file in the `schedules` subdirectory

Everything else is released under the MIT license, available in the LICENSE file.

