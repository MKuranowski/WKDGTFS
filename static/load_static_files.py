# © Copyright 2024-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from pathlib import Path

from impuls import Task, TaskRuntime
from impuls.extern import load_gtfs

STATIC_FILES_DIR = Path(__file__).parent.with_name("static_files")


class LoadStaticFiles(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.released() as db_path:
            load_gtfs(db_path, STATIC_FILES_DIR)
