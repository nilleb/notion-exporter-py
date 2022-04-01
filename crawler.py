from datetime import datetime
import json
import logging
import os
import shutil
from typing import Dict, List


class Crawler(object):
    EXPORT_FOLDER = "notion-export"

    def __init__(
        self, root_pages: List, export_folder: str = EXPORT_FOLDER, resume: bool = False
    ) -> None:
        self.export_folder = export_folder
        self._resume_buffer_and_visited(root_pages, resume)

        if not os.path.isdir(self.export_folder):
            os.makedirs(self.export_folder)

    def compute_buffer(self):
        raise NotImplementedError("Please Implement this method")

    def compute_visited(self):
        raise NotImplementedError("Please Implement this method")

    def _buffer_file_path(self):
        return f"{self.export_folder}/buffer.json"

    def _visited_file_path(self):
        return f"{self.export_folder}/visited.json"

    def _resume_buffer_and_visited(self, buffer: Dict, resume: bool):
        self.buffer = {}
        self.visited = {}

        if resume:
            try:
                with open(self._buffer_file_path()) as fd:
                    self.buffer = json.load(fd)
            except:
                self.compute_buffer()
            try:
                with open(self._visited_file_path()) as fd:
                    self.visited = dict(json.load(fd))
            except:
                self.compute_visited()

        if not self.buffer:
            self.buffer = buffer

        if isinstance(self.buffer, list):
            self.buffer = {item["id"]: item for item in self.buffer}

        if not self.visited:
            self.visited = {}

    def _persist_buffer_and_history(self):
        if self.buffer:
            path = self._buffer_file_path()

            if os.path.exists(path):
                shutil.copyfile(path, f"{path}.backup")

            with open(path, "w") as fd:
                json.dump(self.buffer, fd)

        path = self._visited_file_path()
        with open(path, "w") as fd:
            json.dump(self.visited, fd)

    def append_to_buffer(self, type: str, id: str, title: str=None, parent: str=None):
        self.buffer["id"] = {"type": type, "id": id, "title": title, "parent": parent}

    def crawl(self):
        items = self.buffer

        item = items.popitem() if items else None

        while item:
            _, item = item
            kind = item.pop("type")
            uid = item.pop("id")

            if uid not in self.visited:
                now = datetime.utcnow().isoformat()
                logging.info(f"{now} ({len(self.visited)}✅ {len(self.buffer)}▶️) crawl {kind} {uid}")
                getattr(self, f"crawl_{kind}")(uid, **item)
                self.visited[uid] = item

                self._persist_buffer_and_history()

            item = self.buffer.popitem() if self.buffer else None
