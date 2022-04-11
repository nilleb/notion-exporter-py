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

    def tear_down(self):
        raise NotImplementedError("Please Implement this method")

    def _relative_file_path(self, fp):
        return f"{self.export_folder}/{fp}"


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

    def append_to_buffer(
        self, type: str, uid: str, title: str = None, parent: str = None
    ):
        if uid in self.visited:
            return
        self.buffer[uid] = {"type": type, "id": uid, "title": title, "parent": parent}

    def append_to_visited(
        self, type: str, uid: str, title: str = None, parent: str = None
    ):
        if uid in self.visited:
            raise Exception(f"Visiting twice {uid}")
        self.visited[uid] = {"type": type, "id": uid, "title": title, "parent": parent}

    def crawl(self):
        items = self.buffer

        item = items.popitem() if items else None

        while item:
            _, item = item
            kind = item.pop("type")
            uid = item.pop("id")

            if uid not in self.visited:
                now = datetime.utcnow().isoformat()
                logging.info(
                    f"{now} ({len(self.visited)}✅ {len(self.buffer)}▶️) crawl {kind} {uid}"
                )
                try:
                    getattr(self, f"crawl_{kind}")(uid, **item)
                    self.visited[uid] = item
                except:
                    logging.exception("Unexpected exception caught: persisting buffer and visited.")
                    self._persist_buffer_and_history()
                    raise

            item = self.buffer.popitem() if self.buffer else None

        self.tear_down()
