import json
import logging
import os
import shutil
import sys
from glob import glob
from typing import Dict, List
from slugify import slugify

from notion_client import NotionApiClient


class NotionCrawler(object):
    EXPORT_FOLDER = "notion-export"

    def __init__(
        self, token, root_pages, export_folder=EXPORT_FOLDER, resume=False
    ) -> None:
        self.client = NotionApiClient(token)
        self.export_folder = export_folder
        self._resume_buffer_and_visited(root_pages, resume)

        if not os.path.isdir(self.export_folder):
            os.makedirs(self.export_folder)

    def _buffer_file_path(self):
        return f"{self.export_folder}/buffer.json"

    def _visited_file_path(self):
        return f"{self.export_folder}/visited.json"

    def _compute_visited(self):
        self.visited = {}
        for filepath in glob(f"{self.export_folder}/*.json"):
            uid = filepath[-41:-5]
            self.visited[uid] = filepath

    def _resume_buffer_and_visited(self, buffer, resume):
        self.buffer = []
        self.visited = {}

        if resume:
            try:
                with open(self._buffer_file_path()) as fd:
                    self.buffer = list(json.load(fd))
            except:
                pass
            try:
                with open(self._visited_file_path()) as fd:
                    self.visited = dict(json.load(fd))
            except:
                self._compute_visited()

        if not self.buffer:
            self.buffer = buffer

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

    def _buffer_append(self, type, id, title):
        self.buffer.append({"type": type, "id": id, "title": title})

    def dump(self, object_id, title, data):
        title = slugify(title)[:64] if title else None
        prefix = f"{title}-" if title else ""
        with open(f"{self.export_folder}/{prefix}{object_id}.json", "w") as fd:
            json.dump(data, fd)

    def debug_block(self, block):
        values = {}
        for prop in ("type", "has_children", "archived"):
            values[prop] = block.get(prop)
        return values

    def process_single_block(self, block_id, children_blocks=None) -> List[Dict]:
        """
        Exhaustively (and recursively) return all the children blocks of the given block.
        Adds page and database blocks to the buffer.
        """
        children_blocks = children_blocks if children_blocks else []
        for block in self.client.paginate_children_blocks(block_id):
            self.extract_next_page_to_visit(block)
            self.extract_children_blocks(block)
            children_blocks.append(block)
        return children_blocks

    def extract_children_blocks(self, block):
        block_type = block.get("type", None)
        has_children = block.get("has_children")

        if has_children and block_type not in ("child_page", "child_database"):
            block["children"] = []
            self.process_single_block(block.get("id"), block["children"])

    def _child_title(self, block):
        return block.get(block.get("type"), {}).get("title")

    def extract_next_page_to_visit(self, block):
        block_type = block.get("type", None)
        bid = block.get("id")

        if block_type == "child_page":
            self._buffer_append("page", bid, self._child_title(block))

        if block_type == "child_database":
            self._buffer_append("database", bid, self._child_title(block))

        return block

    def _title_property(self, obj):
        properties = obj.get("properties", {})
        if "title" in properties:
            return {"prop_name": "title", "property_dict": properties.get("title", {})}
        for name, prop in properties.items():
            prop_type = prop.get("type")
            if prop_type == "title":
                return {"prop_name": name, "property_dict": prop}

    def _object_title(self, property_dict, prop_name=None):
        title_parts = property_dict.get("title", [])
        return "-".join([part.get("plain_text") for part in title_parts])

    def crawl_page(self, page_id, title=None):
        logging.info(f"crawling page {page_id}")

        page = self.client.get_page(page_id)
        if page.get("archived"):
            logging.warning(f"The page {page.get('url')} is archived. Skipping.")
            return

        blocks = self.process_single_block(page_id)
        page["blocks"] = blocks
        title = title if title else self._object_title(**self._title_property(page))

        self.dump(page_id, title, page)

    def crawl_database_item(self, item_id, title=None):
        logging.info(f"crawling db item {item_id}")
        blocks = self.process_single_block(item_id)
        self.dump(item_id, title, {"blocks": blocks})

    def crawl_database(self, database_id, title=None):
        logging.info(f"crawling db {database_id}")

        database = self.client.get_database(database_id)

        items = list(self.client.paginate_children_items(database_id))
        for item in items:
            self._buffer_append(
                "database_item",
                item.get("id"),
                self._object_title(**self._title_property(item)),
            )
        database["items"] = items

        title = title if title else self._object_title(database)
        self.dump(database_id, title, database)

    def crawl(self):
        items = self.buffer

        item = items.pop(0) if items else None

        while item:
            kind = item.get("type")
            uid = item.get("id")
            title = item.get("title")

            if uid not in self.visited:
                if kind == "page":
                    self.crawl_page(uid, title=title)
                elif kind == "database":
                    self.crawl_database(uid, title=title)
                elif kind == "database_item":
                    self.crawl_database_item(uid, title=title)

                self.visited[uid] = item

            self._persist_buffer_and_history()

            item = self.buffer.pop(0) if self.buffer else None


if __name__ == "__main__":
    # FIXME send DEBUG level messages to a file
    logging.getLogger().setLevel(logging.INFO)

    job_desc_file = sys.argv[-1]
    with open(job_desc_file) as fp:
        job_desc = json.load(fp)

    crawler = NotionCrawler(
        **job_desc,
    )

    crawler.crawl()
