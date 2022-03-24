import json
import logging
import os
import sys
from typing import Dict, List

from notion_client import NotionApiClient


class NotionCrawler(object):
    EXPORT_FOLDER = "notion-export"

    def __init__(
        self, token, root_pages, export_folder=EXPORT_FOLDER, resume=False
    ) -> None:
        self.client = NotionApiClient(token)
        self.export_folder = export_folder
        self._resume_buffer(root_pages, resume)

        if not os.path.isdir(self.export_folder):
            os.makedirs(self.export_folder)

    def _buffer_file_path(self):
        return f"{self.export_folder}/buffer.json"

    def _resume_buffer(self, buffer, resume):
        self.buffer = buffer

        if resume:
            try:
                with open(self._buffer_file_path()) as fd:
                    self.buffer = json.load(fd)
            except:
                pass

        if not self.buffer:
            self.buffer = buffer

    def _persist_buffer(self):
        path = self._buffer_file_path()
        with open(path, "w") as fd:
            json.dump(self.buffer, fd)

        if not self.buffer and os.path.isfile(path):
            os.unlink(path)

    def dump(self, object_id, title, data):
        with open(f"{self.export_folder}/{title}-{object_id}.json", "w") as fd:
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

    def extract_next_page_to_visit(self, block):
        block_type = block.get("type", None)
        bid = block.get("id")
        title = block.get("title", None)

        if block_type == "child_page":
            self.buffer.append({"type": "page", "id": bid, "title": title})

        if block_type == "child_database":
            self.buffer.append({"type": "database", "id": bid, "title": title})

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
        if not prop_name:
            prop_name = "title"
        title_parts = property_dict.get(prop_name, [])
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
            self.buffer.append({"type": "database_item", "id": item.get("id")})
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

            if kind == "page":
                self.crawl_page(uid, title=title)
            elif kind == "database":
                self.crawl_database(uid, title=title)
            elif kind == "database_item":
                self.crawl_database_item(uid, title=title)

            self._persist_buffer()

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
