import json
import logging
import os
import sys
from glob import glob
from typing import Dict, List
from slugify import slugify

from notion_client import NotionApiClient, format_id
from crawler import Crawler


class NotionBaseCrawler(Crawler):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def _child_title(self, block):
        return block.get(block.get("type"), {}).get("title")

    def extract_next_page_to_visit(self, block):
        block_type = block.get("type", None)
        bid = block.get("id")

        if block_type == "child_page":
            self.append_to_buffer("page", bid, self._child_title(block))
        elif block_type == "child_database":
            self.append_to_buffer("database", bid, self._child_title(block))

        return block

    def _title_property(self, obj):
        properties = obj.get("properties", {})
        if "title" in properties:
            return {"prop_name": "title", "property_dict": properties.get("title", {})}

        for name, prop in properties.items():
            prop_type = prop.get("type")
            if prop_type == "title":
                return {"prop_name": name, "property_dict": prop}

        return {"prop_name": "missing", "property_dict": {"missing": "Default Title"}}

    def _object_title(self, property_dict, prop_name=None):
        title_parts = property_dict.get("title", [])
        return "-".join([part.get("plain_text") for part in title_parts])


class NotionExportCrawler(NotionBaseCrawler):
    def __init__(self, token, **kwargs) -> None:
        self.client = NotionApiClient(token)
        super().__init__(**kwargs)

    def compute_buffer(self):
        self.buffer = {}

    def compute_visited(self):
        self.visited = {}
        path_expr = self._relative_file_path("*.json")
        for filepath in glob(path_expr):
            uid = filepath[-41:-5]
            self.visited[uid] = filepath

    def dump(self, object_id, title, data):
        title = slugify(title)[:64] if title else None
        prefix = f"{title}-" if title else ""
        fp = self._relative_file_path(f"{prefix}{format_id(object_id)}.json")

        if os.path.exists(fp):
            with open(fp) as fd:
                backup = dict(json.load(fd))
                backup.update(data)
                data = backup

        with open(fp, "w") as fd:
            json.dump(data, fd)

        return fp

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
            children = block[block_type].get("children", [])
            children = self.process_single_block(
                block.get("id"), block.get("children", [])
            )
            block[block_type]["children"] = children

    def crawl_page(self, page_id, title=None, **kwargs):
        page = self.client.get_page(page_id)
        if page.get("archived"):
            logging.warning(f"The page {page.get('url')} is archived. Skipping.")
            return

        blocks = self.process_single_block(page_id)
        page["children"] = blocks
        title = title if title else self._object_title(**self._title_property(page))

        return self.dump(page_id, title, page)

    def crawl_database_item(self, item_id, title=None, **kwargs):
        blocks = self.process_single_block(item_id)
        return self.dump(item_id, title, {"blocks": blocks})

    def crawl_database(self, database_id, title=None, **kwargs):
        database = self.client.get_database(database_id)

        items = list(self.client.paginate_children_items(database_id))
        for item in items:
            self.append_to_buffer(
                "database_item",
                item.get("id"),
                self._object_title(**self._title_property(item)),
            )
        database["items"] = items

        title = title if title else self._object_title(database)
        return self.dump(database_id, title, database)


if __name__ == "__main__":
    # FIXME send DEBUG level messages to a file
    logging.getLogger().setLevel(logging.INFO)

    job_desc_file = sys.argv[-1]
    with open(job_desc_file) as fp:
        job_desc = json.load(fp)

    crawler = NotionExportCrawler(
        **job_desc,
    )

    crawler.crawl()
