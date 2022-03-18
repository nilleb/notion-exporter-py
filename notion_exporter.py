import json
import logging
import os
import sys

from notion_client import NotionApiClient


class NotionCrawler(object):
    EXPORT_FOLDER = "notion-export"

    def __init__(
        self, token, root_pages, export_folder=EXPORT_FOLDER, resume=False
    ) -> None:
        self.client = NotionApiClient(token)
        self.export_folder = export_folder
        self._resume_buffer(root_pages, resume)

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

    def process_single_block(self, page_id, children_blocks=None):
        children_blocks = children_blocks if children_blocks else []
        for block in self.client.paginate_children_blocks(page_id):
            self.extract_next_page_to_visit(block)
            self.extract_children_blocks(block)
            children_blocks.append(block)
        return children_blocks

    def extract_children_blocks(self, block):
        if block.get("has_children"):
            block["children"] = []
            self.process_single_block(block.get("id"), block["children"])

    def extract_next_page_to_visit(self, block):
        block_type = block.get("type", None)
        bid = block.get("id")
        title = block.get("title", None)

        if block_type == "child_page":
            self.buffer.append({"page": bid, "title": title})

        if block_type == "child_database":
            self.buffer.append({"database": bid, "title": title})

        return block

    def _title_property(self, obj):
        return obj.get("properties", {}).get("title", {})

    def _object_title(self, obj):
        title_parts = obj.get("title", [])
        return "-".join([part.get("plain_text") for part in title_parts])

    def crawl_page(self, page_id):
        page = self.client.get_page(page_id)
        blocks = self.process_single_block(page_id)
        page["blocks"] = blocks
        title = self._object_title(self._title_property(page))

        self.dump(page_id, title, page)

    def crawl_database(self, database_id):
        database = self.client.get_database(database_id)

        # should we dump a file for every database item?
        items = list(self.client.paginate_children_items(database_id))
        for item in items:
            item["blocks"] = self.process_single_block(item.get("id"))
        database["items"] = items

        title = self._object_title(database)
        self.dump(database_id, title, database)

    def crawl(self):
        items = self.buffer

        item = items.pop(0) if items else None

        while item:
            if item.get("page"):
                self.crawl_page(item.get("page"))
            else:
                self.crawl_database(item.get("database"))

            self._persist_buffer()

            item = self.buffer.pop(0) if self.buffer else None


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    job_desc_file = sys.argv[-1]
    with open(job_desc_file) as fp:
        job_desc = json.load(fp)

    crawler = NotionCrawler(
        **job_desc,
    )

    crawler.crawl()
