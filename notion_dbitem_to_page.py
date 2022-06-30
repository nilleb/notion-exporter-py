import logging
import sys
import json

from notion_exporter import NotionExportCrawler


class NotionTemplateApplier(NotionExportCrawler):
    def __init__(
        self, token, template_id, destination_parent_id, database_item_id, **kwargs
    ) -> None:
        super().__init__(token, **kwargs, export_folder="dumps", root_pages=[])
        self.database_item_id = database_item_id
        self.destination_parent_id = destination_parent_id
        self.template_id = template_id

    def apply(self):
        template_path = self.crawl_page(self.template_id)
        data_path = self.crawl_page(self.database_item_id)
        page = fill_template_with_data(template_path, data_path)
        print(self.client.create_page(self.destination_parent_id, page))


def fill_template_with_data(template_path, data_path):
    with open(template_path) as fd:
        template = json.load(fd)
    with open(data_path) as fd:
        data = json.load(fd)
    return template


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(filename="db_item_to_page.log", level=logging.DEBUG)

    job_desc_file = sys.argv[-1]
    with open(job_desc_file) as fp:
        job_desc = json.load(fp)

    applier = NotionTemplateApplier(
        **job_desc,
    )

    applier.apply()
