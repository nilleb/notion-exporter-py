import logging
import sys
import json
import unicodedata
from notion_client import NotionApiClient, format_id

from notion_exporter import NotionExportCrawler


class NotionTemplateApplier(NotionExportCrawler):
    def __init__(
        self,
        token,
        template_id,
        destination_parent_id,
        database_item_id,
        title=None,
        **kwargs,
    ) -> None:
        super().__init__(token, **kwargs, export_folder="dumps", root_pages=[])
        self.database_item_id = database_item_id
        self.destination_parent_id = destination_parent_id
        self.template_id = template_id
        self.title = title

    def apply(self):
        template_path = self.crawl_page(self.template_id)
        data_path = self.crawl_page(self.database_item_id)
        page = fill_template_with_data(
            template_path, data_path, self.destination_parent_id, self.title
        )

        with open(self.export_folder + "/future_page.json", "w") as fd:
            json.dump(page, fd)

        response = self.client.create_page(page)
        print(response.get("url"))


def remove_useless_properties_for_create(node):
    for prop in (
        "id",
        "parent",
        "created_time",
        "last_edited_time",
        "created_by",
        "last_edited_by",
        "url",
    ):
        if prop in node:
            del node[prop]

    block_type = node.get("type")
    children = node.get("children", [])
    children.extend(node.get(block_type, {}).get("children", []))

    for child in children:
        remove_useless_properties_for_create(child)


def date_property_value(value):
    computed = (
        "{start}->{end}".format(**value)
        if value.get("end")
        else "{start}".format(**value)
    )

    tz = value.get("time_zone")
    if tz:
        computed = f"{computed} ({tz})"

    return computed


def text_property_value(value):
    return "{plain_text}".format(**value[0])


def identity(value):
    return value


class Walker(object):
    def __init__(self, transform):
        self.transform = transform

    def walk_list(self, source):
        for value in source:
            if isinstance(value, dict):
                yield self.walk_dict(value)
            elif isinstance(value, str):
                result = self.transform(value)
                yield result
            else:
                yield value

    def walk_dict(self, source):
        for key, value in source.items():
            if isinstance(value, dict):
                self.walk_dict(value)
            elif isinstance(value, list):
                source[key] = list(self.walk_list(value))
            elif isinstance(value, str):
                result = self.transform(value)
                source[key] = result
            else:
                source[key] = value
        return source


def fill_template_with_data(template_path, data_path, parent_id, title):
    with open(template_path, encoding="utf-8") as fd:
        template = json.load(fd)
    with open(data_path, encoding="utf-8") as fd:
        data = json.load(fd)

    # FIXME drop all the children databases in the template

    remove_useless_properties_for_create(template)

    template["parent"] = {"type": "page_id", "page_id": format_id(parent_id)}

    template["properties"] = {
        "title": {
            "id": "title",
            "type": "title",
            "title": [
                {
                    "type": "text",
                    "text": {"content": title, "link": None},
                    "annotations": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": False,
                        "color": "default",
                    },
                    "plain_text": "Generated Page",
                    "href": None,
                }
            ],
        }
    }

    props = {}
    for name, prop in data.get("properties", {}).items():
        prop_type = prop.get("type")
        value = prop.get(prop_type)
        functions = {
            "date": date_property_value,
            "number": identity,
            "title": text_property_value,
            "rich_text": text_property_value,
        }
        func = functions.get(prop_type, repr)
        new_value = func(value)
        token = f"{{{{{name}}}}}"
        props[token] = str(new_value)

    def transform(val):
        if "{{" in val and "}}" in val:
            for token, new_value in props.items():
                utoken = unicodedata.normalize("NFC", token).replace("’", "'")
                uval = unicodedata.normalize("NFC", val).replace("’", "'")
                val = uval.replace(utoken, new_value)

        return val

    logging.info(props)

    Walker(transform).walk_dict(template)

    return template


def main():
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(filename="db_item_to_page.log", level=logging.DEBUG)

    job_desc_file = sys.argv[-1]
    with open(job_desc_file) as fp:
        job_desc = json.load(fp)

    applier = NotionTemplateApplier(
        **job_desc,
    )

    applier.apply()


def test():
    job_desc_file = sys.argv[-1]
    with open(job_desc_file) as fp:
        job_desc = json.load(fp)
    template = "dumps/invoice-template-8ead81d2-43f2-4bcd-bf7f-34b5091cea80.json"
    data = "dumps/2022-3-503d973c-0e1b-4582-a848-d15479099741.json"
    parent_id = job_desc.get("destination_parent_id")
    token = job_desc.get("token")
    title = job_desc.get("title", "Generated page")
    page = fill_template_with_data(template, data, parent_id, title)
    with open("dumps/future_page.json", "w") as fd:
        json.dump(page, fd)
    response = NotionApiClient(token).create_page(page)
    print(json.dumps(response))
    # FIXME: display the uri of the created page
    print(response.get("url"))


if __name__ == "__main__":
    main()
