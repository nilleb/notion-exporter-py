import json
import logging
import os
import sys
import unicodedata
from glob import glob

from jsonpath_ng.ext import parse

import functions
from notion_client import NotionApiClient, format_id
from notion_exporter import NotionExportCrawler, document_title


class NotionTemplateApplier(NotionExportCrawler):
    def __init__(
        self,
        token,
        template_id,
        destination_parent_id,
        database_item_id,
        **kwargs,
    ) -> None:
        super().__init__(token, **kwargs, export_folder="dumps", root_pages=[])
        self.database_item_id = database_item_id
        self.destination_parent_id = destination_parent_id
        self.template_id = template_id

    def apply(self):
        data_path = self.crawl_page(self.database_item_id)
        template_path = self.crawl_page(self.template_id)
        self.crawl()
        data = json.load(open(data_path))
        title = document_title(data)
        page = fill_template_with_data(
            template_path, data_path, self.destination_parent_id, title
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
    return "{plain_text}".format(**next(iter(value), {"plain_text": ""}))


def identity(value):
    return value


def array_property_value(value):
    return ", ".join(
        [eval_value(val["type"], val[val["type"]]) for val in value["array"]]
    )


def rollup_property_value(value):
    return eval_value(value["type"], value)


def simple_property_value(value):
    return value[value["type"]]


def relation_property_value(value):
    if value:
        return simple_property_value(value[0])


def select_property_value(value):
    return value["name"]


def eval_value(prop_type, value):
    functions = {
        "date": date_property_value,
        "number": identity,
        "title": text_property_value,
        "rich_text": text_property_value,
        "rollup": rollup_property_value,
        "array": array_property_value,
        "relation": relation_property_value,
        "formula": simple_property_value,
        "select": select_property_value,
    }
    func = functions.get(prop_type, repr)
    try:
        new_value = func(value)
    except (KeyError, TypeError):
        logging.exception(
            f"Unexpected exception evaluating '{value}' of type '{prop_type}'"
        )
        new_value = value
    return new_value


class Walker(object):
    def __init__(self, transform):
        self.transform = transform
        self.current_block = None
        self.transformed_blocks = []

    def replace(self, value):
        return self.transformed_blocks.append((self.current_block, value))

    def walk_list(self, source):
        for value in source:
            if isinstance(value, dict):
                yield self.walk_dict(value)
            elif isinstance(value, str):
                result = self.transform(value, replace=self.replace)
                yield result
            else:
                yield value

    def walk_dict(self, source):
        if source.get("object") == "block":
            self.current_block = source

        for key, value in source.items():
            if isinstance(value, dict):
                self.walk_dict(value)
            elif isinstance(value, list):
                source[key] = list(self.walk_list(value))
            elif isinstance(value, str):
                result = self.transform(value, replace=self.replace)
                source[key] = result
            else:
                source[key] = value

        return source

    def update_block(self, original, processed):
        original.clear()
        original.update(processed)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for original, processed in self.transformed_blocks:
            self.update_block(original, processed)


def discover_notion_docs(data_path):
    folder = os.path.dirname(os.path.abspath(data_path))
    files = glob(f"{folder}/*.json")

    db = {}
    for fp in files:
        object_title = fp.replace(".json", "").split("/")[-1]
        uid = object_title[-36:]
        if len(uid) == 36:
            db[uid] = fp

    return db


def read_data_recursively(data_path, db):
    with open(data_path, encoding="utf-8") as fd:
        data = json.load(fd)

    for child in data.get("children", []):
        if child.get("type") == "child_database":
            filename = db[child["id"]]
            child["database"] = read_data_recursively(filename, db)

    if data.get("items"):
        items = []
        for item in data.get("items", []):
            filename = db[item]
            items.append(read_data_recursively(filename, db))
        data["items"] = items

    return data


def fill_template_with_data(template_path, data_path, parent_id, title):
    with open(template_path, encoding="utf-8") as fd:
        template = json.load(fd)

    db = discover_notion_docs(data_path)
    data = read_data_recursively(data_path, db)
    json.dump(data, open("dumps/data.json", "w"), indent=2)

    return _fill_template_with_data(template, data, parent_id, title)


def _fill_template_with_data(template, data, parent_id, title):
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
        new_value = eval_value(prop_type, value)
        token = f"{{{{{name}}}}}"
        props[token] = str(new_value)

    def transform(val, replace):
        if "{{" in val and "}}" in val:
            uval = unicodedata.normalize("NFC", val).replace("’", "'")

            if "(" in uval and ")" in uval:
                fun_name = uval.split("(")[0].split("{{")[1]
                fun = getattr(functions, fun_name, None)
                if fun:
                    expr = uval.split("('")[1].split(")'")[0]
                    jsonpath_expression = parse(expr)
                    matches = [match for match in jsonpath_expression.find(data)]
                    new_block = fun(matches, eval_property_value=eval_value)
                    replace(new_block)

            for token, new_value in props.items():
                utoken = unicodedata.normalize("NFC", token).replace("’", "'")
                uval = unicodedata.normalize("NFC", val).replace("’", "'")
                val = uval.replace(utoken, new_value)

        return val

    with Walker(transform) as walker:
        walker.walk_dict(template)

    template.pop("request_id", None)

    return template


def main():
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(filename="db_item_to_page.log", level=logging.DEBUG)

    job_desc_file = sys.argv[-1]
    with open(job_desc_file) as fp:
        job_desc = json.load(fp)

    applier = NotionTemplateApplier(**job_desc)
    applier.apply()


def test():
    job_desc_file = sys.argv[-1]
    with open(job_desc_file) as fp:
        job_desc = json.load(fp)
    template = "dumps/invoice-template-8ead81d2-43f2-4bcd-bf7f-34b5091cea80.json"
    data = "dumps/2023-09-29-f73e6c75-d34dc45f-f6dc-4288-89eb-cd7cb0ea0dad.json"
    parent_id = job_desc.get("destination_parent_id")
    page = fill_template_with_data(template, data, parent_id, "test")
    with open("dumps/future_page.json", "w") as fd:
        json.dump(page, fd)
    client = NotionApiClient(job_desc.get("token"))
    response = client.create_page(page)
    print(response)


if __name__ == "__main__":
    main()
