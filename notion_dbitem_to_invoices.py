import json
import sys

import requests

from notion_dbitem_to_page import discover_notion_docs, read_data_recursively
from notion_exporter import NotionExportCrawler


def get_value(property):
    property_type = property["type"]
    property_value = property[property_type]
    if isinstance(property_value, dict) and "type" in property_value:
        return property_value[property_value["type"]]
    elif property_type == "select":
        return property_value["name"]
    elif property_type == "text":
        return property_value["content"]
    elif isinstance(property_value, list):
        return " ".join([get_value(p) for p in property_value])
    else:
        return property_value


def convert_notion_to_json(data):
    items = []

    for child in data["children"]:
        if child["type"] == "child_database":
            for item in child["database"]["items"]:
                current = {}
                for pname, pvalue in item["properties"].items():
                    current[pname] = get_value(pvalue)
                items.append(current)

    return items


def convert_json_to_invoice_dragon(items):
    invoice_dragon_items = []

    for idx, item in enumerate(items):
        invoice_dragon_items.append(
            {
                "id": idx,
                "quantity": item["Quantity"],
                "amount": item["Total"],
                "description": item["Name"],
                "details": item["Description"],
                "rate": item["Price"],
            }
        )

    return invoice_dragon_items


def generate_pdf(
    invoice_dragon_items, template="template2", output_fn="output2.pdf", **kwargs
):
    url = "https://invoice-dragon.vercel.app/api/json"
    data = {
        "template": template,
        "email": "hello@nilleb.com",
        "businessName": "nillebco",
        "formName": "Invoice",
        "rows": invoice_dragon_items,
        "logo": "https://avatars.githubusercontent.com/u/108630435?s=400&u=8599aa94ae4bf40efd10bae56c0542e1a9009814&v=4",
    }
    data.update(kwargs)
    response = requests.post(url, json=data)
    if response.status_code != 200:
        response.raise_for_status()

    ## save response body to pdf file
    with open(output_fn, "wb") as f:
        f.write(response.content)


with open("private/api_key.txt") as f:
    token = f.read().strip()

database_item_id = sys.argv[1]

crawler = NotionExportCrawler(token, export_folder="dumps", root_pages=[])
data_path = crawler.crawl_page(database_item_id)

db = discover_notion_docs(data_path)
data = read_data_recursively(data_path, db)

items = convert_notion_to_json(data)
invoice_dragon_items = convert_json_to_invoice_dragon(items)

notes = get_value(data["properties"]["Notes"])

for idx in range(4):
    print(f"Generating invoice {idx + 1}...")
    generate_pdf(
        invoice_dragon_items,
        template=f"template{idx + 1}",
        output_fn=f"output{idx + 1}.pdf",
        notes=notes,
    )
