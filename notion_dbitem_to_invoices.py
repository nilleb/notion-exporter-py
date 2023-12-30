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


def generate_pdf(invoice_dragon_items, template="template2", output_fn="output2.pdf"):
    url = "https://invoice-dragon.vercel.app/api/json"
    data = {
        "template": template,
        "email": "hello@nilleb.com",
        "businessName": "nillebco",
        "formName": "Invoice",
        "rows": invoice_dragon_items,
        "logo": "https://media.licdn.com/dms/image/D4D0BAQF7gcaauOlIFQ/company-logo_100_100/0/1692942090951?e=1704326400&v=beta&t=enXpVsLOO1IkxqUM8AljVv4439UvwSYX2UgnQNOw0os",
    }
    response = requests.post(url, json=data)
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

print("line items in the invoice:")
print(json.dumps(invoice_dragon_items, indent=4))
for idx in range(5):
    generate_pdf(
        invoice_dragon_items,
        template=f'template{idx if idx else ""}',
        output_fn=f'output{idx if idx else ""}.pdf',
    )
