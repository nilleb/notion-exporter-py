# notion-exporter-py

Export a notion page and all its children items (deatabases, pages, ...) to json files

## Installation

`./setup.sh`

## Sample usage: create a new notion page from a database item, using a template

1. prepare a job description (typically, just update the database_item_id)

    ```json
    {
        "token": "secret_lalalala",
        "template_id": "8ead81d243f24bcdbf7f34b5091cea80",
        "destination_parent_id": "46e49e1c3d684ccd8658d8d80fb7ca0a",
        "database_item_id": "c6ec77174d7f472abe6a2e1dd30f6d94"
    }

    ```

2. `python ./notion_dbitem_to_page.py job_desc.json`

## Sample usage: generate invoices from a database

After having executed the "ccreate a new notion page from a database item, using a template"

`python notion_dbitem_to_invoices.py c6ec77174d7f472abe6a2e1dd30f6d94`
