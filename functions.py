import copy

TABLE_TEMPLATE = {
    "object": "block",
    "has_children": True,
    "archived": False,
    "type": "table",
    "table": {
        "table_width": 5,
        "has_column_header": True,
        "has_row_header": False,
        "children": [
            {
                "object": "block",
                "has_children": False,
                "archived": False,
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [
                            {
                                "type": "text",
                                "text": {"content": "Name", "link": None},
                                "annotations": {
                                    "bold": False,
                                    "italic": False,
                                    "strikethrough": False,
                                    "underline": False,
                                    "code": False,
                                    "color": "default",
                                },
                                "plain_text": "Name",
                                "href": None,
                            }
                        ],
                        [
                            {
                                "type": "text",
                                "text": {"content": "Description", "link": None},
                                "annotations": {
                                    "bold": False,
                                    "italic": False,
                                    "strikethrough": False,
                                    "underline": False,
                                    "code": False,
                                    "color": "default",
                                },
                                "plain_text": "Description",
                                "href": None,
                            }
                        ],
                        [
                            {
                                "type": "text",
                                "text": {"content": "Unit Price", "link": None},
                                "annotations": {
                                    "bold": False,
                                    "italic": False,
                                    "strikethrough": False,
                                    "underline": False,
                                    "code": False,
                                    "color": "default",
                                },
                                "plain_text": "Unit Price",
                                "href": None,
                            }
                        ],
                        [
                            {
                                "type": "text",
                                "text": {"content": "Quantity", "link": None},
                                "annotations": {
                                    "bold": False,
                                    "italic": False,
                                    "strikethrough": False,
                                    "underline": False,
                                    "code": False,
                                    "color": "default",
                                },
                                "plain_text": "Quantity",
                                "href": None,
                            }
                        ],
                        [
                            {
                                "type": "text",
                                "text": {"content": "Total", "link": None},
                                "annotations": {
                                    "bold": False,
                                    "italic": False,
                                    "strikethrough": False,
                                    "underline": False,
                                    "code": False,
                                    "color": "default",
                                },
                                "plain_text": "Total",
                                "href": None,
                            }
                        ],
                    ]
                },
            },
        ],
    },
}

CELL_TEMPLATE = {
    "type": "text",
    "text": {"content": "FIXME", "link": None},
    "annotations": {
        "bold": False,
        "italic": False,
        "strikethrough": False,
        "underline": False,
        "code": False,
        "color": "default",
    },
    "plain_text": "FIXME",
    "href": None,
}

ROW_TEMPLATE = {
    "object": "block",
    "has_children": False,
    "archived": False,
    "type": "table_row",
    "table_row": {"cells": []},
}

ORDER = ["Name", "Description", "Price", "Quantity", "Total"]


def _eval_prop(prop, eval_property_value):
    value = eval_property_value(prop["type"], prop[prop["type"]])
    return value


def _build_prop_map(match, eval_property_value=None):
    return {
        name: eval_property_value(prop["type"], prop[prop["type"]])
        for name, prop in match.value["properties"].items()
    }


def _prepare_cell(value):
    cell = copy.deepcopy(CELL_TEMPLATE)
    cell["text"]["content"] = str(value)
    cell["plain_text"] = str(value)
    return cell


def _prepare_row(match, eval_property_value):
    row = copy.deepcopy(ROW_TEMPLATE)
    prop_map = _build_prop_map(match, eval_property_value)
    for column in ORDER:
        cell = _prepare_cell(prop_map.get(column, ""))
        row["table_row"]["cells"].append([cell])
    return row


def line_items(matches, eval_property_value=None):
    table = copy.deepcopy(TABLE_TEMPLATE)

    for match in matches:
        row = _prepare_row(match, eval_property_value)
        table["table"]["children"].append(row)

    return table
