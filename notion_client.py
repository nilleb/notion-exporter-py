from datetime import datetime, timedelta
import logging
from time import sleep
import requests

VERSION = "2022-02-22"


def logged(prefix):
    def decorate(f):
        def wrapper(*args, **kwargs):
            logging.debug(f"{prefix} {f.__name__} args {args} kwargs {kwargs}")
            cr = f(*args, **kwargs)
            logging.debug(f"{prefix} {f.__name__} result {cr}")
            return cr

        return wrapper

    return decorate


def format_id(item_id):
    steps = [8, 4, 4, 4, 12]
    steps.reverse()
    counter = steps.pop()
    output = []
    for character in item_id:
        if "-" == character:
            return item_id
        if counter == 0:
            counter = steps.pop()
            output.append("-")
        counter -= 1
        output.append(character)

    return "".join(output)


class NotionApiClient(object):
    BASE_URL = "https://api.notion.com/v1"

    def __init__(self, token) -> None:
        super().__init__()
        self.token = token
        self.last_call = datetime.now()

    def default_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": VERSION,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @logged("wrapper")
    def _call_api(self, path, method="POST", payload_dict=None):
        delta = timedelta(seconds=0.33)
        how_long = self.last_call + delta - datetime.now()
        if how_long.microseconds > 0:
            sleep(how_long.microseconds/1000000)

        try:
            return requests.request(
                method,
                f"{self.BASE_URL}/{path}",
                headers=self.default_headers(),
                json=payload_dict,
                timeout=30,
            ).json()
        except:
            logging.exception(f"Unexpected exception caught while {method}ing {path} with {payload_dict}")
            return {}
        finally:
            self.last_call = datetime.now()

    def list_database_items(
        self, database_id, filter=None, sort_order=None, start_cursor=None
    ):
        data = self.prepare_list_database_items_payload(
            filter, sort_order, start_cursor
        )
        return self._call_api(f"databases/{database_id}/query", payload_dict=data)

    def get_database(self, database_id):
        return self._call_api(f"databases/{database_id}", method="GET")

    def patch_page_property(self, page_id, property_name, value):
        data = {"properties": {property_name: value}}
        return self._call_api(f"pages/{page_id}", method="PATCH", payload_dict=data)

    def list_databases(self):
        return self._call_api("databases", method="GET")

    def list_databases_ids(self):
        for db in self.list_databases().get("results", []):
            if db.get("object") == "database":
                yield db.get("id")

    def prepare_list_database_items_payload(self, filter, sort_order, start_cursor):
        result = dict()
        if filter:
            result.update(filter)
        if sort_order:
            result.update(sort_order)
        if start_cursor:
            result["start_cursor"] = start_cursor
        return result

    def get_database(self, database_id):
        return self._call_api(f"databases/{database_id}", method="GET")

    def list_database_properties(self, database_id):
        response = self.get_database(database_id)
        for name, value in response.get("properties").items():
            kind = value.get("type")
            info = value.get(kind)
            yield (name, kind, info)

    def get_user(self, user_id):
        return self._call_api(f"users/{user_id}", method="GET")

    def get_page(self, page_id):
        return self._call_api(f"pages/{page_id}", method="GET")

    def create_page(self, page):
        return self._call_api(f"pages", payload_dict=page)

    def retrieve_children_blocks(self, block_id, page_size=100, start_cursor=None):
        url = f"blocks/{block_id}/children?page_size={page_size}"
        if start_cursor:
            url += f"&start_cursor={start_cursor}"
        return self._call_api(url, method="GET")

    def _paginate(self, object_id, fun):
        has_more, start_cursor = True, None
        while has_more:
            response = fun(format_id(object_id), start_cursor=start_cursor)
            blocks = response.get("results", [])
            for block in blocks:
                yield block
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor", None)

    def paginate_children_blocks(self, page_id):
        for item in self._paginate(page_id, self.retrieve_children_blocks):
            yield item

    def paginate_children_items(self, page_id):
        for item in self._paginate(page_id, self.list_database_items):
            yield item


def sample():
    client = NotionApiClient("secret_secret")
    database_ids = list(client.list_databases_ids())
    print(database_ids)
    db_id = next(iter(database_ids))
    print(list(client.list_database_properties(db_id)))
    print(list(client.list_database_items(db_id)))
    print(
        client.patch_page_property(
            "bcc79545-a953-4c9f-b96f-3d5cf64365a6",
            "Interval",
            {"date": {"start": "2021-05-30", "end": "2021-06-17"}},
        )
    )
