import time
from .query import sync_db_insert, sync_db_select


def make_insert(query: str):
    errors_count = 0
    while True:
        if errors_count == 5:
            return
        try:
            if sync_db_insert(sql_query=query):
                return True
            else:
                errors_count += 1
                time.sleep(5)
        except Exception:
            errors_count += 1
            time.sleep(5)


def make_select(query: str):
    errors_count = 0
    while True:
        if errors_count == 5:
            return
        try:
            res = sync_db_select(sql_query=query)
            if res:
                return res
            elif isinstance(res, list) and len(res) == 0:
                return []
            else:
                errors_count += 1
                time.sleep(5)
        except Exception:
            errors_count += 1
            time.sleep(5)
