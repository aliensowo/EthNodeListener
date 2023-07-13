import json
from .base import make_insert, make_select


def create_collection(
    name: str,
    contract_addr: str,
    erc: str,
    description: str = "",
    total_supply_nft: int = 0,
    all_attributes: dict = {},
    release_date: int = 0,
    current_total_supply: int = 0,
    owner_count: int = 0,
):
    query = f"""
                INSERT INTO collections
                (name,contract_address, description, total_supply_nft , all_attributes, current_total_supply, erc,
                 release_date, owner_count)
                VALUES
                ('{name}', '{contract_addr}', '{description}', {total_supply_nft}, '{json.dumps(all_attributes)}',
                 {current_total_supply}, '{erc}', {release_date}, {owner_count})
                """
    make_insert(query=query)


def get_all_collection() -> list:
    query = """
                SELECT name, contract_address, current_total_supply FROM collections
            """
    return make_select(query=query)


def update_attributes_collection(name: str, contract_addr: str, attributes: dict):
    query = f"""
       UPDATE collections
       SET all_attributes = '{json.dumps(attributes)}'
       where name='{name}' and contract_address = '{contract_addr}'
       """
    make_insert(query=query)


def update_curr_ts_collection(name: str, contract_addr: str, current_total_supply: int):
    query = f"""
       UPDATE collections
       SET current_total_supply = {current_total_supply}
       WHERE name='{name}' AND contract_address = '{contract_addr}'
       """
    make_insert(query=query)


def get_name_collection(contract_address: str, token_id: int):
    query = f"""
                SELECT name_collection FROM nfts
                WHERE token_id = {token_id} and contract_address = '{contract_address}'
                LIMIT 1
            """
    res = make_select(query=query)
    if res:
        return res[0][0]
    else:
        return ""


def check_exist_collection_with_contract_db(contract_address: str):
    query = f"""
                    SELECT * FROM collections
                    WHERE contract_address = '{contract_address}'
                    LIMIT 1
                """
    return bool(make_select(query=query))


def add_trash_collection(contract_addr: str, name_col: str = ""):
    query = f"""
                    INSERT INTO trash_collections
                    (contract_address, name_collection)
                    VALUES
                    ('{contract_addr}', '{name_col}')
                    """
    make_insert(query=query)


def update_collection_open_sea(
    contract_addr: str,
    name_collection: str,
    total_supply: int,
    owner_count: int,
    market_cap: float,
    avg_price: float,
    sales: int,
    volume: float,
    floor_price: float,
):
    query = f"""
       UPDATE collections
       SET total_supply_nft = {total_supply}, owner_count = {owner_count},
       market_cap = {market_cap}, avg_price = {avg_price}, sales = {sales}, volume = {volume},
       floor_price = {floor_price}
       WHERE contract_address = '{contract_addr}' and name = '{name_collection}'
       """
    make_insert(query=query)


def update_collection_cap_avg(contract_addr: str, name_collection: str, market_cap: float, avg_price: float):
    query = f"""
       UPDATE collections
       SET market_cap = {market_cap}, avg_price = {avg_price}
       WHERE contract_address = '{contract_addr}' and name = '{name_collection}'
       """
    make_insert(query=query)


def update_collection_sales_vol_floor(
    contract_addr: str, name_collection: str, sales: int, volume: float, floor_price: float
):
    query = f"""
       UPDATE collections
       SET sales = {sales}, volume = {volume}, floor_price = {floor_price}
       WHERE contract_address = '{contract_addr}' and name = '{name_collection}'
       """
    make_insert(query=query)


def get_coll_empty_image():
    query = """
                    SELECT name, contract_address FROM collections
                    WHERE image_collection is NULL or image_collection = ''
                """
    return make_select(query=query)


def update_image_collection(name: str, contract_addr: str, image_path: str):
    query = f"""
       UPDATE collections
       SET image_collection = '{image_path}'
       WHERE name='{name}' and contract_address = '{contract_addr}'
       """
    make_insert(query=query)


def update_owners_count(name: str, contract_addr: str, owners_count: int):
    query = f"""
       UPDATE collections
       SET owner_count = {owners_count}
       WHERE name='{name}' and contract_address = '{contract_addr}'
       """
    make_insert(query=query)
