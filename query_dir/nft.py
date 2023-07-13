import json
from .base import make_insert, make_select


def create_nft_1155(
    token_id: int,
    path_json_schema: str,
    attributes: dict,
    image_url: str,
    name_collection: str,
    contract_addr: str,
    rarity: float = 0.0,
    image_path: str = "",
):
    query = f"""
            INSERT INTO nfts
            (token_id, rarity, json_schema_path, name_collection, attributes_nft, image_url, contract_address,
            image_path)
            VALUES
            ({token_id}, {rarity}, '{path_json_schema}', '{name_collection}', '{json.dumps(attributes)}', '{image_url}',
            '{contract_addr}', '{image_path}')
            """
    make_insert(query=query)


def check_exist_nft_1155(token_id: int, contract_address: str):
    query = f"""
        SELECT * FROM nfts
        WHERE token_id = {token_id} AND contract_address = '{contract_address}'
    """
    return bool(make_select(query=query))


def count_nft_in_collection(
    name_collection: str,
    contract_address: str,
) -> int:
    query = f"""
            SELECT count(*) FROM nfts
            WHERE name_collection = '{name_collection}' and contract_address = '{contract_address}'
        """
    res = make_select(query=query)
    return res[0][0]


def get_all_nft_attr(
    name_collection: str,
    contract_address: str,
) -> list:
    query = f"""
            SELECT token_id, attributes_nft FROM nfts
            WHERE name_collection = '{name_collection}' and contract_address = '{contract_address}'
        """
    return make_select(query=query)


def update_nft_rariry(token_id: int, attributes: dict, rarity: float, name_collection: str):

    query = f"""
    UPDATE nfts
    SET attributes_nft = '{json.dumps(attributes)}', rarity = {rarity}
    where token_id={token_id} and name_collection='{name_collection}'
    """
    make_insert(query=query)


def count_empty_image_path():
    query = """
               SELECT count(*) FROM nfts
               WHERE image_path is NULL or image_path = ''
            """
    res = make_select(query=query)
    return res[0][0]


def empty_image_path(offset_num: int):
    query = f"""
                            SELECT token_id, name_collection, image_url FROM nfts
                            WHERE image_path is NULL or image_path = ''
                            ORDER BY contract_address LIMIT 1000 OFFSET {offset_num}
                        """
    return make_select(query=query)


def update_image_path(token_id: int, name_coll: str, image_path_: str):
    query = f"""
           UPDATE nfts
           SET image_path = '{image_path_}'
           WHERE name_collection = '{name_coll}' and token_id = {token_id}
           """
    make_insert(query=query)


def get_image_collection(name_col: str, contr_addr: str):
    query = f"""
                SELECT image_path FROM nfts
                WHERE (image_path is not NULL or image_path != '') and name_collection = '{name_col}'
                and contract_address = '{contr_addr}' LIMIT 1
                """
    res = make_select(query=query)
    if res:
        return res[0][0]
    else:
        return None


def get_random_token_id_coll(name_col: str, contr_addr: str):
    query = f"""
                SELECT token_id FROM nfts
                WHERE name_collection = '{name_col}' and contract_address = '{contr_addr}' LIMIT 1
                """
    res = make_select(query=query)
    if res:
        return res[0][0]
    else:
        return None


def get_all_token_id_coll(name_col: str, contr_addr: str):
    query = f"""
                SELECT token_id FROM nfts
                WHERE name_collection = '{name_col}' and contract_address = '{contr_addr}'
                """
    return make_select(query=query)
