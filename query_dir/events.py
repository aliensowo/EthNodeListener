from .base import make_insert, make_select


def add_nft_log_20(
    trx_hash: str,
    log_index: int,
    contract_addr: str,
    event_name: str,
    addr_from: str,
    addr_to: str,
    token_id: list,
    name_marketplace: str,
    price_eth: float,
):
    query = f"""
            INSERT INTO nft_logs
            (trx_hash, log_index , contract_addr, event_name, addr_from, addr_to, token_id,
             name_marketplace, price_eth)
            VALUES
            ('{trx_hash}', {log_index}, '{contract_addr}', '{event_name}', '{addr_from}', '{addr_to}',\
             ARRAY {token_id}, '{name_marketplace}', {price_eth})
            """
    make_insert(query=query)


def add_nft_log_721_1155(
    trx_hash: str,
    log_index: int,
    contract_addr: str,
    event_name: str,
    addr_from: str,
    addr_to: str,
    token_id: list,
    name_exchange_nft: str,
):
    query = f"""
            INSERT INTO nft_logs
            (trx_hash, log_index , contract_addr, event_name, addr_from, addr_to, token_id, name_marketplace)
            VALUES
            ('{trx_hash}', {log_index}, '{contract_addr}', '{event_name}', '{addr_from}', '{addr_to}',\
             ARRAY {token_id}, '{name_exchange_nft}')
            """
    make_insert(query=query)


def get_order_matched(trx_hash: str, log_index: int):
    query = f"""
            SELECT price_eth FROM nft_logs
            WHERE trx_hash = '{trx_hash}' and log_index = {log_index+1}
        """
    res = make_select(query=query)
    if res and res[0][0] is not None:
        return float(res[0][0])
    else:
        return 0.0


def find_log_in_trx_price(trx_hash: str, token_id: int):
    query = f"""
            SELECT price_eth FROM nft_logs
            WHERE trx_hash = '{trx_hash}' and price_eth != 0 and array_position(token_id, '{token_id}') is not null
        """
    res = make_select(query=query)
    if res:
        return float(res[0][0])
    else:
        return 0


def current_floor_price(name: str, contract_address: str):

    query = f"""
            SELECT floor_price FROM collections
            WHERE name = '{name}' and contract_address = '{contract_address}'
        """
    res = make_select(query=query)
    if res and res[0][0] is not None:
        return res[0][0]
    else:
        return 0.0


def get_nft_log_with_contract_query(contract_address: str) -> list:
    query = f"""
                SELECT nft_logs.trx_hash, nft_logs.log_index, nft_logs.token_id  FROM
                transactions JOIN nft_logs ON transactions.trx_hash = nft_logs.trx_hash
                WHERE nft_logs.contract_addr = '{contract_address}'
                ORDER BY transactions.block_number
            """
    return make_select(query=query)


def get_all_nft_log_with_block(number_block: int) -> list:
    query = f"""
                SELECT f.trx_hash, nft_logs.log_index, nft_logs.contract_addr, nft_logs.token_id, nft_logs.price_eth
                FROM (
                      SELECT blocks.block_number, transactions.trx_hash  FROM blocks JOIN transactions
                      ON blocks.block_number = transactions.block_number
                      ) as f
                      JOIN nft_logs ON f.trx_hash = nft_logs.trx_hash
                WHERE f.block_number = {number_block} AND price_eth is NULL;
            """
    return make_select(query=query)
