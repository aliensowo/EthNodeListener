from .base import make_insert, make_select


def create_coin(contract_address: str, name: str, total_supply: float, decimals: int):
    query = f"""
            INSERT INTO coins
            (contract_address, name, total_supply, decimals)
            VALUES
            ('{contract_address}', '{name}', {total_supply}, {decimals})
            """
    make_insert(query=query)


def update_coin(contract_address: str, total_supply: float):
    query = f"""UPDATE coins
                SET total_supply = {total_supply}
                WHERE contract_address='{contract_address}'
            """
    make_insert(query=query)


def create_event_coin(trx_hash: str, log_index: int, contract_address: str, addr_from: str, addr_to: str, value: float):
    query = f"""
            INSERT INTO coin_logs
            (trx_hash, log_index, contract_address, addr_from, addr_to, value)
            VALUES
            ('{trx_hash}', {log_index}, '{contract_address}', '{addr_from}', '{addr_to}', {value})
            """
    make_insert(query=query)


def check_coin_db(contract_address: str):
    query = f"""
            SELECT * FROM coins
            WHERE contract_address = '{contract_address}'
            """
    return bool(make_select(query=query))
