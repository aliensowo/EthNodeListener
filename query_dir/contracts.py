from .base import make_insert, make_select


def create_contract(address: str, abi_path: str = "", bytecode_path: str = ""):
    query = f"""
        INSERT INTO contracts
        (contract_address, abi_path, bytecode_path)
        VALUES
        ('{address}', '{abi_path}', '{bytecode_path}')

        """
    make_insert(query=query)


def check_exist_contract(address: str):
    query = f"""
        SELECT * FROM contracts
        WHERE contract_address = '{address}'
    """

    return bool(make_select(query=query))


def update_contract(address: str, abi_path: str):
    query = f"""
    UPDATE contracts
    SET abi_path = '{abi_path}'
    where contract_address='{address}'
    """
    make_insert(query=query)


def abi_contract(address: str):
    query = f"""
        SELECT abi_path FROM contracts
        WHERE contract_address = '{address}'
    """
    res = make_select(query=query)
    if not res:
        return None
    elif res and res[0][0] is not None:
        return res[0][0]
    else:
        return None
