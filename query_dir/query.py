import time
import psycopg2
from variables import POSTGRES_DBNAME, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USERNAME


def base_insert(query: str):
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


def create_log(trx_hash: str, data: str, topics: list, log_index: int, address: str):
    if not topics:
        topics.append("")
    query = f"""
    INSERT INTO logs
    (trx_hash, data_log, topics, log_index, contract_addr)
    VALUES ('{trx_hash}', '{data}', ARRAY {topics}, {log_index}, '{address}')
    """
    base_insert(query)


def create_uncle(
    block_number: int, uncle_number: int, uncle_index: int, timestamp: int, reward: float, uncle_hash: str
):
    query = f"""
        INSERT INTO uncles_block
        (block_number, uncle_number, uncle_index, timestamp, block_reward, hash)
        VALUES
        ({block_number}, {uncle_number}, {uncle_index}, {timestamp}, {reward}, '{uncle_hash}')

        """
    base_insert(query)


def create_block(
    block_number: int,
    block_timestamp: int,
    block_hash: str,
    block_trx_count: int,
    base_fee_per_gas: int,
    gas_used: int,
    block_rew: float,
):
    query = f"""
        INSERT INTO blocks
        (block_number, block_timestamp, block_hash, block_trx_count, base_fee_per_gas, gas_used, block_reward)
        VALUES
        ({block_number}, {block_timestamp}, '{block_hash}', {block_trx_count}, {base_fee_per_gas},
        {gas_used}, {block_rew})

        """
    base_insert(query)


def create_trx(
    trx_hash: str,
    block_number: int,
    from_address: str,
    to_address: str,
    gas: float,
    gas_price: float,
    input_data: str,
    value: int,
    timestamp: int,
):
    query = f"""
    INSERT INTO transactions
    (trx_hash, block_number, from_address, to_address, gas, gas_price, input_data, value, timestamp)
    VALUES
    ('{trx_hash}', {block_number}, '{from_address}', '{to_address}', {gas}, {gas_price}, '{input_data}', {value},
    {timestamp})

    """
    base_insert(query)


def get_day_blocks(start: int, end: int):
    query = f"""
    SELECT block_number FROM blocks
    WHERE block_timestamp >= {start} and block_timestamp <= {end}
    """
    return sync_db_select(sql_query=query)


def sync_db_insert(sql_query: str) -> bool:
    conn = psycopg2.connect(
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DBNAME,
    )
    cur = conn.cursor()
    try:
        cur.execute(sql_query)
        res = True
        # print("Selecting rows from mobile table using cursor.fetchall")
    except (psycopg2.errors.UniqueViolation):
        res = True
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        res = False
    finally:
        conn.commit()
        cur.close()
        conn.close()
    return res


def sync_db_select(sql_query: str) -> list:
    conn = psycopg2.connect(
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DBNAME,
    )
    cur = conn.cursor()
    try:
        cur.execute(sql_query)
        res = cur.fetchall()
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        res = False
    finally:
        conn.commit()
        cur.close()
        conn.close()
    return res
