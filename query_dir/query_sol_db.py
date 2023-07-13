import psycopg2
from variables import (
    POSTGRES_USERNAME_DB_SOL,
    POSTGRES_PASSWORD_DB_SOL,
    POSTGRES_HOST_DB_SOL,
    POSTGRES_PORT_DB_SOL,
    POSTGRES_DBNAME_DB_SOL,
)


def get_data_download_service(url: str):
    query = f"""
                SELECT content_id, file_extension, status FROM files
                WHERE url = '{url}'
             """
    return bool(sync_db_select(sql_query=query))


def sync_db_select(sql_query: str) -> list:
    conn = psycopg2.connect(
        user=POSTGRES_USERNAME_DB_SOL,
        password=POSTGRES_PASSWORD_DB_SOL,
        host=POSTGRES_HOST_DB_SOL,
        port=POSTGRES_PORT_DB_SOL,
        database=POSTGRES_DBNAME_DB_SOL,
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
