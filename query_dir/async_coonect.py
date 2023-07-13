import asyncpg
from loguru import logger
from variables import POSTGRES_HOST


async def save_nft_async(nfts: list):
    conn = await asyncpg.connect(f"postgresql://eth:eth@{POSTGRES_HOST}:5432/eth_blockchain")
    query = """
                INSERT INTO nfts
                (token_id, name_collection, rarity, attributes_nft, image_url, json_schema_path, contract_address,
                image_path)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT DO NOTHING
             """
    try:
        await conn.executemany(query, nfts)
        print(f"Async save NFT in DB. Count: {len(nfts)}")
    except Exception as ex:
        logger.info(f"Error async request DB(INSERT). Error: {str(ex)}")
    finally:
        await conn.close()
