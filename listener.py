import asyncio
import aio_pika
from web3 import Web3
from services.reward_calculating import get_uncle_inclusion_reward, get_block_reward
from query_dir.query import create_block
from variables import NAME_MAIN_QUEUE_TRX, POKT_URL, RABBIT_MQ_LOCAL  # RABBIT_MQ,
from loguru import logger
import json
from router import changing_types_data_trx

logger.add(
    "ethereum_blockchain/error_logs/errors_listener.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="200 MB",
    retention="30 days",
)

web3 = Web3(Web3.HTTPProvider(POKT_URL))


async def main() -> None:
    while True:
        try:
            connection = await aio_pika.connect_robust(
                RABBIT_MQ_LOCAL,
            )
            break
        except ConnectionError:
            print("Error connection broker listener")

    async with connection:
        channel = await connection.channel()

        queue = await channel.declare_queue(NAME_MAIN_QUEUE_TRX, durable=True)

        start_block_realtime = web3.eth.get_block_number()
        while True:
            latest_block_realtime = web3.eth.get_block_number()
            if start_block_realtime != latest_block_realtime:
                for number_block in range(start_block_realtime, latest_block_realtime):
                    print(f"Номер блока {number_block}")
                    block = web3.eth.getBlock(number_block, full_transactions=True)
                    timestamp = block["timestamp"]
                    txs_full = block["transactions"]
                    for tx in txs_full:
                        try:
                            tx.hash.hex()
                        except Exception as ex:
                            logger.info(f"Error trx_hash.hex():{ex}. Trx_hash: {tx.hash}")
                            continue
                        trx = changing_types_data_trx(tx)
                        put_queue = {"trx_full": trx, "timestamp": timestamp}
                        print(f"processing tx - {tx.hash.hex()}")

                        await channel.default_exchange.publish(
                            aio_pika.Message(body=json.dumps(put_queue).encode("utf-8")),
                            routing_key=queue.name,
                        )
                    try:
                        block_hash = block["hash"].hex()
                        block_trx_count = len(txs_full)
                        base_fee_per_gas = block.get("baseFeePerGas", 1)
                        gas_used = block.get("gasUsed", 0)
                        uncles = block.get("uncles", [])

                        block_reward = get_block_reward(w3=web3, block=block)
                        get_uncle_inclusion_reward(w3=web3, block_number=number_block, uncles=uncles)
                        create_block(
                            block_number=number_block,
                            block_timestamp=timestamp,
                            block_hash=block_hash,
                            block_trx_count=block_trx_count,
                            base_fee_per_gas=base_fee_per_gas,
                            gas_used=gas_used,
                            block_rew=block_reward,
                        )
                    except Exception as ex:
                        logger.info(f"Error write db block: {ex}. BlockNumber: {number_block}")

                start_block_realtime = latest_block_realtime


if __name__ == "__main__":
    asyncio.run(main())
