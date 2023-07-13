import asyncio
from web3.eth import AsyncEth
from variables import NAME_MAIN_QUEUE_TRX, POKT_URL, RABBIT_MQ_LOCAL  # RABBIT_MQ, OWN_NODE_URL,
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from web3 import Web3
import json
from celery_main.tasks_services import write_data_trx_db
from celery_main.tasks_1155 import check_erc_1155
from celery_main.tasks_20 import check_erc_20, check_coins
from celery_main.tasks_721 import check_erc_721
from loguru import logger

"""
    celery -A celery_main worker --loglevel=INFO -n scan_erc_1155 -Q erc-1155
    celery -A celery_main worker --loglevel=INFO -n scan_20 -Q erc-20
    celery -A celery_main worker --loglevel=INFO -n write_db_trx -Q write_db_trx
    celery -A celery_main worker --loglevel=INFO -n coins -Q coins
    celery -A celery_main worker --loglevel=INFO -n scan_erc_721 -Q erc-721
"""

logger.add(
    "ethereum_blockchain/error_logs/errors_router.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="200 MB",
    retention="30 days",
)

web3 = None


def changing_types_data_trx(data_trx: dict):
    new_data_trx = {
        "hash": data_trx["hash"].hex(),
        "to": data_trx["to"],
        "from": data_trx["from"],
        "blockNumber": data_trx["blockNumber"],
        "gas": data_trx["gas"],
        "gasPrice": data_trx["gasPrice"],
        "input": data_trx["input"],
        "value": data_trx["value"],
    }

    return new_data_trx


def changing_types_logs(logs_trx: list):
    for index, value in enumerate(logs_trx):
        logs_trx[index] = dict(value)
        logs_trx[index]["blockHash"] = logs_trx[index]["blockHash"].hex()
        logs_trx[index]["transactionHash"] = logs_trx[index]["transactionHash"].hex()
        logs_trx[index]["topics"] = [topic.hex() for topic in logs_trx[index]["topics"]]
    return logs_trx


async def distribution_message_filters(message: AbstractIncomingMessage):
    trx_full = json.loads(message.body.decode("ascii"))
    while True:
        try:
            trx_get_info = trx_full.get("trx_full")
            trx_hash = trx_get_info.get("hash")
            print(f"Processing trx -- {trx_hash}")
            timestamp_trx = trx_full.get("timestamp")
            trx_get_logs = await web3.eth.get_transaction_receipt(trx_hash)
            logs_trx = trx_get_logs["logs"]
            logs_trx = changing_types_logs(logs_trx)

            trx_to = trx_get_info.get("to")  # ChecksumAddress

            if trx_to and trx_get_logs["status"] == 1:
                write_data_trx_db.delay(trx_data=trx_get_info, trx_logs=logs_trx, timestamp=timestamp_trx)

                event_tx = [event["topics"][0] for event in logs_trx]

                check_erc_721.delay(trx_hash=trx_hash, trx_to=trx_to, trx_logs=logs_trx)
                check_erc_1155.delay(trx_hash=trx_hash, trx_to=trx_to, trx_logs=logs_trx, event_tx=event_tx)
                check_erc_20.delay(trx_hash=trx_hash, trx_to=trx_to, trx_logs=logs_trx)
                check_coins.delay(trx_hash=trx_hash, logs_trx=logs_trx)
            await message.ack()
            break
        except Exception as ex:
            logger.info(f"Error distribution_message_filters - {trx_full['trx_full']['hash']} -- {ex}")
            if f"Cannot connect to host {POKT_URL}" in str(ex):
                await asyncio.sleep(5)


async def main() -> None:
    global web3
    provider = Web3.AsyncHTTPProvider(endpoint_uri=POKT_URL)
    web3 = Web3(provider, modules={"eth": (AsyncEth,)}, middlewares=[])

    while True:
        try:
            connection = await aio_pika.connect_robust(
                RABBIT_MQ_LOCAL,
            )
            break
        except ConnectionError:
            print("Error connection broker router")

    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Maximum message count which will be processing at the same time.
        await channel.set_qos(prefetch_count=100)

        transactions_queue = await channel.declare_queue(NAME_MAIN_QUEUE_TRX, durable=True)
        await transactions_queue.consume(distribution_message_filters)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
