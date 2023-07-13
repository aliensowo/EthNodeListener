from typing import List
from celery_main.celery import app
from web3 import Web3
from query_dir.events import add_nft_log_721_1155
from query_dir.collections import check_exist_collection_with_contract_db, create_collection, add_trash_collection
from erc_1155.main_func_1155 import get_abi
from variables import (
    Transfer_erc_721,
    # OWN_NODE_URL,
    contracts_marketplace,
    possible_supply_count_names,
    possible_release_data_names,
    NULL_ADDRESS,
    POKT_URL,
)
from erc_721.scanner_erc_721 import (
    BaseTask,
    data_topics_log,
    get_token_url_by_token_id,
    parse_nft_json_batch,
    save_jsons,
    save_pictures,
    find_method_in_contract,
    convert_to_nft_model,
)
from query_dir.contracts import create_contract
from query_dir.async_coonect import save_nft_async
import json
from queue import Queue
import threading
import asyncio
from loguru import logger

logger.add(
    "ethereum_blockchain/error_logs/errors_erc_721.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="200 MB",
    retention="30 days",
)

web3 = Web3(Web3.HTTPProvider(POKT_URL))

"""Tasks erc-721 NFT"""


@app.task(ignore_result=True)  # проверка транзакции на событие ERC-721
def check_erc_721(trx_hash: str, trx_to: str, trx_logs: list):
    current_contract_addr = None
    try:
        for log in trx_logs:
            if log["topics"][0] == Transfer_erc_721 and len(log["topics"]) == 4:
                print(f"Find event Transfer ERC-721 | trx_hash: {trx_hash}")
                log_index, contract_address, sender, recipient, id_token = data_topics_log(trx_log=log)

                if contract_address == current_contract_addr:
                    continue
                else:
                    current_contract_addr = contract_address

                if (id_token and id_token > 2**32) or not contract_address:  # проверить условие
                    continue

                if sender == NULL_ADDRESS:
                    event_name = "Transfer ERC-721 (MINT)"
                elif recipient == NULL_ADDRESS:
                    event_name = "Transfer ERC-721 (BURN)"
                else:
                    event_name = "Transfer ERC-721"

                tokens = [id_token]

                if contracts_marketplace.get(contract_address):
                    exchange_name = contracts_marketplace.get(contract_address)
                else:
                    exchange_name = contracts_marketplace.get(trx_to.lower())

                add_nft_log_721_1155(  # запись лога в БД
                    trx_hash=str(trx_hash),
                    log_index=int(log_index),
                    contract_addr=contract_address,
                    event_name=event_name,
                    addr_from=sender,
                    addr_to=recipient,
                    token_id=tokens,
                    name_exchange_nft=exchange_name,
                )
                if not check_exist_collection_with_contract_db(contract_address=contract_address):
                    #  проверяем есть ли коллекция с там контрактом уже в БД
                    parsing_collection_erc_721.delay(contract_address=contract_address)
                    #  если нет, то запускаем парсер инфы по коллекции
    except Exception as ex:
        logger.info(f"Error check_erc_721: {str(ex)}")


@app.task(ignore_result=True)  # таск парсера информации по контракту
def parsing_collection_erc_721(contract_address: str):
    abi = get_abi(contract_address=contract_address, erc="721")
    contract = web3.eth.contract(address=web3.toChecksumAddress(contract_address), abi=abi)
    if isinstance(abi, str):
        abi = json.loads(abi)

    try:
        contract_functions = [function.get("name") for function in abi if function.get("name")]
        if "name" not in contract_functions:
            print("This contract has no name function... Skipping...")
            # there write trash collection in separate table !!!!!!
            add_trash_collection(contract_addr=contract_address)
            return None
        collection_name = contract.functions.name().call()
    except Exception as ex:
        logger.info(f"No methods name contract abi. Error: {ex}")
        # there write trash collection in separate table !!!!!!
        add_trash_collection(contract_addr=contract_address)
        return None

    collection_token_count = find_method_in_contract(contract, contract_functions, possible_supply_count_names)

    release_date = find_method_in_contract(contract, contract_functions, possible_release_data_names)
    if not release_date:
        release_date = 0

    if not collection_token_count:
        print("Could not find token count... Skipping...")
        add_trash_collection(contract_addr=contract_address, name_col=collection_name)
        return None

    print(
        f"Parsing NFT collection. Name_collection: {collection_name} | TotalSupply: {collection_token_count}"
        f"Contract address: {contract_address}"
    )

    create_contract(
        address=contract_address,
        abi_path=f"ethereum_blockchain/abi_contract/{contract_address}.txt",
    )

    create_collection(  # запись коллекции
        name=collection_name,
        contract_addr=contract_address,
        description="",
        total_supply_nft=collection_token_count,
        all_attributes={},
        release_date=release_date,
        erc="ERC-721",
    )
    # получение всех URL на json из контракта коллекции
    get_urls_721.delay(
        contract_address=contract_address,
        contract_abi=abi,
        collection_token_count=collection_token_count,
        collection_name=collection_name,
    )


@app.task(ignore_result=True)  # получение всех URL на json из контракта коллекции
def get_urls_721(contract_address: str, contract_abi: list, collection_token_count: int, collection_name: str):
    contract = web3.eth.contract(address=web3.toChecksumAddress(contract_address), abi=contract_abi)
    # сбор из abi названия всех методов
    contract_functions = [function.get("name") for function in contract_abi if function.get("name")]

    token_url_queue = Queue()  # очередь для сбора urls с 50 потоков
    threads = []  # список потоков
    semaphore = threading.Semaphore(value=50)
    threads_count = 50  # кол-во потоков в одном таске (процессе)

    for token_batch_index in range(0, collection_token_count + 1, threads_count):
        current_threads_count = (
            threads_count
            if token_batch_index + threads_count < collection_token_count
            else collection_token_count + 1 - token_batch_index
        )
        # print(f"Creating {current_threads_count} threads.")
        for token_index in range(token_batch_index, token_batch_index + current_threads_count):
            # Create a thread for every index to get urls concurrently
            thread = threading.Thread(
                target=get_token_url_by_token_id,
                args=(contract, token_index, contract_functions, token_url_queue, semaphore),
            )
            threads.append(thread)
            thread.start()

        for index, thread in enumerate(threads):  # ждем завершения 50 потоков в этом таске
            thread.join()

        # отправляем собранную очередь на получение метаданных из json
        get_nft_metadata.delay(list(token_url_queue.queue), collection_name, contract_address)

        token_url_queue = Queue()  # обнуляем очередь и список потоков для следующих новых 50 потоков
        threads = []


@app.task(base=BaseTask, ignore_result=True)  # получение метаданных из urls собранных через tokenURI
def get_nft_metadata(token_urls: List[dict], collection_name: str, contract_address: str):
    # создаем event loop для асинхронного отправления запросов на сервер для получения json
    loop = asyncio.get_event_loop()
    # запускаем в event_loop асинхронную функцию пакетного сбора jsons
    nfts = loop.run_until_complete(parse_nft_json_batch(get_nft_metadata.http_client, token_urls))

    nfts = [nft for nft in nfts if nft]  # список не пустых json токенов
    if not nfts:
        add_trash_collection(contract_addr=contract_address, name_col=collection_name)
        return None

    save_nft_metadata.delay(nfts, collection_name)  # сохранение в свое хранилище json-ов

    # сохраняем картинки только уникальных image_url в каждом процессе где было по 50 потоков
    unique_nft_image_urls = set()
    nfts_to_download = []
    for nft in nfts:
        if nft.get("image") not in unique_nft_image_urls:
            nfts_to_download.append(nft)
            unique_nft_image_urls.add(nft.get("image"))

    nft_image_urls = [nft.get("image") for nft in nfts_to_download]

    # скачивание и сохранение картинки в свое хранилище
    save_nft_pictures.delay(collection_name, nft_image_urls)
    save_nfts_to_db.delay(nfts, collection_name, contract_address)  # сохранение в БД инфы по токену


@app.task(base=BaseTask, ignore_result=True)  # таск для сохранения всех json-ов асинхронно
def save_nft_metadata(nfts: list, collection_name: str):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(save_jsons(collection_name, nfts))


@app.task(base=BaseTask, ignore_result=True)  # таск для сохранения всех картинок асинхронно
def save_nft_pictures(collection_name, nft_image_urls):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(save_pictures(collection_name, nft_image_urls))


@app.task(base=BaseTask, ignore_result=True)  # таск для сохранения инфы по токену в БД
def save_nfts_to_db(nfts: list, collection_name: str, contract_address: str):
    nfts_list = []
    for nft in nfts:
        nft_tuple = convert_to_nft_model(nft=nft, collection_name=collection_name, contract_address=contract_address)
        nfts_list.append(nft_tuple)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(save_nft_async(nfts_list))
