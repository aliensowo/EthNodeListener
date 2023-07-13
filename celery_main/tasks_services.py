import datetime

# import time
import os
from celery_main.celery import app
from web3 import Web3
from query_dir.events import (
    get_order_matched,
    get_nft_log_with_contract_query,
    get_all_nft_log_with_block,
    current_floor_price,
    find_log_in_trx_price,
)
from query_dir.query import create_trx, create_log, get_day_blocks
from query_dir.collections import (
    get_all_collection,
    update_attributes_collection,
    update_curr_ts_collection,
    update_collection_cap_avg,
    get_name_collection,
    update_owners_count,
    update_collection_sales_vol_floor,
)
from query_dir.contracts import create_contract
from query_dir.nft import (
    count_nft_in_collection,
    get_all_nft_attr,
    update_nft_rariry,
    empty_image_path,
    count_empty_image_path,
    update_image_path,
    get_all_token_id_coll,
)
from query_dir.query_sol_db import get_data_download_service
from services.rarity import get_rarity_collection, update_rarity_nft
import copy
from variables import (
    # OWN_NODE_URL,
    download_manager_url,
    abi_owner_of,
    OpenSea_contract_1155,
    CryptoPunks_contract,
    CryptoKitties_contract,
    POKT_URL,
)
from loguru import logger
import requests
from services.api_open_sea import get_all_stat_api

logger.add(
    "ethereum_blockchain/error_logs/errors_services.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="200 MB",
    retention="30 days",
)

web3 = Web3(Web3.HTTPProvider(POKT_URL))

"""Tasks update owners count collections DB"""


@app.task(ignore_result=True)
def owners_count_all_collection():
    all_collection = get_all_collection()  # запрос к бд на получение всех коллекций
    for collection in all_collection:
        name_coll = collection[0]
        contract_address = collection[1]
        if contract_address.lower() not in [OpenSea_contract_1155, CryptoPunks_contract, CryptoKitties_contract]:
            print(
                f"Start calculate count owner. Name_collection: {name_coll} |"
                f"Contract_address: {contract_address.lower()}"
            )
            check_owner_address.delay(contract_addr=contract_address, name_collection=name_coll)


@app.task(ignore_result=True)
def check_owner_address(contract_addr: str, name_collection: str):
    contract_owner_of = web3.eth.contract(address=web3.toChecksumAddress(contract_addr), abi=abi_owner_of)
    all_token_coll = get_all_token_id_coll(name_col=name_collection, contr_addr=contract_addr)  # request DB
    owners_count = 0
    for token in all_token_coll:
        token_id = token[0]
        try:
            owner_address = contract_owner_of.functions.ownerOf(int(token_id)).call()
        except Exception as ex:
            logger.info(f"Error contract function ownerOf: {ex}")
            return None
        try:
            owner_entity = web3.eth.get_code(owner_address)
        except Exception as ex:
            logger.info(f"Error get bytecode owner_address: {ex}")
            continue
        if owner_entity == b"":
            owners_count += 1

    update_owners_count(name=name_collection, contract_addr=contract_addr, owners_count=owners_count)
    print(
        f"New data write. Name_collection: {name_collection} | Contract_address: {contract_addr} |"
        f"New count owners: {owners_count}"
    )


"""###################################################################"""

"""Tasks services finance statistic"""


@app.task(ignore_result=True)
def get_mar_cup_avg():
    print("Start calculate: market_cap, avg_price")
    all_collections = get_all_collection()
    for collection in all_collections:
        name_collection = collection[0]
        contract_address = collection[1]

        analise_collection.delay(name_collection, contract_address)
    print("All collections placed tasks(analise_collection)")


@app.task(ignore_result=True)
def analise_collection(name, contract_addr):

    if (
        contract_addr == OpenSea_contract_1155
        or contract_addr == CryptoKitties_contract
        or contract_addr == CryptoPunks_contract
    ):  # Open Sea получаем всю статистику из api
        # + сразу записываем sales, volume, floor_price если есть (чтобы 2 раза не обращаться к API)
        get_all_stat_api(contract_address=contract_addr, name_coll=name)
        return None

    market_cap = 0
    name_collection = name

    all_nft_collection = get_all_token_id_coll(name_col=name_collection, contr_addr=contract_addr.lower())

    count_tokens = len(all_nft_collection)

    if count_tokens == 0:
        print(f"Counts tokens this collection({name_collection}) == 0. Continue")
        return

    all_logs_contract = get_nft_log_with_contract_query(contract_address=contract_addr.lower())

    for nft in all_nft_collection:
        token_id = int(nft[0])
        market_cap_task = analise_nfts(list(all_logs_contract), token_id)

        market_cap += market_cap_task

    avg_price = market_cap / count_tokens

    update_collection_cap_avg(
        contract_addr=contract_addr.lower(), name_collection=name_collection, market_cap=market_cap, avg_price=avg_price
    )
    print(
        f"New data write database. Name_collection: {name_collection} |  Contract_address: {contract_addr.lower()} |"
        f"Market_cap: {market_cap} | Avg_price: {avg_price}"
    )


def analise_nfts(all_logs: list, token_id: int):
    for lg in all_logs:
        if str(token_id) in lg[-1]:  # проверка совпадения токен id в списке
            price_db = get_order_matched(trx_hash=lg[0], log_index=lg[1])
            if price_db != 0.0 and isinstance(price_db, float):
                return price_db
            else:
                price_db = find_log_in_trx_price(trx_hash=lg[0], token_id=token_id)
                if price_db != 0 and isinstance(price_db, float):
                    return price_db
    return 0.0


def get_sales_vol_floor_pr():
    print("Start simple function Calculate: Sales, Volume, Floor_price")
    cur_data = datetime.datetime.now()
    unix_end = int(cur_data.timestamp())
    last_day = cur_data - datetime.timedelta(days=1000)
    unix_start = int(last_day.timestamp())

    latest_day_blocks = get_day_blocks(start=unix_start, end=unix_end)

    print(f"Count search block last day: {len(latest_day_blocks)}")

    tasks = []
    for block in latest_day_blocks:
        task = analise_block.delay(int(block[0]))
        tasks.append(task)
    print("All blocks placed tasks(analise_block)")

    res_name_contr_addr = {}
    res_collections_volume = {}
    res_collections_sales = {}
    for task in tasks:
        name_contr_addr, collections_volume, collections_sales = task.get()

        for name_collection, contract_address in name_contr_addr.items():
            if res_name_contr_addr.get(name_collection, "") == "":
                res_name_contr_addr[name_collection] = contract_address

            if res_collections_volume.get(name_collection, "") == "":
                res_collections_volume[name_collection] = collections_volume[name_collection]
            else:
                list_volume = collections_volume[name_collection]
                res_list_volume = res_collections_volume[name_collection] + list_volume
                res_collections_volume[name_collection] = res_list_volume
            if res_collections_sales.get(name_collection, "") == "":
                res_collections_sales[name_collection] = collections_sales[name_collection]
            else:
                res_collections_sales[name_collection] += collections_sales[name_collection]

    for name_collection, contract_address in res_name_contr_addr.items():

        list_price = res_collections_volume[name_collection]
        volume = sum(list_price)

        for val in list_price:
            if val == 0:
                list_price.remove(val)

        if len(list_price) != 0:
            floor_price = min(list_price)
        else:
            floor_price = 0

        if floor_price == 0:
            last_floor_price = current_floor_price(name=name_collection, contract_address=contract_address)
            floor_price = last_floor_price

        sales = res_collections_sales[name_collection]
        update_collection_sales_vol_floor(
            contract_addr=contract_address,
            name_collection=name_collection,
            sales=sales,
            volume=volume,
            floor_price=floor_price,
        )
        print(
            f"New data write database. Name_collection: {name_collection} |  Contract_address: {contract_address} |"
            f"Sales: {sales} | Volume: {volume} | Floor_price: {floor_price}"
        )
    print("Complete function 'Get Sales, Volume, Floor price'")


@app.task
def analise_block(block_number: int):
    collections_volume = {}
    collections_sales = {}
    name_contr_addr = {}

    all_nft_trx_block = get_all_nft_log_with_block(block_number)

    for nft_log in all_nft_trx_block:
        trx_hash = nft_log[0]
        log_index = nft_log[1]
        contract_address = nft_log[2]
        token_id = nft_log[3][0]
        price = nft_log[4]

        if (
            contract_address == OpenSea_contract_1155
            or contract_address == CryptoKitties_contract
            or contract_address == CryptoPunks_contract
        ):
            continue

        name_collection = get_name_collection(contract_address, int(token_id))
        if not name_collection:
            print(
                f"Not found name collection in DataBase. Contract_address: {contract_address}|"
                f"Token_id: {int(token_id)}. Continue"
            )
            continue
        if price == 0 or price is None:
            price = get_order_matched(trx_hash=trx_hash, log_index=log_index)

        if collections_volume.get(name_collection, "") == "":
            collections_volume[name_collection] = [price]
        else:
            list_volume = collections_volume[name_collection]
            list_volume.append(price)
            collections_volume[name_collection] = list_volume
        if collections_sales.get(name_collection, "") == "":
            collections_sales[name_collection] = 1
        else:
            collections_sales[name_collection] += 1

        name_contr_addr[name_collection] = contract_address

    return name_contr_addr, collections_volume, collections_sales


"""###################################################################"""


"""Task write all good trx in db"""


@app.task(ignore_result=True)
def write_data_trx_db(trx_data: dict, trx_logs: dict, timestamp: int):
    try:
        trx_hash = trx_data["hash"]
        trx_to = trx_data["to"]
        trx_from = trx_data["from"].lower()
        number_block = trx_data["blockNumber"]

        trx_to = trx_to.lower()
        create_trx(
            trx_hash=trx_hash,
            block_number=number_block,
            from_address=trx_from,
            to_address=trx_to,
            gas=trx_data["gas"],
            gas_price=trx_data["gasPrice"],
            input_data=str(trx_data["input"]),
            value=trx_data["value"],
            timestamp=timestamp,
        )

        for lg in trx_logs:
            create_log(
                trx_hash=trx_hash,
                data=lg["data"],
                topics=[topic for topic in lg["topics"]],
                log_index=lg["logIndex"],
                address=lg["address"].lower(),
            )

        if not os.path.exists(f"ethereum_blockchain/bytecodes/{trx_to}.bin"):
            bytecode_address = web3.eth.getCode(Web3.toChecksumAddress(trx_to))
            if bytecode_address != b"":
                try:
                    with open(f"ethereum_blockchain/bytecodes/{trx_to}.bin", "w") as fp:
                        fp.write(bytecode_address.hex())
                    bytecode_path = f"ethereum_blockchain/bytecodes/{trx_to}.bin"
                    create_contract(address=trx_to, bytecode_path=bytecode_path)
                except FileNotFoundError:
                    logger.info("No right local path write bytecode")

        print(f"Success write db transaction and logs. Trx_hash: {trx_hash}")
    except Exception as ex:
        trx_hash = trx_data["hash"]
        logger.info(f"Error: {ex} | trx_hash: {trx_hash}")


"""###################################################################"""

"""Tasks update_rarity_db"""


@app.task(ignore_result=True)
def rarity_nft(nft_info: tuple, schema: dict, name_collection: str):
    token_id = nft_info[0]
    attr_nft = nft_info[1]
    try:
        rar_schema, rarity = update_rarity_nft(nft=attr_nft, rarity_schema=schema)
    except Exception as ex:
        logger.info(ex)
        return None

    update_nft_rariry(token_id=token_id, attributes=rar_schema, rarity=rarity, name_collection=name_collection)
    print(f"Write new data rarity NFT. Token_id: {token_id} | Name_collection: {name_collection} | Rarity: {rarity}")


@app.task(ignore_result=True)
def rarity_collection(collection_info: tuple):
    name_collection = collection_info[0]
    contract_address = collection_info[1]
    current_total_supply = collection_info[2]
    count_nft = count_nft_in_collection(name_collection=name_collection, contract_address=contract_address)
    print(
        f"Analise rarity collection. Name_collection: {name_collection} | Count_nft_db: {count_nft} |"
        f"Current_ts: {current_total_supply}"
    )
    if current_total_supply != count_nft:
        all_nft_collection = get_all_nft_attr(name_collection=name_collection, contract_address=contract_address)
        original_attr_nfts = copy.deepcopy(all_nft_collection)
        try:
            schema = get_rarity_collection(nft_collection_list=all_nft_collection)
        except Exception as ex:
            logger.info(f"Error get rarity schema: {ex}")
            return None
        if schema:
            update_attributes_collection(name=name_collection, contract_addr=contract_address, attributes=schema)
            for nft in original_attr_nfts:
                rarity_nft.delay(nft_info=nft, schema=schema, name_collection=name_collection)

        update_curr_ts_collection(name=name_collection, contract_addr=contract_address, current_total_supply=count_nft)


@app.task(ignore_result=True)
def main_rarity_service():
    all_collection = get_all_collection()
    for collection in all_collection:
        rarity_collection.delay(collection_info=collection)


"""###################################################################"""

"""Tasks update image path"""


@app.task(ignore_result=True)
def get_db_service_download_image(token_id: int, name_coll: str, url: str):
    #  запрос к бд 192.168.2.9
    res = get_data_download_service(url)
    if not res:
        print(f"No result DB ServiceDownloadImage. Make request service ServiceDownloadImage. Url: {url}")
        requests.post(download_manager_url, json=[url])
    elif res[2] == "Downloaded":
        #  /media/ (корень пути)
        content_id = res[0]
        format_img = res[1]
        image_path = f"media/{content_id}.{format_img}"
        update_image_path(token_id, name_coll, image_path)
        print(f"Success downloaded image. Write DB Image_path: {content_id}.{format_img}")


@app.task(ignore_result=True)
def main_image_path_service():
    count_str = count_empty_image_path()
    for offset_num in range(0, count_str + 1, 10000):
        # запрос к БД 192.168.2.223 где нет image_path
        nfts_without_image_path = empty_image_path(offset_num=offset_num)
        # print(nfts_without_image_path)
        for nft in nfts_without_image_path:
            token_id = nft[0]
            name_collection = nft[1]
            image_url = nft[2]
            get_db_service_download_image.delay(token_id=token_id, name_coll=name_collection, url=image_url)


"""###################################################################"""
