import base64
import asyncio
import threading
from queue import Queue
import json
import time
import os
import math
import uvloop
import requests.exceptions
import aiofiles
import itertools
import httpx
import requests
from variables import ipfs_public_gateways, headers, download_manager_url
from celery import Task
from loguru import logger
from erc_1155.secondary_func_1155 import transformation_attributes


def patch_http_connection_pool(**constructor_kwargs):
    """
    This allows to override the default parameters of the
    HTTPConnectionPool constructor.
    For example, to increase the poolsize to fix problems
    with "HttpConnectionPool is full, discarding connection"
    call this function with maxsize=16 (or whatever size
    you want to give to the connection pool)
    """
    from urllib3 import connectionpool, poolmanager

    class MyHTTPConnectionPool(connectionpool.HTTPConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPConnectionPool, self).__init__(*args, **kwargs)

    poolmanager.pool_classes_by_scheme["http"] = MyHTTPConnectionPool


def patch_https_connection_pool(**constructor_kwargs):
    """
    This allows to override the default parameters of the
    HTTPConnectionPool constructor.
    For example, to increase the poolsize to fix problems
    with "HttpSConnectionPool is full, discarding connection"
    call this function with maxsize=16 (or whatever size
    you want to give to the connection pool)
    """
    from urllib3 import connectionpool, poolmanager

    class MyHTTPSConnectionPool(connectionpool.HTTPSConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPSConnectionPool, self).__init__(*args, **kwargs)

    poolmanager.pool_classes_by_scheme["https"] = MyHTTPSConnectionPool


class BaseTask(Task):
    patch_http_connection_pool(maxsize=50)
    patch_https_connection_pool(maxsize=50)
    uvloop.install()

    _http_client = None

    @property
    def http_client(self):
        if self._http_client is None:
            limits = httpx.Limits(max_keepalive_connections=50, max_connections=50)
            # лимиты подключений из одного процесса на одном воркере было 12 процессов
            self._http_client = httpx.AsyncClient(timeout=30, limits=limits, headers=headers, follow_redirects=True)
        return self._http_client


def data_topics_log(trx_log: dict):
    try:
        index = trx_log["logIndex"]
        address = trx_log["address"].lower()
        from_ = "0x" + trx_log["topics"][1][-40:]
        to_ = "0x" + trx_log["topics"][2][-40:]
        id_ = int(trx_log["topics"][3], 16)
        return index, address, from_, to_, id_
    except Exception as ex:
        logger.info(f"Method data_topics_log error: {ex}")
        return 0, "", "", "", 0


async def try_to_get_request(
    http_client: httpx.AsyncClient, url: str, timeout: int = 30, content_type: str = "meta", request_type: str = "get"
):
    print(f"Try to get request contract(TokenUrl(url): {url})")
    global ipfs_iterator
    ipfs_iterator = itertools.cycle(ipfs_public_gateways)
    ipfs_addr = None
    content_id = ""
    if "ipfs://" in url:
        content_id = url.replace("ipfs://", "")
        ipfs_addr = "ipfs://" + content_id
        url = next(ipfs_iterator) + content_id
    elif "/ipfs/" in url:
        content_id = url.split("/ipfs/")[-1]
        ipfs_gateway = url.split("/ipfs/")[0] + "/ipfs/"
        # if not ipfs_gateway in ipfs_public_gateways:
        if ipfs_gateway not in ipfs_public_gateways:
            ipfs_public_gateways.append(ipfs_gateway)
            ipfs_iterator = itertools.cycle(ipfs_public_gateways)
        ipfs_addr = "ipfs://" + content_id
        url = next(ipfs_iterator) + content_id
    status_code = 0
    err_count = 0
    err_404 = 0
    response = None

    while status_code != 200 and err_count < 5:
        try:
            if content_type == "picture":
                response_head = await http_client.head(url, timeout=30)
                content_length = response_head.headers.get("content-length")
                if content_length:
                    timeout = math.ceil(response_head.elapsed.total_seconds() + (int(content_length) / (50 * 1024)))
                else:
                    timeout = 60

            response = await getattr(http_client, request_type)(url, timeout=timeout)

            if response.status_code in [404, 429]:
                if response.status_code == 404:
                    if err_404 >= 3:
                        logger.info(f"Error request tokenUrl. Url: {url}. Err 404 >=3")
                        return None
                    err_404 += 1

                err_count += 1
                if ipfs_addr:
                    print(f"Changing ipfs gateway because of {response.status_code}")
                    url = next(ipfs_iterator) + content_id
                else:
                    logger.info(f"Error request tokenUrl. Url: {url}. ipfs_addr is empty")
                    return None

            if response.status_code == 406:
                logger.info(f"Error request tokenUrl. Url: {url}. status response = 406")
                return None

        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
            logger.info(f"Got {e}/{type(e)}... Sleeping and trying again... URL: {url}")
            await asyncio.sleep(1)
            if ipfs_addr:
                url = next(ipfs_iterator) + content_id
            err_count += 1
            continue
        except httpx.ReadError:
            logger.info("Got ReadError... Sleeping 5 seconds...")
            await asyncio.sleep(5)
            if ipfs_addr:
                url = next(ipfs_iterator) + content_id
            err_count += 1
            continue

        status_code = response.status_code
        err_count += 1

    if status_code != 200:
        logger.info(f"Response StatusCode != 200 or ErrorCount >= 5. Url: {url}")
        return None
    return response


#  получение url на json из tokenURI
def get_token_url_by_token_id(
    contract, token_id: int, contract_functions, url_queue: Queue, semaphore: threading.Semaphore
):
    token_index_function = None
    for contract_function in contract_functions:
        if contract_function.lower() in ["tokenbyindex"]:
            print("Getting token index function")
            token_index_function = getattr(contract.functions, contract_function)

    with semaphore:
        token_url = None
        print(f"Trying to get token url for token_id = {token_id}")
        while not token_url:
            if token_index_function:
                try:
                    token_id = token_index_function(token_id).call()
                except Exception:
                    logger.info("Got error while tokenbyindex getting token_id")
                    time.sleep(3)

            try:
                print(f"Calling tokenURI for {token_id}")
                token_url = contract.functions.tokenURI(token_id).call()
                print(f"Got token url {token_url}")
                # в очередь кладется словарь с token_id и token_url
                url_queue.put({"token_id": token_id, "token_url": token_url})
            except Exception as e:
                logger.info(f"Got exception {str(e)} -- {token_id}")
                break


# функция парсинга json token
async def parse_nft_json(http_client: httpx.AsyncClient, url: str, token_id: int):
    ipfs_addr = None

    if "data:application/json;base64," in url:
        result = json.loads(base64.b64decode(url.replace("data:application/json;base64,", "")))
        result["token_id"] = token_id
        return result
    elif "data:application/json;utf8," in url:
        result = json.loads(url.replace("data:application/json;utf8,", ""))
        result["token_id"] = token_id
    else:
        if "ipfs://" in url:
            content_id = url.replace("ipfs://", "")
            ipfs_addr = "ipfs://" + content_id
        elif "/ipfs/" in url:
            content_id = url.split("/ipfs/")[-1]
            ipfs_addr = "ipfs://" + content_id

        response = await try_to_get_request(http_client, url)  # отправляет запрос на сервер по url
        if not response:
            logger.info(f"url - {url}, token_id - {token_id}. No response")
            return None
        try:
            result = response.json()
        except json.decoder.JSONDecodeError:
            logger.info(f"url - {url}, token_id - {token_id}. Response no decoded JSON")
            return None

    result["token_id"] = token_id  # добавляет в общий json по токену его id и url (ссылку на json в сети)
    if ipfs_addr:
        result["token_uri"] = ipfs_addr
    else:
        result["token_uri"] = url

    return result


async def parse_nft_json_batch(http_client: httpx.AsyncClient, urls: list):
    tasks = []
    for url in urls:
        # запускаем асинхронно выполнения сбора json с одного url
        task = asyncio.ensure_future(parse_nft_json(http_client, url.get("token_url"), url.get("token_id")))
        tasks.append(task)
    # ждем завершения всех тасков
    result = await asyncio.gather(*tasks)
    return result


async def save_jsons(folder, jsons):
    if not os.path.exists(f"ethereum_blockchain/json_schema/{folder}"):  # путь к папке со всеми json коллекции
        os.mkdir(f"ethereum_blockchain/json_schema/{folder}")
    tasks = []
    for _json in jsons:
        task = asyncio.ensure_future(save_json(_json, folder))  # сохранение одного json асинхронно
        tasks.append(task)
    await asyncio.gather(*tasks)  # ждем ответа от всех
    print(f"---Probably saved count:{len(jsons)} JSONs from collection {folder}---")


async def save_json(_json, _folder):
    async with aiofiles.open(f"ethereum_blockchain/json_schema/{_folder}/{_json.get('token_id')}.json", "w") as f:
        await f.write(json.dumps(_json, indent=4))


async def save_pictures(folder, urls):
    clear_urls = []
    for url in urls:
        try:
            if not str(url).startswith("data:image"):
                clear_urls.append(url)
        except AttributeError:
            continue

    requests.post(download_manager_url, json=clear_urls)
    print(f"---Probably saved count:{len(urls)} pictures from collection {folder}---")


def find_method_in_contract(contract, contract_functions: list, possible_names: list):
    for contract_function in contract_functions:
        if any(possible_name in contract_function.lower() for possible_name in possible_names):
            try:
                value = getattr(contract.functions, contract_function)().call()
                return value
            except Exception:
                continue
    return None


def convert_to_nft_model(nft: dict, collection_name: str, contract_address: str):
    attributes = transformation_attributes(nft.get("attributes", []))
    image_path = ""
    if nft.get("image", "").startswith("data:"):
        image_path = nft.get("image", "")
    json_schema_path = f"ethereum_blockchain/json_schema/{collection_name}/{nft.get('token_id')}.json"
    answer_tuple = (
        nft.get("token_id"),
        collection_name,
        0,
        json.dumps(attributes),
        nft.get("image"),
        json_schema_path,
        contract_address,
        image_path,
    )
    return answer_tuple
