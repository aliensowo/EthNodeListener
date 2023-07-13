import requests
import re
import base64
import time
import os
from query_dir.contracts import create_contract, update_contract, abi_contract, check_exist_contract
from hexbytes import HexBytes
from variables import abi_standard_change_name_args, API_KEY_ETHERSCAN, proxies, download_manager_url
import json
from loguru import logger


def transformation_attributes(attributes):
    try:
        attr_dict = {}
        for attr in attributes:
            try:
                trait_type = attr["trait_type"].replace("'", "")
            except Exception:
                trait_type = attr["trait_type"]
            try:
                value = attr["value"].replace("'", "")
            except Exception:
                value = attr["value"]

            if trait_type not in attr_dict.keys():
                attr_dict[trait_type] = value
            else:
                old = attr_dict[trait_type]
                if isinstance(old, list):
                    old.append(value)
                    attr_dict[trait_type] = old
                else:
                    new_ = [old, value]
                    attr_dict[trait_type] = new_
        return attr_dict
    except Exception as ex:
        logger.info(f"Error transformation attributes: {ex}")
        return attributes


def get_data_open_sea_api(token_id: int):
    """Method get information from api OpenSea"""
    headers_ = {"x-api-key": "2f6f419a083c46de9d83ce3dbe7db601"}
    proxies_ = {"http": "socks5://192.168.1.44:1080", "https": "socks5://192.168.1.44:1080"}
    status_code = None
    error_count = 0
    try:
        while status_code != 200 and error_count < 10:
            print(
                f"Request API OpenSea. Contract_address: 0x495f947276749ce646f68ac8c248420045cb7b5e |"
                f"token_id :{token_id}"
            )
            try:
                r = requests.get(
                    f"https://api.opensea.io/api/v1/asset/0x495f947276749ce646f68ac8c248420045cb7b5e/{token_id}",
                    headers=headers_,
                    proxies=proxies_,
                )
                status_code = r.status_code
                if status_code == 404:
                    break
            except Exception as ex:
                logger.info(f"Error request OpenSe API: TypeError: {type(ex)} | Error: {ex}")
                error_count += 1

        if error_count == 10 or status_code == 404:
            return None

        param = r.content
        info = json.loads(param)
        name_token = info["name"].replace("'", "")
        image_url = info["image_url"]
        name_collection = info["collection"]["name"]
        stats = info["collection"]["stats"]

        description_collect = info["collection"]["description"]

        total_supply = int(stats.get("total_supply"))
        if not total_supply:
            total_supply = int(stats.get("count", 0))

        num_owners = stats.get("num_owners", 0)
        attributes = info["traits"]

        attr_dict = transformation_attributes(attributes)
        release_date = 1606920053

        print(
            f"Success request API OpenSea. Name_collection: {name_collection} | Name_token: {name_token} |"
            f"Token_id: {token_id}"
        )

        name_collection = name_collection.replace("'", "")
        try:
            description_collect = description_collect.replace("'", "")
        except Exception:
            description_collect = ""

        if image_url.startswith("data:"):
            image_path = image_url
        else:
            image_path = ""
            requests.post(download_manager_url, json=[image_url])  # запрос к сервису скачивания картинок

        path_schema = save_json_schema(name_collection=name_collection, id_token=token_id, schema=info)
        answer_tuple = (
            name_token,
            name_collection,
            description_collect,
            total_supply,
            num_owners,
            attr_dict,
            release_date,
            image_url,
            path_schema,
            image_path,
        )

        return answer_tuple

    except Exception as ex:
        logger.info(f"Error method get info OpenSea API: {ex}")
        return None


def find_collection_name(name_token):
    name_token = name_token.strip()

    if re.search(r"(^#)([0-9]+)\s[\w._%+-]", name_token) is not None:
        name_collection_words_list = name_token.split(" ")
        name_collection_full = ""
        for i in range(1, len(name_collection_words_list)):
            name_collection_full += name_collection_words_list[i]
            name_collection_full += " "
        name_collection_full = name_collection_full.strip()
        return name_collection_full
    elif re.search(r"(^.([a-zA-Z0-9\"\:\(\)\_\,\/\!\s-]+)(#)([0-9\s]+))", name_token) is not None:
        name_collection = name_token.split("#")
        if name_collection[0] == "":
            return name_collection[1].strip()
        else:
            return name_collection[0].strip()

    return name_token


def find_name_key_json(json_data: dict, possible_names: list):
    for key in json_data.keys():
        # if any(possible_name in key.lower() for possible_name in possible_names):
        if any(possible_name == key.lower() for possible_name in possible_names):
            value = json_data.get(key)
            return value
    return None


def make_request(url: str):
    print(f"Request url from contract. Get JSON. Url: {url}")
    if "data:application/json;base64," in url:
        link = url[29:]
        data = base64.b64decode(link)
        data = json.loads(data)

        return data
    else:
        max_errors = 10
        error_count = 0
        while True:
            if error_count == max_errors:
                return None
            try:
                url = url.strip()
                response = requests.get(url, timeout=(4, 4), proxies=proxies)
            except Exception as ex:
                logger.info(f"Error requests json: {ex} | Url: {url}")
                error_count += 1
                continue
            if response.status_code != 200:
                logger.info(f"Response error status code != 200: {response.status_code} | Sleep for 1 sec")
                time.sleep(1)
                error_count += 1
                continue
            else:
                try:
                    data = response.json()
                    return data
                except AttributeError as ex:
                    logger.info(f"Error response convert to Json: {ex}")
                    error_count += 1
                    continue


def update_link_metadata_token(token_url, id_token):
    if "data:application/json;base64," not in token_url:
        if "0x{id}" in token_url:
            id_str = str(hex(id_token)).lower()
            id_str = id_str.replace("0x", "")
            token_url = token_url.replace("{id}", f"{id_str}")
        elif "{id}.json" in token_url or "{id}" in token_url:
            id_str = str(hex(id_token)).lower()
            id_str = id_str.replace("0x", "")
            if len(id_str) < 64:
                id_str = ("0" * (64 - len(id_str))) + id_str
            token_url = token_url.replace("{id}", f"{id_str}")
        if "ipfs://ipfs/" in token_url:
            token_url = "https://ipfs.io/ipfs/" + token_url[12:]
        elif "ipfs://" in token_url:
            token_url = "https://ipfs.io/ipfs/" + token_url[7:]
        elif "http" not in token_url:
            token_url = "https://ipfs.io/ipfs/" + token_url
    return token_url


def get_json_metadata_token(token_url, id_token):
    if "data:application/json;utf8," in token_url:
        response = json.loads(token_url[27:])
    else:
        update_token_url = update_link_metadata_token(token_url, id_token)
        response = make_request(update_token_url)
    if (("{id}.json" in token_url) or ("{id}" in token_url)) and response is None:
        token_url = token_url.replace("{id}", f"{id_token}")
        response = make_request(token_url)
    return response


def save_json_schema(name_collection: str, id_token: int, schema: dict):
    """Method save json_schema on right path local"""
    try:
        if not os.path.exists(f"ethereum_blockchain/json_schema/{name_collection}"):
            os.mkdir(f"ethereum_blockchain/json_schema/{name_collection}")
        with open(f"ethereum_blockchain/json_schema/{name_collection}/{id_token}.json", "w") as fp:
            json.dump(schema, fp)
        path_schema = f"ethereum_blockchain/json_schema/{name_collection}/{id_token}.json"
        return path_schema
    except Exception as ex:
        logger.info(f"Error method save json schema: {ex}")
        return "No save json NFT (Error)"


def find_method_in_contract_with_arg(contract, contract_functions: list, possible_names: list, token_id: int):
    for contract_function in contract_functions:
        if any(possible_name in contract_function.lower() for possible_name in possible_names):
            try:
                value = getattr(contract.functions, contract_function)(token_id).call()
                return value
            except Exception:
                continue
    return None


def find_method_in_contract_without_arg(contract, contract_functions: list, possible_names: list):
    for contract_function in contract_functions:
        if any(possible_name in contract_function.lower() for possible_name in possible_names):
            try:
                value = getattr(contract.functions, contract_function)().call()
                return value
            except Exception:
                continue
    return None


def get_abi_database(contract_address: str):
    abi_path = abi_contract(address=contract_address)
    if abi_path is not None:
        try:
            with open(f"{abi_path}", "r") as fp:
                abi = str(fp.readline())
            return abi
        except FileNotFoundError:
            logger.info(f"No right path to read abi_contract -- {abi_path}")
            return None
    return None


def get_abi_api(contract_address: str):
    status_code = None
    error_count = 0
    print(f"Request API EtherScan get ABI. Contract_address: {contract_address}")
    while status_code != 200 and error_count < 10:
        try:
            r = requests.get(
                f"https://api.etherscan.io/api?module=contract&"
                f"action=getabi&address={contract_address}&apikey={API_KEY_ETHERSCAN}",
                timeout=10,
                proxies=proxies,
            )
            status_code = r.status_code
        except Exception as ex:
            logger.info(f"Error request API EtherScan. Type Error: {type(ex)}. Error: {ex}")
            error_count += 1

    if error_count == 10 or status_code == 404:
        return None

    data = json.loads(r.content)
    if data["status"] != "1":
        return None

    abi = data["result"].strip()
    try:
        with open(f"ethereum_blockchain/abi_contract/{contract_address}.txt", "w") as fp:
            abi = abi.replace("true", '"True"')
            abi = abi.replace("false", '"False"')
            fp.write(abi)
        path_api = f"ethereum_blockchain/abi_contract/{contract_address}.txt"
        if check_exist_contract(contract_address):
            update_contract(contract_address, path_api)
        else:
            create_contract(contract_address, path_api)
    except FileNotFoundError:
        logger.info("No right local path to write abi_contract.")
    return abi


def transformation_image_url(image_url):
    if "ipfs://ipfs/" in image_url:
        image_url = "https://ipfs.io/ipfs/" + image_url[12:]
    elif "ipfs://" in image_url:
        image_url = "https://ipfs.io/ipfs/" + image_url[7:]
    elif r"\/" in image_url:
        image_url = image_url.replace(r"\/", "/")
    elif "https://" not in image_url:
        image_url = "https://ipfs.io/ipfs/" + image_url
    return image_url


def save_bytes_image(image_bytes, format_picture, name_collection, token_id):
    if not os.path.exists(f"ethereum_blockchain/images/{name_collection}"):
        os.mkdir(f"ethereum_blockchain/images/{name_collection}")
    try:
        with open(f"ethereum_blockchain/images/{name_collection}/{token_id}.{format_picture}", "wb") as fp:
            fp.write(image_bytes)
        path_picture = f"ethereum_blockchain/images/{name_collection}/{token_id}.{format_picture}"
    except Exception:
        logger.info(f"Failed to save bytes picture. Token_id: {token_id} | Name_collection: {name_collection}")
        path_picture = "No save bytes picture"
    return path_picture


def transfer_batch(web3, contract, contract_address, log_to_process):
    log_to_process["blockHash"] = HexBytes(log_to_process["blockHash"])
    log_to_process["transactionHash"] = HexBytes(log_to_process["transactionHash"])
    log_to_process["topics"] = [HexBytes(topic) for topic in log_to_process["topics"]]

    try:
        logs = contract.events.TransferBatch().processLog(log_to_process)
        sender = logs["args"]["from"]
        recipient = logs["args"]["to"]
        ids = logs["args"]["ids"]
        value = logs["args"]["values"]
    except Exception:
        contract = web3.eth.contract(
            address=web3.toChecksumAddress(contract_address), abi=abi_standard_change_name_args
        )
        try:
            logs = contract.events.TransferBatch().processLog(log_to_process)
            sender = logs["args"]["_from"]
            recipient = logs["args"]["_to"]
            ids = logs["args"]["_ids"]
            value = logs["args"]["_values"]
        except Exception:
            return None

    id_with_count_one = []
    for index in range(len(ids)):
        if value[index] > 1:
            continue
        id_with_count_one.append(ids[index])

    data_in_log = (id_with_count_one, sender, recipient)
    return data_in_log


def get_data_log_uri(web3, contract, contract_address, log_to_process):
    log_to_process["blockHash"] = HexBytes(log_to_process["blockHash"])
    log_to_process["transactionHash"] = HexBytes(log_to_process["transactionHash"])
    log_to_process["topics"] = [HexBytes(topic) for topic in log_to_process["topics"]]

    try:
        logs = contract.events.URI().processLog(log_to_process)
        token_url = logs["args"]["value"]
        id_token = logs["args"]["id"]
    except Exception:  # web3.exceptions.LogTopicError:
        contract = web3.eth.contract(
            address=web3.toChecksumAddress(contract_address), abi=abi_standard_change_name_args
        )
        try:
            logs = contract.events.URI().processLog(log_to_process)
            token_url = logs["args"]["_value"]
            id_token = logs["args"]["_id"]
        except Exception:
            return None
    data_in_log = (token_url, id_token)
    return data_in_log
