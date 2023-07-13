from erc_1155.secondary_func_1155 import (
    get_data_open_sea_api,
    get_abi_api,
    get_abi_database,
    find_method_in_contract_with_arg,
    get_json_metadata_token,
    find_name_key_json,
    save_json_schema,
    find_collection_name,
    transformation_image_url,
    transformation_attributes,
    find_method_in_contract_without_arg,
)
from variables import (
    abi_standard,
    abi_721,
    abi_coins,
    OWN_NODE_URL,
    possible_supply_count_names,
    possible_uri_names,
    possible_is_nft_names,
    possible_image_json,
    possible_implementation,
    contracts_marketplace,
    download_manager_url,
    possible_name_token_json,
    NULL_ADDRESS,
    OpenSea_contract_1155,
)
from query_dir.collections import create_collection
from query_dir.nft import create_nft_1155
from query_dir.events import add_nft_log_721_1155
from web3 import Web3
import json
import requests
from loguru import logger


def get_abi(contract_address: str, erc: str = "1155"):
    abi = get_abi_database(contract_address=contract_address)
    if abi is None:
        abi = get_abi_api(contract_address=contract_address)
        if abi is None:
            if erc == "1155":
                abi = abi_standard
            elif erc == "721":
                abi = abi_721
            elif erc == "20":
                abi = abi_coins
    return abi


def get_name_contract(contract, contract_address):
    try:
        name_contract = contract.functions.name().call()
    except Exception:  # нет функции name в контракте
        logger.info(f"No function name contract -- {contract_address}")
        name_contract = None
    return name_contract


def analise_contract_and_get_data(contract, contract_address, token_id, name_contract, contract_functions):
    print(f"Start analise contract. Contract_address: {contract_address}. Token_id: {token_id}")
    if contract_address == OpenSea_contract_1155:
        total_supply_token = contract.functions.totalSupply(token_id).call()
        if total_supply_token == 1:
            data_coll_and_token = get_data_open_sea_api(token_id=token_id)
            if data_coll_and_token:
                return data_coll_and_token
            else:
                return None
        else:
            return None
    elif name_contract:
        name_collection = name_contract
        total_supply_token = find_method_in_contract_with_arg(
            contract, contract_functions, possible_supply_count_names, token_id
        )
        total_supply_collection = find_method_in_contract_without_arg(
            contract, contract_functions, possible_supply_count_names
        )

        if not total_supply_token and not total_supply_collection:
            is_nft_bool = find_method_in_contract_with_arg(
                contract, contract_functions, possible_is_nft_names, token_id
            )
            if is_nft_bool is False:
                return None
        elif total_supply_token and total_supply_token != 1:
            return None

        token_url_json = find_method_in_contract_with_arg(contract, contract_functions, possible_uri_names, token_id)

        if not token_url_json:
            implementation = find_method_in_contract_without_arg(contract, contract_functions, possible_implementation)
            if not implementation:
                return None
            else:
                print(f"implementation in another contract: {implementation}")
                abi = get_abi(contract_address=implementation.lower())
                web3 = Web3(Web3.HTTPProvider(OWN_NODE_URL))
                contract = web3.eth.contract(address=web3.toChecksumAddress(implementation.lower()), abi=abi)
                if isinstance(abi, str):
                    abi = json.loads(abi)
                contract_functions = [function.get("name") for function in abi if function.get("name")]
                name_contract = get_name_contract(contract, implementation)
                data_contr_imp = analise_contract_and_get_data(
                    contract, implementation.lower(), token_id, name_contract, contract_functions
                )
                if data_contr_imp:
                    return data_contr_imp
                else:
                    return None

        metadata_json = get_json_metadata_token(token_url_json, token_id)
        if not metadata_json:
            return None

        name_metadata = metadata_json.get("name", "").replace("'", "")

        description_collect = metadata_json.get("description", "").replace("'", "")
        if total_supply_collection:
            total_supply = total_supply_collection
        else:
            total_supply = 0
        num_owners = 0
        attr_dict = metadata_json.get("attributes", {})  # find_name_key_json(metadata_json, possible_total_supply_json)
        attr_dict_right = transformation_attributes(attr_dict)
        release_date = metadata_json.get("release_date", 0)
        image_url = find_name_key_json(metadata_json, possible_image_json)
        image_url = transformation_image_url(image_url)
        path_schema = save_json_schema(name_collection, token_id, metadata_json)
        if image_url.startswith("data:"):
            image_path = image_url
        else:
            image_path = ""
            requests.post(download_manager_url, json=[image_url])  # запрос к сервису скачивания картинок

        answer_tuple = (
            name_metadata,
            name_collection,
            description_collect,
            total_supply,
            num_owners,
            attr_dict_right,
            release_date,
            image_url,
            path_schema,
            image_path,
        )
        return answer_tuple
    else:
        total_supply_token = find_method_in_contract_with_arg(
            contract, contract_functions, possible_supply_count_names, token_id
        )
        total_supply_collection = find_method_in_contract_without_arg(
            contract, contract_functions, possible_supply_count_names
        )
        if not total_supply_token and not total_supply_collection:
            is_nft_bool = find_method_in_contract_with_arg(
                contract, contract_functions, possible_is_nft_names, token_id
            )
            if is_nft_bool is False:
                return None
        elif total_supply_token and total_supply_token != 1:
            return None

        token_url_json = find_method_in_contract_with_arg(contract, contract_functions, possible_uri_names, token_id)

        if not token_url_json:
            implementation = find_method_in_contract_without_arg(contract, contract_functions, possible_implementation)
            if not implementation:
                return None
            else:
                print(f"implementation in another contract: {implementation}")
                abi = get_abi(contract_address=implementation.lower())
                web3 = Web3(Web3.HTTPProvider(OWN_NODE_URL))
                contract = web3.eth.contract(address=web3.toChecksumAddress(implementation.lower()), abi=abi)
                if isinstance(abi, str):
                    abi = json.loads(abi)
                contract_functions = [function.get("name") for function in abi if function.get("name")]
                name_contract = get_name_contract(contract, implementation)
                data_contr_imp = analise_contract_and_get_data(
                    contract, implementation.lower(), token_id, name_contract, contract_functions
                )
                if data_contr_imp:
                    return data_contr_imp
                else:
                    return None

        metadata_json = get_json_metadata_token(token_url_json, token_id)
        if not metadata_json:
            return None

        name_metadata = find_name_key_json(metadata_json, possible_name_token_json)
        if not name_metadata:
            return None
        else:
            name_metadata = name_metadata.replace("'", "")

        description_collect = metadata_json.get("description", "").replace("'", "")
        total_supply = 0
        num_owners = 0
        attr_dict = metadata_json.get("attributes", {})  # find_name_key_json(metadata_json, possible_total_supply_json)
        attr_dict_right = transformation_attributes(attr_dict)
        release_date = metadata_json.get("release_date", 0)
        image_url = find_name_key_json(metadata_json, possible_image_json)
        image_url = transformation_image_url(image_url)
        name_collection = find_collection_name(name_metadata)
        path_schema = save_json_schema(name_collection, token_id, metadata_json)

        if image_url.startswith("data:"):
            image_path = image_url
        else:
            image_path = ""
            requests.post(download_manager_url, json=[image_url])  # запрос к сервису скачивания картинок

        answer_tuple = (
            name_metadata,
            name_collection,
            description_collect,
            total_supply,
            num_owners,
            attr_dict_right,
            release_date,
            image_url,
            path_schema,
            image_path,
        )
        return answer_tuple


def write_db_collection_and_token(
    name_collection,
    id_token,
    contract_address,
    description_collection,
    total_supply,
    path_schema,
    attributes_nft,
    image_url,
    release_date,
    owner_count,
    image_path,
):

    create_collection(
        name=name_collection,
        contract_addr=contract_address,
        description=description_collection,
        total_supply_nft=total_supply,
        release_date=release_date,
        owner_count=owner_count,
        erc="ERC-1155",
    )
    print(f"Save in DataBase collection with name: {name_collection}")
    create_nft_1155(
        token_id=id_token,
        name_collection=name_collection,
        path_json_schema=path_schema,
        attributes=attributes_nft,
        image_url=image_url,
        contract_addr=contract_address,
        image_path=image_path,
    )
    print(f"Save in DataBase new nft token_id: {id_token} | Collection: {name_collection}")


def search_contract_exchange(is_contract_exchange_tx, contract_addr_log):
    exchange_name = ""
    if is_contract_exchange_tx != "":
        exchange_name = is_contract_exchange_tx
    elif contracts_marketplace.get(contract_addr_log.lower(), "") != "":
        exchange_name = contracts_marketplace.get(contract_addr_log.lower(), "")
    return exchange_name


def write_db_log(trx, sender, recipient, log_index, tokens, contract_address, is_contract_exchange_tx, name_event: str):

    if sender == NULL_ADDRESS:
        name_event = f"{name_event}(MINT)"
    elif recipient == NULL_ADDRESS:
        name_event = f"{name_event}(BURN)"

    exchange_name = search_contract_exchange(is_contract_exchange_tx, contract_address)

    add_nft_log_721_1155(
        trx_hash=str(trx),
        log_index=int(log_index),
        contract_addr=contract_address,
        event_name=name_event,
        addr_from=sender,
        addr_to=recipient,
        token_id=tokens,
        name_exchange_nft=exchange_name,
    )
    print(f"Save event name: {name_event} | log_index: {log_index} | trx_hash: {trx}")
