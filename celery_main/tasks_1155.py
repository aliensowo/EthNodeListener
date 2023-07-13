from celery_main.celery import app
from web3 import Web3
from variables import contracts_marketplace
from query_dir.nft import check_exist_nft_1155
from loguru import logger
from variables import TransferSingle, TransferBatch, URI, POKT_URL  # OWN_NODE_URL,
import json
from erc_1155.main_func_1155 import (
    get_abi,
    get_name_contract,
    analise_contract_and_get_data,
    write_db_collection_and_token,
    write_db_log,
)
from erc_1155.secondary_func_1155 import transfer_batch, get_data_log_uri

logger.add(
    "ethereum_blockchain/error_logs/errors_erc_1155.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="200 MB",
    retention="30 days",
)

"""Tasks erc-1155 NFT"""

web3 = Web3(Web3.HTTPProvider(POKT_URL))


@app.task(ignore_result=True)
def get_data_1155(transaction, is_contract_marketplace_tx, logs_transaction):
    for lg in logs_transaction:
        try:
            if (lg["topics"][0] == TransferSingle) and (len(lg["topics"]) == 4):
                print(f"Find event TransferSingle | trx_hash: {transaction}")
                name_event = "TransferSingle"
                log_index = lg["logIndex"]
                contract_address = lg["address"].lower()
                sender = "0x" + lg["topics"][2][-40:]
                recipient = "0x" + lg["topics"][3][-40:]
                id_token = int("0x" + lg["data"][2:66], 16)
                value = int("0x" + lg["data"][-64:], 16)

                if value > 1:
                    print(f"Transfer not NFT in event TransferSingle | trx_hash: {transaction}")
                    continue

                if check_exist_nft_1155(token_id=id_token, contract_address=contract_address):
                    print(
                        f"This NFT already exist DataBase | trx_hash: {transaction} , token_id: {id_token},"
                        f"contract_address: {contract_address}"
                    )
                    continue

                abi = get_abi(contract_address=contract_address)

                contract = web3.eth.contract(address=web3.toChecksumAddress(contract_address), abi=abi)

                if isinstance(abi, str):
                    abi = json.loads(abi)

                contract_functions = [function.get("name") for function in abi if function.get("name")]

                name_contract = get_name_contract(contract, contract_address)

                data_token_collection = analise_contract_and_get_data(
                    contract=contract,
                    contract_address=contract_address,
                    token_id=id_token,
                    name_contract=name_contract,
                    contract_functions=contract_functions,
                )
                if not data_token_collection:
                    continue

                name_coll = data_token_collection[1]
                description_coll = data_token_collection[2]
                ts = data_token_collection[3]
                path_json = data_token_collection[8]
                attr_dict = data_token_collection[5]
                image_url = data_token_collection[7]
                release = data_token_collection[6]
                owner_count = data_token_collection[4]
                image_path = data_token_collection[9]

                print(f"Data TrSingle - trx_hash({transaction}): -- {data_token_collection}")

                write_db_collection_and_token(
                    name_collection=name_coll,
                    id_token=id_token,
                    contract_address=contract_address,
                    description_collection=description_coll,
                    total_supply=ts,
                    path_schema=path_json,
                    attributes_nft=attr_dict,
                    image_url=image_url,
                    release_date=release,
                    owner_count=owner_count,
                    image_path=image_path,
                )

                tokens = [id_token]
                write_db_log(
                    trx=transaction,
                    sender=sender,
                    recipient=recipient,
                    log_index=log_index,
                    tokens=tokens,
                    contract_address=contract_address,
                    is_contract_exchange_tx=is_contract_marketplace_tx,
                    name_event=name_event,
                )

                print(f"NFT written(TrSingle)(trx_hash: {transaction}) - {id_token}, {name_coll}, {contract_address}")
            elif lg["topics"][0] == TransferBatch:
                print(f"Find event TransferBatch | trx_hash: {transaction}")
                name_event = "TransferBatch"
                log_index = lg["logIndex"]
                contract_address = lg["address"].lower()

                abi = get_abi(contract_address=contract_address)
                contract = web3.eth.contract(address=web3.toChecksumAddress(contract_address), abi=abi)
                if isinstance(abi, str):
                    abi = json.loads(abi)
                contract_functions = [function.get("name") for function in abi if function.get("name")]
                name_contract = get_name_contract(contract, contract_address)

                data_in_log = transfer_batch(
                    web3=web3, contract=contract, contract_address=contract_address, log_to_process=lg
                )

                if data_in_log is None:
                    continue

                token_id_count_one_transfer = data_in_log[0]
                sender = data_in_log[1]
                recipient = data_in_log[2]

                if len(token_id_count_one_transfer) == 0:
                    continue

                bad_event_flag = False
                tokens = []
                for token_id in token_id_count_one_transfer:

                    if check_exist_nft_1155(token_id=token_id, contract_address=contract_address):
                        print(
                            f"This NFT already exist DataBase | trx_hash: {transaction} , token_id: {token_id},"
                            f"contract_address: {contract_address}"
                        )
                        continue

                    data_token_collection = analise_contract_and_get_data(
                        contract=contract,
                        contract_address=contract_address,
                        token_id=token_id,
                        name_contract=name_contract,
                        contract_functions=contract_functions,
                    )

                    if not data_token_collection:
                        bad_event_flag = True
                        break

                    name_coll = data_token_collection[1]
                    description_coll = data_token_collection[2]
                    ts = data_token_collection[3]
                    path_json = data_token_collection[8]
                    attr_dict = data_token_collection[5]
                    image_url = data_token_collection[7]
                    release = data_token_collection[6]
                    owner_count = data_token_collection[4]
                    image_path = data_token_collection[9]

                    print(f"Data TrBatch - trx_hash({transaction}): -- {data_token_collection}")

                    write_db_collection_and_token(
                        name_collection=name_coll,
                        id_token=token_id,
                        contract_address=contract_address,
                        description_collection=description_coll,
                        total_supply=ts,
                        path_schema=path_json,
                        attributes_nft=attr_dict,
                        image_url=image_url,
                        release_date=release,
                        owner_count=owner_count,
                        image_path=image_path,
                    )
                    tokens.append(token_id)

                if bad_event_flag or not tokens:
                    continue

                write_db_log(
                    trx=transaction,
                    sender=sender,
                    recipient=recipient,
                    log_index=log_index,
                    tokens=tokens,
                    contract_address=contract_address,
                    is_contract_exchange_tx=is_contract_marketplace_tx,
                    name_event=name_event,
                )
                print(f"NFT written(TrBatch)(trx_hash: {transaction}) - {tokens}, {contract_address}")
            elif lg["topics"][0] == URI:
                print(f"Find event URI | trx_hash: {transaction}")
                name_event = "URI"
                log_index = lg["logIndex"]
                contract_address = lg["address"].lower()

                abi = get_abi(contract_address=contract_address)
                contract = web3.eth.contract(address=web3.toChecksumAddress(contract_address), abi=abi)
                if isinstance(abi, str):
                    abi = json.loads(abi)
                contract_functions = [function.get("name") for function in abi if function.get("name")]
                name_contract = get_name_contract(contract, contract_address)

                data_in_log = get_data_log_uri(
                    web3=web3, contract=contract, contract_address=contract_address, log_to_process=lg
                )

                if data_in_log is None:
                    continue

                id_token = data_in_log[1]

                data_token_collection = analise_contract_and_get_data(
                    contract=contract,
                    contract_address=contract_address,
                    token_id=id_token,
                    name_contract=name_contract,
                    contract_functions=contract_functions,
                )
                if not data_token_collection:
                    continue

                name_coll = data_token_collection[1]
                description_coll = data_token_collection[2]
                ts = data_token_collection[3]
                path_json = data_token_collection[8]
                attr_dict = data_token_collection[5]
                image_url = data_token_collection[7]
                release = data_token_collection[6]
                owner_count = data_token_collection[4]
                image_path = data_token_collection[9]

                print(f"Data URI - trx_hash({transaction}): -- {data_token_collection}")

                write_db_collection_and_token(
                    name_collection=name_coll,
                    id_token=id_token,
                    contract_address=contract_address,
                    description_collection=description_coll,
                    total_supply=ts,
                    path_schema=path_json,
                    attributes_nft=attr_dict,
                    image_url=image_url,
                    release_date=release,
                    owner_count=owner_count,
                    image_path=image_path,
                )

                tokens = [id_token]
                write_db_log(
                    trx=transaction,
                    sender="",
                    recipient="",
                    log_index=log_index,
                    tokens=tokens,
                    contract_address=contract_address,
                    is_contract_exchange_tx=is_contract_marketplace_tx,
                    name_event=name_event,
                )
                print(f"NFT written(URI)(trx_hash: {transaction}) - {id_token}, {name_coll}, {contract_address}")
        except Exception as ex:
            logger.info(f"Error: {ex} | trx_hash: {transaction}")


@app.task(ignore_result=True)
def check_erc_1155(trx_hash: str, trx_to: str, trx_logs: list, event_tx: list):
    try:
        is_contract_marketplace = contracts_marketplace.get(trx_to, "")
        if TransferSingle in event_tx or TransferBatch in event_tx or URI in event_tx:
            get_data_1155.delay(trx_hash, is_contract_marketplace, trx_logs)
    except Exception as ex:
        logger.info(f"Error: {ex} | trx_hash: {trx_hash}")
