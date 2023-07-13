from celery_main.celery import app
from web3 import Web3
from query_dir.ft import create_coin, create_event_coin, update_coin, check_coin_db
from variables import (
    Transfer_erc_721,
    contract_addresses_kitties,
    contract_addresses_punks,
    events_kitties,
    events_punk,
    contracts_marketplace,
    abi_coins,
    # OWN_NODE_URL,
    OrdersMatched,
    CryptoKitties_contract,
    CryptoPunks_contract,
    POKT_URL,
)
from query_dir.events import add_nft_log_20
from erc_20.get_info_erc_20 import get_data_log, get_data_event_punk, get_data_event_kitti
from loguru import logger

logger.add(
    "ethereum_blockchain/error_logs/errors_erc_20.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="200 MB",
    retention="30 days",
)

web3 = Web3(Web3.HTTPProvider(POKT_URL))

"""Service coins_logs and coins write"""


@app.task(ignore_result=True)
def check_coins(trx_hash: str, logs_trx: list):
    try:
        for lg in logs_trx:
            if Transfer_erc_721 == lg["topics"][0] and len(lg["topics"]) == 3:
                log_index = lg["logIndex"]
                contract_address = lg["address"].lower()
                sender = "0x" + lg["topics"][1][-40:]
                recipient = "0x" + lg["topics"][2][-40:]
                value = int("0x" + lg["data"][-32:], 16)

                get_info_token.delay(contract_address, log_index, sender, recipient, value, trx_hash)
    except Exception as ex:
        logger.info(f"Error: {ex} | trx_hash: {trx_hash}")


@app.task(ignore_result=True)
def get_info_token(contract_address, log_index, sender, recipient, value, trx_hash):
    try:
        contract = web3.eth.contract(address=web3.toChecksumAddress(contract_address), abi=abi_coins)
        try:
            name_token = contract.functions.name().call()

            if isinstance(name_token, bytes):
                name_token = name_token.decode("utf-8")
            name_token = name_token.rstrip("\x00")
            name_token = name_token.replace("'", "").strip()

            total_supply = contract.functions.totalSupply().call()
            decimals = contract.functions.decimals().call()
            div = 10**decimals
            total_supply = total_supply / div
        except Exception:
            print(f"Bad coin {contract_address}")
            return

        div = 10**decimals

        exists_coin_db = check_coin_db(contract_address=contract_address)

        if not exists_coin_db:
            create_coin(
                contract_address=contract_address,
                name=name_token,
                total_supply=float(total_supply),
                decimals=decimals,
            )
        else:
            update_coin(
                contract_address=contract_address,
                total_supply=float(total_supply),
            )

        create_event_coin(
            trx_hash=trx_hash,
            log_index=log_index,
            contract_address=contract_address,
            addr_from=sender,
            addr_to=recipient,
            value=value / div,
        )

        print(f"Success write log - trx_hash {trx_hash}")
    except Exception as ex:
        logger.info(f"Error:{ex} | trx_hash: {trx_hash}")


"""###################################################################"""

"""Tasks erc-20 NFT(CryptoPunks and CryptoKitties)"""


@app.task(ignore_result=True)
def get_data_20(lg: dict, tx_hash: str, tx_to: str, event: str):
    try:
        if event == "CryptoKitties":
            log_index, contract_address, event_name = get_data_log(lg, events_kitties)

            sender, recipient, token, name_marketplace, price_eth = get_data_event_kitti(lg, event_name, tx_to)

            add_nft_log_20(
                trx_hash=tx_hash,
                log_index=log_index,
                contract_addr=contract_address,
                event_name=event_name,
                addr_from=sender,
                addr_to=recipient,
                token_id=token,
                name_marketplace=name_marketplace,
                price_eth=price_eth,
            )
            print(f"Write EventKitties | trx_hash: {tx_hash}")
        elif event == "CryptoPunks":
            log_index, contract_address, event_name = get_data_log(lg, events_punk)
            sender, recipient, token, name_marketplace, price_eth = get_data_event_punk(lg, event_name, tx_to)
            add_nft_log_20(
                trx_hash=tx_hash,
                log_index=log_index,
                contract_addr=tx_to.lower(),
                event_name=event_name,
                addr_from=sender,
                addr_to=recipient,
                token_id=token,
                name_marketplace=name_marketplace,
                price_eth=price_eth,
            )
            print(f"Write EventPunk | trx_hash: {tx_hash}")
        elif event == "OrderMatched":
            log_index = lg["logIndex"]
            price = int("0x" + lg["data"][-32:], 16) / (10**18)
            contract_address = lg["address"].lower()
            sender = "0x" + lg["topics"][1][-40:]
            recipient = "0x" + lg["topics"][2][-40:]
            name_marketplace = contracts_marketplace.get(contract_address, "")

            token = [""]

            add_nft_log_20(
                trx_hash=tx_hash,
                log_index=log_index,
                contract_addr=contract_address,
                event_name="OrdersMatched",
                addr_from=sender,
                addr_to=recipient,
                token_id=token,
                name_marketplace=name_marketplace,
                price_eth=price,
            )
            print(f"Write event OrderMatched | trx_hash: {tx_hash}")
    except Exception as ex:
        logger.info(f"Error: {ex} | trx_hash: {tx_hash}")


@app.task(ignore_result=True)
def check_erc_20(trx_hash: str, trx_to: str, trx_logs: list):
    try:
        for lg in trx_logs:
            contract_address = lg["address"].lower()
            event_kitties = (
                events_kitties.get(str(lg["topics"][0]), "") != ""
                and contract_addresses_kitties.get(lg["address"].lower(), "") != ""
            )
            event_punks = (
                events_punk.get(str(lg["topics"][0]), "") != ""
                and contract_addresses_punks.get(lg["address"].lower(), "") != ""
            )
            order_matched = str(lg["topics"][0]) == OrdersMatched
            if event_kitties and contract_address == CryptoKitties_contract:
                print(f"Find event CryptoKitties | trx_hash: {trx_hash}")
                get_data_20.delay(lg=lg, tx_hash=trx_hash, tx_to=trx_to, event="CryptoKitties")
            elif event_punks and contract_address == CryptoPunks_contract:
                print(f"Find event CryptoPunks | trx_hash: {trx_hash}")
                get_data_20.delay(lg=lg, tx_hash=trx_hash, tx_to=trx_to, event="CryptoPunks")
            elif order_matched:
                print(f"Find event OrderMatched | trx_hash: {trx_hash}")
                get_data_20.delay(lg=lg, tx_hash=trx_hash, tx_to=trx_to, event="OrderMatched")
    except Exception as ex:
        logger.info(f"Error: {ex} | trx_hash: {trx_hash}")
