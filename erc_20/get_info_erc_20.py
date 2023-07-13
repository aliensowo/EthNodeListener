from variables import contracts_marketplace, NULL_ADDRESS

"""
Модуль для сбора информации для информации по ERC-20
"""


def get_data_log(lg, dict_name_event):
    log_index = lg["logIndex"]
    contract_address = lg["address"].lower()
    event_name = dict_name_event.get(lg["topics"][0], "")

    return log_index, contract_address, event_name


def get_data_event_kitti(lg, event_name, tx_to):
    sender = ""
    recipient = ""
    token_id = None
    price_eth = 0

    if event_name == "Transfer":
        if len(lg["topics"]) == 1:
            sender = "0x" + lg["data"][2:66][-40:]
            recipient = "0x" + lg["data"][66:130][-40:]
            token_id = int("0x" + lg["data"][130:], 16)
        elif len(lg["topics"]) == 3:
            sender = "0x" + lg["topics"][1][-40:]
            recipient = "0x" + lg["topics"][2][-40:]
            price_eth = int(lg["data"], 16) / (10**18)
    elif event_name == "BurnTokenAndWithdrawKitty":
        token_id = int(lg["data"], 16)
    elif event_name == "AuctionCreated":
        token_id = int("0x" + lg["data"][2:66], 16)
        price_eth = int("0x" + lg["data"][130:194], 16) / (10**18)
    elif event_name == "AuctionCancelled":
        token_id = int(lg["data"], 16)
    elif event_name == "AuctionSuccessful":
        token_id = int("0x" + lg["data"][2:66], 16)
        price_eth = int("0x" + lg["data"][66:130], 16) / (10**18)
        # winner = '0x' + lg['data'][130:194][-40:]
    elif event_name == "Birth":
        sender = NULL_ADDRESS
        recipient = "0x" + lg["data"][2:66][-40:]
        token_id = int("0x" + lg["data"][66:130], 16)
    elif event_name == "DepositKittyAndMintToken":
        token_id = int(lg["data"], 16)

    if token_id is None:
        token = [""]
    else:
        token = [token_id]

    name_marketplace = contracts_marketplace.get(tx_to.lower(), "")
    if name_marketplace == "":
        name_marketplace = "CryptoKitties"

    return sender, recipient, token, name_marketplace, price_eth


def get_data_event_punk(lg, event_name, tx_to):
    sender = ""
    recipient = ""
    token_id = None
    price_eth = 0

    if event_name == "PunkTransfer":
        sender = "0x" + lg["topics"][1][-40:]
        recipient = "0x" + lg["topics"][2][-40:]
        token_id = int(lg["data"], 16)
    elif event_name == "PunkBidWithdrawn":
        token_id = int(lg["topics"][1], 16)
        sender = "0x" + lg["topics"][2][-40:]
        price_eth = int(lg["data"], 16) / (10**18)
    elif event_name == "PunkOffered":
        token_id = int(lg["topics"][1], 16)
        recipient = "0x" + lg["topics"][2][-40:]
        price_eth = int(lg["data"], 16) / (10**18)
    elif event_name == "PunkNoLongerForSale":
        token_id = int(lg["topics"][1], 16)
    elif event_name == "PunkBought":
        token_id = int(lg["topics"][1], 16)
        sender = "0x" + lg["topics"][2][-40:]
        recipient = "0x" + lg["topics"][3][-40:]
    elif event_name == "Transfer":
        sender = "0x" + lg["topics"][1][-40:]
        recipient = "0x" + lg["topics"][2][-40:]
    elif event_name == "PunkBidEntered":
        token_id = int(lg["topics"][1], 16)
        sender = "0x" + lg["topics"][2][-40:]
        price_eth = int(lg["data"], 16) / (10**18)

    if token_id is None:
        token = [""]
    else:
        token = [token_id]

    name_marketplace = contracts_marketplace.get(str(tx_to).lower(), "")
    if name_marketplace == "":
        name_marketplace = "CryptoPunks"

    return sender, recipient, token, name_marketplace, price_eth
