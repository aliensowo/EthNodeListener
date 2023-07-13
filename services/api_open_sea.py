import requests
from loguru import logger
import json
from query_dir.collections import update_collection_open_sea
from query_dir.nft import get_random_token_id_coll
from variables import proxies

headers = {"x-api-key": "2f6f419a083c46de9d83ce3dbe7db601"}


def get_all_stat_api(contract_address: str, name_coll: str):
    status_code = None
    error_count = 0
    token_id = get_random_token_id_coll(name_col=name_coll, contr_addr=contract_address)
    if not token_id:
        return None

    try:
        while status_code != 200 and error_count < 10:
            try:
                print(f"Request API OpenSea. Contract_address: {contract_address} | token_id :{token_id}")
                r = requests.get(
                    f"https://api.opensea.io/api/v1/asset/{contract_address}/{token_id}",
                    headers=headers,
                    proxies=proxies,
                )
                status_code = r.status_code
                if status_code == 404:
                    break
            except Exception as ex:
                logger.info(f"Error request OpenSe API: TypeError: {type(ex)} | Error: {ex}")
                error_count += 1

        if error_count == 10 or status_code == 404:
            print(f"No response API OpenSea. Contract_address: {contract_address} | token_id :{token_id}")
            return None

        param = r.content
        info = json.loads(param)

        stats = info["collection"]["stats"]
        volume = stats["one_day_volume"]
        sales = stats["one_day_sales"]
        floor_price = stats["floor_price"]
        avg_price = stats["average_price"]
        market_cap = stats["market_cap"]
        owners_count = stats["num_owners"]
        total_supply = stats["total_supply"]

        update_collection_open_sea(
            contract_addr=contract_address,
            name_collection=name_coll,
            total_supply=total_supply,
            owner_count=owners_count,
            market_cap=market_cap,
            avg_price=avg_price,
            sales=sales,
            volume=volume,
            floor_price=floor_price,
        )
        print(
            f"Get Data API OpenSea. Name_collection: {name_coll} | Contract_address: {contract_address} |"
            f"Market_cap: {market_cap} | Avg_price: {avg_price} | Sales: {sales} | Volume: {volume} |"
            f"Floor_price: {floor_price}"
        )
    except Exception as ex:
        logger.info(f"Error requests OpenSea API finance statistic. Type: {type(ex)} | Error: {ex}")
