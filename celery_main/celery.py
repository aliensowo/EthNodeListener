from celery import Celery
from variables import RABBIT_MQ, RABBIT_MQ_LOCAL

# Optional configuration, see the application user guide.

task_routes = {
    "celery_main.tasks_services.get_mar_cup_avg_owner_count": {
        "queue": "market_cap",
    },
    "celery_main.tasks_services.analise_collection": {
        "queue": "market_cap",
    },
    "celery_main.tasks_services.analise_block": {
        "queue": "sales_vol_floor",
    },
    "celery_main.tasks_services.main_image_path_service": {
        "queue": "image_path",
    },
    "celery_main.tasks_services.get_db_service_download_image": {
        "queue": "image_path",
    },
    "celery_main.tasks_services.owners_count_all_collection": {
        "queue": "check_owner",
    },
    "celery_main.tasks_services.check_owner_address": {
        "queue": "check_owner",
    },
    "celery_main.tasks_services.main_rarity_service": {
        "queue": "rarity",
    },
    "celery_main.tasks_services.rarity_collection": {
        "queue": "rarity",
    },
    "celery_main.tasks_services.rarity_nft": {
        "queue": "rarity",
    },
    "celery_main.tasks_services.write_data_trx_db": {
        "queue": "write_db_trx",
    },
    "celery_main.tasks_20.check_coins": {
        "queue": "coins",
    },
    "celery_main.tasks_20.get_info_token": {
        "queue": "coins",
    },
    "celery_main.tasks_20.check_erc_20": {
        "queue": "erc-20",
    },
    "celery_main.tasks_20.get_data_20": {
        "queue": "erc-20",
    },
    "celery_main.tasks_1155.check_erc_1155": {
        "queue": "erc-1155",
    },
    "celery_main.tasks_1155.get_data_1155": {
        "queue": "erc-1155",
    },
    "celery_main.tasks_721.check_erc_721": {
        "queue": "erc-721",
    },
    "celery_main.tasks_721.parsing_collection_erc_721": {
        "queue": "erc-721",
    },
    "celery_main.tasks_721.get_urls_721": {
        "queue": "erc-721",
    },
    "celery_main.tasks_721.get_nft_metadata": {
        "queue": "erc-721",
    },
    "celery_main.tasks_721.save_nft_metadata": {
        "queue": "erc-721",
    },
    "celery_main.tasks_721.save_nft_pictures": {
        "queue": "erc-721",
    },
    "celery_main.tasks_721.save_nfts_to_db": {
        "queue": "erc-721",
    },
}

app = Celery(
    "celery_main",
    backend="rpc://",
    broker=RABBIT_MQ_LOCAL,
    include=["celery_main.tasks_services", "celery_main.tasks_721", "celery_main.tasks_1155", "celery_main.tasks_20"],
    task_routes=task_routes,
)

app.conf.update(
    result_expires=3600,
)


if __name__ == "__main__":
    app.start()
