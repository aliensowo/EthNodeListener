from celery_main.tasks_services import (
    main_image_path_service,
    main_rarity_service,
    get_mar_cup_avg,
    get_sales_vol_floor_pr,
    owners_count_all_collection,
)
from services.update_image_collections import update_image_collection_func
import time


"""
    Скрипт запускает таски celery на разных workers ( update_rarity_service, update_image_path_service,  )
    И запускает функцию обновления image_collection
    Обновляет данные каждые 24 часа в БД
    Rarity ( celery -A celery_main worker --loglevel=INFO -n update_rarity -Q rarity )
    Finance Statistic 1) ( celery -A celery_main worker --loglevel=INFO -n fin_stat_cap_avg -Q market_cap )
                      2) ( celery -A celery_main worker --loglevel=INFO -n fin_stat_sal_vol_fl -Q sales_vol_floor )
    Update image_path( celery -A celery_main worker --loglevel=INFO -n update_image_path -Q image_path )
    Update owner_count tokens collection
    ( celery -A celery_main worker --loglevel=INFO -n update_owners_count -Q check_owner )
    Update image_collection (simple function)(import image_nfts_collections update_image_collection_func)
"""


while True:
    start_time = time.time()
    # update fin stat (market_сap, avg) and (sales, volume, ...)
    # update market_сap and avg_price (task)
    get_mar_cup_avg.delay()
    # update_count_owner
    owners_count_all_collection.delay()
    # update rarity (task)
    main_rarity_service.delay()
    # update image_path (task)
    main_image_path_service.delay()
    # update sales, volume and floor_price (synchronous function inside tasks)
    get_sales_vol_floor_pr()
    # image_path_collection (synchronous function)
    update_image_collection_func()

    end_time = time.time()

    if (end_time - start_time) < 86400:
        time.sleep(int(86400 - (end_time - start_time)))
