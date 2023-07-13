from query_dir.nft import get_image_collection
from query_dir.collections import get_coll_empty_image, update_image_collection

"""
    Запись image_collection(image_path(random))
"""


def update_image_collection_func():
    collections = get_coll_empty_image()
    for collection in collections:
        name_col = collection[0]
        contr_addr = collection[1]
        image_coll = get_image_collection(name_col=name_col, contr_addr=contr_addr)
        if image_coll:
            update_image_collection(name=name_col, contract_addr=contr_addr, image_path=image_coll)
