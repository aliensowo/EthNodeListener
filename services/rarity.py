from collections import Counter
from math import log
from typing import Dict, List, Tuple

# # # # # # # #
#
# Импортируйте get_rarity_collection для получения схемы вероятностей параметров, которую можно добавлять в бд
# Импортируйте update_rarity_nft для получения редкости конкретной нфт
#
# # # # # # # #


def get_rarity_collection(nft_collection_list: List) -> Dict:
    """
    Метод считает рарити всей схемы коллеции

    :param nft_collection_list: список диктов атрибутов
    :return:
    """
    attributes_schema_dict: dict = {}
    for nft in nft_collection_list:
        for param, key in nft[1].items():
            if type(key) == list:
                try:
                    attributes_schema_dict[param] = attributes_schema_dict[param] + key
                except KeyError:
                    attributes_schema_dict[param] = key
                continue
            if type(key) == dict:
                for key_standard, _ in key.items():
                    key = key_standard
            try:
                attributes_schema_dict[param].append(key)
            except KeyError:
                attributes_schema_dict[param] = [key]

    return rarity_calculate(schema=attributes_schema_dict)


def update_rarity_nft(nft: Dict, rarity_schema: Dict) -> Tuple:
    """
    Добавляем рарити для каждого атрибута нфт

    :param nft: словарь с полями нфт
    :param rarity_schema: схема готовой рарити
    :return:
    """
    rarity = 1
    result_dict: dict = {"attributes": {}}
    for param, key in nft.items():
        if type(key) == dict:
            for key_standart, _ in key.items():
                key = key_standart
        if type(key) == list:
            result_dict["attributes"][param] = {}
            for k in key:
                result_dict["attributes"][param][k] = rarity_schema[param][k]
                rarity += abs(log(rarity_schema[param][k]))
            continue
        result_dict["attributes"][param] = {key: rarity_schema[param][key]}
        rarity += abs(log(rarity_schema[param][key]))
    return result_dict["attributes"], round(rarity, 2)


def rarity_calculate(schema: Dict) -> Dict:
    """
    Подствет вероятностей выпадения для всех значений в параметрах

    :param schema:
    :return:
    """
    counter: dict = {}
    for param, value_list in schema.items():
        variable = len(value_list)
        counter[param] = dict(Counter(value_list))
        for value, value_count in counter[param].items():
            counter[param][value] = round(value_count / variable, 6)
    return counter
