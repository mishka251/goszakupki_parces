from typing import Dict
from xml.dom import minidom
import os
from database import orm, Purchase, PoClass, Region, PO
import datetime
import urllib.parse as parse_url
import urllib.request as url_request
from bs4 import BeautifulSoup


def get_okpd2_from_xml(tree) -> str:
    okpd2 = tree.getElementsByTagName('OKPD2')[0]
    code = okpd2.getElementsByTagName('code')[0]
    return code.childNodes[0].data


def get_name_from_xml_file(tree) -> str:
    name = tree.getElementsByTagName('purchaseObjectInfo')[0]
    return name.childNodes[0].data


def get_date_from_xml_file(tree) -> str:
    date = tree.getElementsByTagName('docPublishDate')[0]
    return date.childNodes[0].data


def get_price_from_xml_file(tree) -> str:
    price = tree.getElementsByTagName('maxPrice')[0]
    return price.childNodes[0].data


def get_purchase_object(tree) -> str:
    price = tree.getElementsByTagName('purchaseObjectInfo')[0]
    purchase_object: str = price.childNodes[0].data
    return purchase_object


def get_po_name(purchase_object: str) -> str:
    """
    Поиск имени ПО в объекте закупки(такое себе, но что есть)
    :param purchase_object: объект закупки
    :return:
    """
    r_quote = purchase_object.rfind("\"")
    l_quote = purchase_object.find("\"")
    if r_quote != l_quote and l_quote != -1 and r_quote != -1:
        name = purchase_object[purchase_object.find("\"") + 1:purchase_object.rfind("\"")]
        return name

    l_quote = purchase_object.find("«")
    r_quote = purchase_object.find("»")
    if r_quote != l_quote and l_quote != -1 and r_quote != -1:
        name = purchase_object[purchase_object.find("«") + 1:purchase_object.rfind("»")]
        return name

    return purchase_object


def check_is_russian(po_name: str) -> bool:
    """
    Проверка является ли ПО российским
    :param po_name: название по
    :return:
    """
    url = "https://reestr.minsvyaz.ru/reestr/?"
    data = {
        "name": po_name,
        "show_count": 100,
        "set_filter": "Y"
    }

    url += parse_url.urlencode(data)
    page = url_request.urlopen(url).read()
    soap = BeautifulSoup(page)
    results = soap.find('div', {'class': 'result_area'})
    return results is not None


def get_data_from_xml(file_path: str) -> Dict:
    """
    олучение данных о закупке из файла
    :param file_path: путь к файлу
    :return: словарь с окпд2, названием(лишнее, но уже не буду убирать), датой, ценой,
     объектов закупки, названием по(получено плохо, но как есть) и является ли ПО российским
    """
    tree = minidom.parse(file_path)
    okpd2 = get_okpd2_from_xml(tree)
    name = get_name_from_xml_file(tree)
    date_str = get_date_from_xml_file(tree)
    date = datetime.datetime.fromisoformat(date_str)
    price = get_price_from_xml_file(tree)
    obj = get_purchase_object(tree)
    po_name = get_po_name(purchase_object=obj)
    is_russian = check_is_russian(po_name)

    return {
        'okpd2': okpd2,
        'name': name,
        'date': date,
        'price': price,
        'object': obj,
        'po_name': po_name,
        'is_russian': is_russian,
    }


def save_file_to_db(filepath: str, region: str):
    """
    Грузим из одного файла в базу
    :param filepath: путь к файлу
    :param region: регион
    :return:
    """
    code: Dict = get_data_from_xml(filepath)
    po_class = PoClass.get(code=code['okpd2'])

    if po_class is None:  # не нашли код - забили
        print(code['okpd2'], "WARNING unknown class")
        return

    if not code['po_name']:  # не нашли имя ПО - забили
        print("NO NAME", code['object'])
        return

    db_region = Region.get(name=region) or Region(name=region)

    po = PO.get(name=code['po_name']) \
         or PO(name=code['po_name'], po_class=po_class, is_russian=code['is_russian'])

    purchase = Purchase.get(name=code['name']) or \
               Purchase(name=code['name'],
                        region=db_region,
                        date=code['date'],
                        price=code['price'],
                        pos=po,
                        object_name=code['object'])


def get_region_codes(region: str):
    """
    Обработка всех загруженных файлов для региона
    :param region:
    :return:
    """
    downloads_dir: str = 'downloads'
    region_dir: str = os.path.join(downloads_dir, region)
    if not os.path.exists(region_dir):
        print(f"Не найдена папка загрузок {region_dir} для региона {region}")
        return

    total: int = len(os.listdir(region_dir))

    for i, folder in enumerate(os.listdir(region_dir)):

        folder_path: str = os.path.join(region_dir, folder)

        if not os.path.isdir(folder_path):
            print(f"Warning, is not dir {folder_path}")
            continue

        for file in os.listdir(folder_path):
            save_file_to_db(os.path.join(folder_path, file), region)

        print(f"{region_name} - {int(100 * i / total)}% added to db")


with orm.db_session:
    for region_name in orm.select(r.name for r in Region):
        get_region_codes(region_name)
