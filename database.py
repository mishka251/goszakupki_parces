from datetime import date
from pony import orm

"""Файл для работы с БД. Тут описание таблиц и подключение к БД"""

database = orm.Database()


class Region(database.Entity):
    """
    Регион
    :name - название латиницей
    :readable_name - название на русском
    :purchases - список закупок в регионе
    """
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str, unique=True)
    readable_name = orm.Optional(str)
    purchases = orm.Set("Purchase")


class Purchase(database.Entity):
    """
    Закупка
    """
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str)
    date = orm.Required(date)
    price = orm.Required(float)
    pos = orm.Required("PO")
    region = orm.Required(Region)
    object_name = orm.Required(str)
    is_finished = orm.Optional(bool)


class Classifier(database.Entity):
    """
    Классификатор
    """
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str)
    classes = orm.Set("PoClass")


class PoClass(database.Entity):
    """
    Класс ПО - код
    """
    id = orm.PrimaryKey(int, auto=True)
    code = orm.Required(str, unique=True)
    #
    po = orm.Set("PO")
    classifier = orm.Set(Classifier)


class PO(database.Entity):
    """
    ПО
    """
    id = orm.PrimaryKey(int, auto=True)
    po_class = orm.Required(PoClass)
    name = orm.Required(str, unique=True)
    is_russian = orm.Required(bool, default=False)
    purchase = orm.Set(Purchase)


database.bind(provider='sqlite', filename='database.sqlite', create_db=True)
database.generate_mapping(create_tables=True)
