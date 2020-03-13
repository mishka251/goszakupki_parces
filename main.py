import operator
from typing import List, Collection, Dict, Tuple
import sys
import datetime
from enum import Enum
from dateutil.relativedelta import relativedelta
from PyQt5 import QtCore, QtWidgets, uic
from PyQt5.QtWidgets import QMessageBox, QCompleter

import matplotlib

matplotlib.use('QT5Agg')
import matplotlib.pylab as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

from database import Classifier, orm, Region, Purchase

# временные интервалы
timeintervals = {
    "последний месяц": relativedelta(months=1),
    "последние 2 месяца": relativedelta(months=2),
    "последние 3 месяца": relativedelta(months=3),
    "последние 4 месяца": relativedelta(months=4),
    "последние 5 месяцев": relativedelta(months=5),
    "последние 6 месяцев": relativedelta(months=6),
    "последние 7 месяцев": relativedelta(months=7),
    "последние 8 месяцев": relativedelta(months=8),
    "последние 9 месяцев": relativedelta(months=9),
    "последние 10 месяцев": relativedelta(months=10),
    "последние 11 месяцев": relativedelta(months=11),
    "последний год": relativedelta(months=12),
    "последние 2 года": relativedelta(years=2),
    "последние 3 года": relativedelta(years=3),
    "последние 4 года": relativedelta(years=4),
    "последние 5 лет": relativedelta(years=5),
}

ALL_REGIONS: str = 'все'


class CalculateBy(Enum):
    """
    по количеству или сумме
    """
    count = 1
    sum = 2


class PurchaseView(object):
    is_russian: bool
    price: float
    # code: str
    region: str
    date: datetime.date

    def __init__(self, rus: bool, price: float, region: str, date: datetime.date):
        self.date = date
        self.region = region
        self.price = price
        self.is_russian = rus
        # self.code = code


def log_uncaught_exceptions(ex_cls, ex, tb):
    """
    Эта для обработки ошибок, скопировано с инета
    :param ex_cls:
    :param ex:
    :param tb:
    :return:
    """
    text = '{}: {}:\n'.format(ex_cls.__name__, ex)
    import traceback
    text += ''.join(traceback.format_tb(tb))

    print(text)
    QMessageBox.critical(None, 'Error', text)
    quit()


sys.excepthook = log_uncaught_exceptions


def get_rus_po_perc(purchases: List[Purchase]) -> float:
    """
    % российского ПО в закупках
    :param purchases: коллекция(список или ещё что) закупок
    :return: число процентов от 0 до 100
    """
    rus_po_count = 0
    for purchase in purchases:
        if purchase.pos.is_russian:
            rus_po_count += 1

    rus_po_percent = 100 * rus_po_count / len(purchases)
    return rus_po_percent


def get_purchases(region_name: str, po_class_name: str, period: str) -> List[PurchaseView]:
    """
    Получение закупок по параметрам
    :param region_name: интересующий регион(или все)
    :param po_class_name: класс ПО
    :param period: период
    :return: список закупок
    """

    date_end = datetime.date.today()
    date_start = date_end - timeintervals[period]

    with orm.db_session:
        classifier = Classifier.get(name=po_class_name)
        po_codes = classifier.classes
        purchases = orm.select(
            (purchase.region.readable_name, purchase.price, purchase.date, purchase.pos.is_russian)
            for purchase in Purchase
            if purchase.pos.po_class in po_codes
            and purchase.date >= date_start)
        if region_name != ALL_REGIONS:
            purchases = purchases.where(lambda purchase: purchase.region.readable_name == region_name)

        def create_view(purchase: Tuple) -> PurchaseView:
            return PurchaseView(purchase[3], purchase[1], purchase[0], purchase[2])

        return list(map(create_view, purchases))


def calculate(purchases: Collection[PurchaseView], region_name: str, by: CalculateBy = CalculateBy.count) -> Dict:
    """
    Вычисление статистики
    :param purchases: коллекция закупок
    :param region_name: название региона(или все) - от этого зависит по региону или месяцу считаем
    :param by: по сумме или кол-ву
    :return:
    """
    with orm.db_session:
        if region_name == ALL_REGIONS:
            regions_cnt = {}
            for purchase in purchases:
                with orm.db_session:
                    region = purchase.region
                with orm.db_session:
                    value = purchase.price if by == CalculateBy.sum else 1
                if region in regions_cnt:
                    regions_cnt[region] += value
                else:
                    regions_cnt[region] = value
            return regions_cnt
        else:
            months_cnt = {}
            for purchase in purchases:
                with orm.db_session:
                    month = purchase.date.strftime("%Y.%m")
                with orm.db_session:
                    value = purchase.price if by == CalculateBy.sum else 1
                if month in months_cnt:
                    months_cnt[month] += value
                else:
                    months_cnt[month] = value
            return months_cnt


class MyWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(MyWindow, self).__init__()
        uic.loadUi('form1.ui', self)

        with orm.db_session:
            classes: List[str] = list(orm.select(c.name for c in Classifier))
            for classifier in classes:
                self.comboBox_5.addItem(classifier)

            regions: List[str] = list(orm.select(r.readable_name for r in Region))
            regions.insert(0, ALL_REGIONS)
            completer = QCompleter(regions, self)
            completer.setCaseSensitivity(False)
            completer.setFilterMode(QtCore.Qt.MatchContains)
            self.plainTextEdit.setCompleter(completer)

        for key in timeintervals.keys():
            self.comboBox.addItem(key)

        self.button.clicked.connect(lambda: self.click_handler())
        self.lay = QtWidgets.QVBoxLayout(self.content_plot)
        self.lay.setContentsMargins(0, 0, 0, 0)
        self.plotWidget = None

    def plot(self, data, title):
        if self.plotWidget is not None:
            self.lay.removeWidget(self.plotWidget)

        fig, ax = plt.subplots()
        fig.subplots_adjust(0.2, 0.4, 0.9, 0.9)
        keys = list(data.keys())
        values: List[float] = [data[k] for k in keys]
        x = [i for i in range(len(values))]
        ax.bar(x, values)
        ax.set_title(title, fontdict={'fontsize': 8})
        #pos, x_tick_labels = plt.xticks(x, keys)
        #plt.setp(x_tick_labels, rotation=60, fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(keys, rotation=45, fontsize='small', ha='right')
        # plot
        self.plotWidget = FigureCanvas(fig)

        self.lay.addWidget(self.plotWidget)

    def click_handler(self):
        region_name: str = self.plainTextEdit.text()
        po_class_name: str = self.comboBox_5.currentText()
        period: str = self.comboBox.currentText()
        by: CalculateBy = CalculateBy.sum if self.comboBox_4.currentText() == "По стоимости" else CalculateBy.count
        purchases: List[PurchaseView] = get_purchases(region_name, po_class_name, period)
        d = calculate(purchases, region_name, by)

        if region_name==ALL_REGIONS:
            d = dict(sorted(d.items(), key=operator.itemgetter(1), reverse=True)[:10])


        with orm.db_session:
            percent = get_rus_po_perc(orm.select(p for p in Purchase))

        print(d, percent)
        self.lineEdit.setText(str(percent)[:6])
        param = "Стоимость" if self.comboBox_4.currentText() == "По стоимости" else "Количество"
        region = "всех регионах" if region_name == 'все' else region_name
        title = f"{param} закупок ПО \n в {region} \n за {period}"
        self.plot(d, title)


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec_())
