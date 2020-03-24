import sys
from PyQt5 import QtWidgets, uic
from PyQt5.QtWidgets import QTableWidgetItem

from database import orm, Purchase
from statistic import MyWindow


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        uic.loadUi('MainWindow.ui', self)
        self.pushButton.clicked.connect(self.open_statistic)

        with orm.db_session:
            purchases = orm.select(
                (p.pos.name, p.pos.po_class.code, p.pos.po_class.classifier.name, p.price, p.region.readable_name) for p
                in Purchase)
            purchases_list = list(purchases)
        self.tableWidget.setRowCount(len(purchases_list))
        for i, row in enumerate(purchases_list):
            for j, value in enumerate(row):
                cell = QTableWidgetItem(str(value))
                self.tableWidget.setItem(i, j, cell)

    def open_statistic(self):
        statistic = MyWindow()
        statistic.show()


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
