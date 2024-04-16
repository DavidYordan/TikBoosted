from datetime import date
from PyQt6.QtCore import (
    pyqtSlot,
    QObject,
    Qt
)
from PyQt6.QtGui import (
    QColor,
    QIntValidator
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget
)

from globals import Globals
from operate_sqlite import DBSchema

class AccountsDetailsTab(QWidget):
    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        self.columns = list(DBSchema.tables['accounts_detail'].keys()) + list(DBSchema.tables['accounts'].keys())
        self.user = 'AccountsDetailsTab'
        self.sorted_columns()
        self.setup_ui()
        self.reload()

        Globals._Log.info(self.user, 'Successfully initialized.')

    def cell_was_clicked(self):
        pass

    def cell_was_double_clicked(self):
        current_index = self.table.currentIndex()
        row = current_index.row()
        account = self.table.item(row, self.columns.index('uniqueId')).text()
        Globals._WS.list_videos_right_signal.emit(account)

    # def cell_was_double_clicked(self):
    #     index = self.table.currentIndex()
    #     uniqueId = self.table.item(index.row(), self.columns.index('uniqueId')).text()
    #     dialog = IntegerInputDialog(self)
    #     num, accepted = dialog.getInteger()
    #     if accepted:
    #         Globals._WS.insert_smmsky_task_signal.emit((1, ('add_order', {
    #             'action': 'add',
    #             'service': 4198,
    #             'link': f'https://www.tiktok.com/@{uniqueId}',
    #             'quantity': num
    #         })))
            # Globals._WS.insert_smmsky_task.emit((1, ('add_order', {
            #     'action': 'add',
            #     'service': 3558,
            #     'link': f'https://www.tiktok.com/@{uniqueId}',
            #     'quantity': num
            # })))

    def reload(self):
        accounts_detail_fields = Globals._SQL.get_table_fields('accounts_detail')
        accounts_detail_datas = Globals._SQL.read('accounts_detail')
        today_date = date.today().isoformat()

        if not accounts_detail_datas:
            self.table.setRowCount(0)
            return
        
        try:
            self.table.setSortingEnabled(False)
            self.table.setRowCount(len(accounts_detail_datas))
            for row, data in enumerate(accounts_detail_datas):
                account = data[accounts_detail_fields.index('uniqueId')]
                account_data = Globals.accounts_dict.get(account, {})
                for idx, field in enumerate(self.columns):
                    if field == 'latestEarningDay':
                        if account_data.get('latestEarningDay', '') != today_date:
                            account_data['todayEarnings'] = 0
                    if field in accounts_detail_fields:
                        item = QTableWidgetItem(str(data[accounts_detail_fields.index(field)]))
                    else:
                        item = QTableWidgetItem(str(account_data.get(field, '')))
                    self.table.setItem(row, idx, item)
            self.table.setSortingEnabled(True)
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')

    def setup_ui(self):
        layout = QVBoxLayout(self)

        top_layout = QHBoxLayout()
        layout.addLayout(top_layout)
        button_update_accounts = QPushButton('Update Accounts')
        Globals.components.append(button_update_accounts)
        # button_update_accounts.clicked.connect(self.update_accounts)
        top_layout.addWidget(button_update_accounts)
        button_update_earnings = QPushButton('Update Earnings')
        Globals.components.append(button_update_earnings)
        # button_update_earnings.clicked.connect(self.update_earnings)
        top_layout.addWidget(button_update_earnings)
        button_reload = QPushButton('Reload')
        Globals.components.append(button_reload)
        button_reload.clicked.connect(self.reload)
        top_layout.addWidget(button_reload)
        top_layout.addStretch()
        button_save = QPushButton('Save')
        # button_save.clicked.connect(self.save_data_to_file)
        top_layout.addWidget(button_save)

        middle_layout = QHBoxLayout()
        layout.addLayout(middle_layout)
        self.table = QTableWidget(0, len(self.columns))
        middle_layout.addWidget(self.table)
        header_labels = self.columns
        self.table.setHorizontalHeaderLabels(header_labels)
        
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 10)

        self.table.mousePressEvent = self.table_mouse_press_event
        self.table.cellClicked.connect(self.cell_was_clicked)
        self.table.doubleClicked.connect(self.cell_was_double_clicked)

    def show_context_menu(self, pos, index):
        menu = QMenu()

        row = index.row()
        account = self.table.item(row, self.columns.index('account')).text()

        # action_show_user_details = menu.addAction('Show Details')
        # action_show_user_details.triggered.connect(lambda _, r=row: self.show_user_details(r))
        
        action_show_videos = menu.addAction('Show Videos')
        action_show_videos.triggered.connect(lambda _, a=account: Globals._WS.list_videos_right_signal.emit(a))

        tk_link = f'https://www.tiktok.com/@{account}'
        action_copy_tk_link = menu.addAction('Copy TK Link')
        action_copy_tk_link.triggered.connect(lambda: QApplication.clipboard().setText(tk_link))

        rs_link = self.table.item(row, self.columns.index('referralLink')).text()
        if rs_link:
            if not rs_link.startswith('https://'):
                rs_link = 'https://' + rs_link
            action_copy_rs_link = menu.addAction('Copy Invitation Link')
            action_copy_rs_link.triggered.connect(lambda: QApplication.clipboard().setText(rs_link))

        menu.exec(self.table.viewport().mapToGlobal(pos))

    def sorted_columns(self):
        priority_elements = ['uniqueId', 'followerCount', 'heartCount', 'videoCount', 'nickname', 'link', 'risk', 'signature']
        columns = priority_elements + [x for x in self.columns if x not in priority_elements]
        self.columns = columns

    def table_mouse_press_event(self, event):
        if event.button() == Qt.MouseButton.RightButton:
            index = self.table.indexAt(event.pos())
            if not index.isValid():
                return
            else:
                self.show_context_menu(event.pos(), index)
        else:
            QTableWidget.mousePressEvent(self.table, event)
        
class IntegerInputDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Input followerCount")
        self.layout = QVBoxLayout()
        
        self.label = QLabel("Enter a number:")
        self.layout.addWidget(self.label)

        self.lineEdit = QLineEdit()
        self.lineEdit.setValidator(QIntValidator(10, 10000))
        self.layout.addWidget(self.lineEdit)
        
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)
        self.layout.addWidget(self.buttonBox)

        self.setLayout(self.layout)
    
    def getInteger(self):
        if self.exec() == QDialog.DialogCode.Accepted:
            return int(self.lineEdit.text()), True
        return 0, False