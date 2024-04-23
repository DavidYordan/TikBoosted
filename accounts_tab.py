import time
from datetime import date
from PyQt6.QtCore import (
    pyqtSignal,
    pyqtSlot,
    Qt
)
from PyQt6.QtGui import (
    QColor
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QHBoxLayout,
    QHeaderView,
    QMenu,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget
)

from accounts_dialog import AccountDialog
from globals import Globals

class AccountsTab(QWidget):
    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        self.columns = [
            'id', 'referralLink', 'link', 'risk', 'tkAccount', 'referralCode', 'customer', 'region', 'team', 'position', 'status', 'rsAccount',
            'latestEarningDay','todayEarnings', 'earningDays', 'totalEarnings', 'account', 'clientRemark', 'serverRemark', 'uniqueId', 
            'heart','followerCount', 'update', 'create', 'videoCount', 'followingCount', 'friendCount', 'diggCount', 'nickname', 'signature', 'ttSeller'
        ]
        self.user = 'AccountsTab'
        self.setup_ui()

        Globals._WS.update_account_earnings_signal.connect(self.update_earnings)
        Globals._WS.update_account_changed_signal.connect(self.update_account_changed)
        Globals._WS.update_account_table_signal.connect(self.update_account_table)

        self.reload()

        Globals._Log.info(self.user, 'Successfully initialized.')
    
    def add_account(self, account, data):
        Globals._MUTEX_ACDICT.lock()
        try:
            row = self.find_row_by_account(account)
            data['createTime'] = int(time.time())
            columns = list(data.keys())
            Globals._WS.database_operation_signal.emit('insert',{
                'table_name': 'accounts',
                'columns': columns,
                'data': data
            }, None)
            Globals.accounts_dict[account] = data
            col = self.columns.index('account')
            self.table.item(row, col).setData(256, None)
            self.table.item(row, col).setBackground(QColor('white'))
            Globals._WS.insert_account_to_tiktok_spider.emit(account)
            Globals._Log.info(self.user, f'Successfully added account {account}.')

        except Exception as e:
            Globals._Log.error(self.user, f'{e}')

        finally:
            Globals._MUTEX_ACDICT.unlock()

    def cell_was_clicked(self):
        current_index = self.table.currentIndex()
        text = self.table.item(current_index.row(), current_index.column()).text()
        Globals._log_label.setText(text)
    
    def cell_was_double_clicked(self):
        current_index = self.table.currentIndex()
        self.show_user_details(current_index.row())

    def delete_account(self, account):
        Globals._MUTEX_ACDICT.lock()
        try:
            Globals._WS.database_operation_signal.emit('delete',{
                'table_name': 'accounts',
                'condition': f'account="{account}"'
            }, None)
            del Globals.accounts_dict[account]
            row = self.find_row_by_account(account)
            if row != -1:
                self.table.removeRow(row)
            Globals._Log.info(self.user, f'Successfully deleted account {account}.')

        except Exception as e:
            Globals._Log.error(self.user, f'{e}')

        finally:
            Globals._MUTEX_ACDICT.unlock()

    def fill_row(self, row, data):
        status = data.get('status', '')
        for col, field in enumerate(self.columns):
            value = data.get(field, '')
            item = QTableWidgetItem(str(value))
            self.table.setItem(row, col, item)
            if status == "5.退學":
                item.setBackground(QColor(192, 192, 192))

    def find_row_by_account(self, account):
        account_col = self.columns.index('account')
        for row in range(self.table.rowCount()):
            if self.get_text(row, account_col) == account:
                return row
        return -1

    def format_create_intervals(self, seconds):
        days, seconds = divmod(seconds, 86400)
        hours, seconds = divmod(seconds, 3600)
        minutes = seconds // 60
        return f"{days:02}d:{hours:02}h:{minutes:02}m"

    def format_update_intervals(self, seconds):
        days, seconds = divmod(seconds, 86400)
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        return f"{hours:02}h:{minutes:02}m:{seconds:02}s"

    def get_text(self, row, column):
        return self.table.item(row, column).text()
    
    def modify_account(self, account, data):
        Globals._MUTEX_ACDICT.lock()
        try:
            Globals._WS.database_operation_signal.emit('update',{
                'table_name': 'accounts',
                'updates': {data[0]: data[1]},
                'condition': f'account="{account}"'
            }, None)
            Globals.accounts_dict[account][data[0]] = data[1]
            row = self.find_row_by_account(account)
            col = self.columns.index(data[0])
            self.table.item(row, col).setText(data[1])
            self.table.item(row, col).setData(256, None)
            self.table.item(row, col).setBackground(QColor('white'))

            Globals._Log.info(self.user, f'')

        except Exception as e:
            Globals._Log.error(self.user, f'{e}')

        finally:
            Globals._MUTEX_ACDICT.unlock()

    def reload(self):
        fields = Globals._SQL.get_table_fields('accounts')
        datas = Globals._SQL.read('accounts')
        today_date = date.today().isoformat()
        now = int(time.time())
        self.table.setRowCount(0)

        if not datas:
            return
        
        self.table.setSortingEnabled(False)
        Globals._MUTEX_ACDICT.lock()
        try:
            Globals.accounts_dict.clear()
            self.table.setRowCount(len(datas))
            for row, data in enumerate(datas):
                account = data[fields.index('account')]
                Globals.accounts_dict[account] = {}
                for idx, field in enumerate(fields):
                    Globals.accounts_dict[account][field] = data[idx]
                if Globals.accounts_dict[account]['latestEarningDay'] != today_date:
                    Globals.accounts_dict[account]['todayEarnings'] = 0

                updateTime = Globals.accounts_dict[account]['updateTime']
                if updateTime:
                    Globals.accounts_dict[account]['update'] = self.format_update_intervals(now - updateTime)

                createTime = Globals.accounts_dict[account]['createTime']
                if createTime:
                    Globals.accounts_dict[account]['create'] = self.format_create_intervals(now - createTime)

                self.fill_row(row, Globals.accounts_dict[account])

        except Exception as e:
            Globals._Log.error(self.user, f'reload: {e}')

        finally:
            self.table.setSortingEnabled(True)
            Globals._MUTEX_ACDICT.unlock()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        top_layout = QHBoxLayout()
        layout.addLayout(top_layout)
        button_update_accounts = QPushButton('Update Accounts')
        Globals.components.append(button_update_accounts)
        button_update_accounts.setEnabled(False)
        # button_update_accounts.clicked.connect(self.update_accounts)
        top_layout.addWidget(button_update_accounts)
        button_update_earnings = QPushButton('Update Earnings')
        button_update_earnings.setEnabled(False)
        Globals.components.append(button_update_earnings)
        # button_update_earnings.clicked.connect(self.update_earnings)
        top_layout.addWidget(button_update_earnings)
        button_reload = QPushButton('Reload')
        Globals.components.append(button_reload)
        button_reload.clicked.connect(self.reload)
        top_layout.addWidget(button_reload)
        top_layout.addStretch()
        button_save = QPushButton('Save')
        button_save.setEnabled(False)
        # button_save.clicked.connect(self.save_data_to_file)
        top_layout.addWidget(button_save)

        middle_layout = QHBoxLayout()
        layout.addLayout(middle_layout)
        self.table = QTableWidget(0, len(self.columns))
        middle_layout.addWidget(self.table)
        self.table.setHorizontalHeaderLabels(self.columns)

        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 5)

        self.table.mousePressEvent = self.table_mouse_press_event
        self.table.cellClicked.connect(self.cell_was_clicked)
        self.table.doubleClicked.connect(self.cell_was_double_clicked)

    def show_changed_menu(self, pos, data):
        menu = QMenu()
        account = data.get("account")
        action_type = data.get('action_type')
        if action_type == 'modification':
            change = data.get('datas')
            action_text = f'Confirm Change: {change}'
            action = menu.addAction(action_text)
            action.triggered.connect(lambda _, a=account, d=change: self.modify_account(a, d))
        elif action_type == 'addition':
            action_text = f'Confirm Addition: {account}'
            action = menu.addAction(action_text)
            action.triggered.connect(lambda _, a=account, d=data.get('datas'): self.add_account(a, d))
        elif action_type == 'deletion':
            action_text = f'Confirm Deletion: {account}'
            action = menu.addAction(action_text)
            action.triggered.connect(lambda _, a=account: self.delete_account(a))
        else:
            Globals._Log.error(self.user, f'')
            return

        menu.exec(self.table.viewport().mapToGlobal(pos))

    def show_context_menu(self, pos, index):
        menu = QMenu()

        row = index.row()
        account = self.get_text(row, self.columns.index('account'))

        action_show_user_details = menu.addAction('Show Details')
        action_show_user_details.triggered.connect(lambda _, r=row: self.show_user_details(r))
        
        action_show_videos = menu.addAction('Show Videos')
        action_show_videos.triggered.connect(lambda _, a=account: Globals._WS.list_videos_right_signal.emit(a))

        tk_link = f'https://www.tiktok.com/@{account}'
        action_copy_tk_link = menu.addAction('Copy TK Link')
        action_copy_tk_link.triggered.connect(lambda: QApplication.clipboard().setText(tk_link))

        rs_link = self.get_text(row, self.columns.index('referralLink'))
        if rs_link:
            if not rs_link.startswith('https://'):
                rs_link = 'https://' + rs_link
            action_copy_rs_link = menu.addAction('Copy Invitation Link')
            action_copy_rs_link.triggered.connect(lambda: QApplication.clipboard().setText(rs_link))

        menu.exec(self.table.viewport().mapToGlobal(pos))

    def show_user_details(self, row):
        referralLink = self.get_text(row, self.columns.index('referralLink'))
        region = self.get_text(row, self.columns.index('region'))
        rsAccount = self.get_text(row, self.columns.index('rsAccount'))

        is_agent = False
        if 'inviterType=1' in referralLink:
            is_agent = True

        if not rsAccount:
            Globals._Log.warning(self.user, 'No rsAccount available for the operation.')
            return
        
        try:
            if is_agent:
                res = Globals._SQL.read('agents_america' if region == '美洲' else 'agents_asia', condition=f'mobile="{rsAccount}"')
            else:
                res = Globals._SQL.read('users_america' if region == '美洲' else 'users_asia', condition=f'phone="{rsAccount}"')
            userId = res[0][0]
            Globals._Log.info(self.user, f'User ID {userId} retrieved successfully.')
            AccountDialog(self, row, region, userId, rsAccount, is_agent)
        except Exception as e:
            Globals._Log.error(self.user, f'Error retrieving user ID for rsAccount {rsAccount}, Please update users: {e}')

    def table_mouse_press_event(self, event):
        if event.button() == Qt.MouseButton.RightButton:
            index = self.table.indexAt(event.pos())
            if not index.isValid():
                return
            data = index.data(256)
            if data:
                self.show_changed_menu(event.pos(), data)
            else:
                self.show_context_menu(event.pos(), index)
        else:
            QTableWidget.mousePressEvent(self.table, event)

    @pyqtSlot(dict)
    def update_account_changed(self, change):
        Globals._Log.warning(self.user, f"Change detected: {change}")
        try:
            color = QColor(change.get('color'))
            account = change.get('account')
            data = change.get('datas')
            action_type = change.get('action_type')
            
            account_col = self.columns.index('account')
            if action_type == 'addition':
                row = self.find_row_by_account(account)
                if row == -1:
                    self.table.setSortingEnabled(False)
                    row = self.table.rowCount()
                    self.table.insertRow(row)
                    self.fill_row(row, data)
                    self.table.setSortingEnabled(True)
                self.table.item(row, account_col).setBackground(color)
                self.table.item(row, account_col).setData(256, change)
            elif action_type == 'deletion':
                row = self.find_row_by_account(account)
                self.table.item(row, account_col).setBackground(color)
                self.table.item(row, account_col).setData(256, change)
            elif action_type == 'modification':
                row = self.find_row_by_account(account)
                self.table.item(row, self.columns.index(data[0])).setBackground(color)
                self.table.item(row, self.columns.index(data[0])).setData(256, change)
        except Exception as e:
            Globals._Log.error(self.user, f'Failed to apply change to the table: {e}')

    @pyqtSlot(int, dict)
    def update_earnings(self, row, earning_dict):
        self.table.setSortingEnabled(False)
        Globals._MUTEX_ACDICT.lock()
        try:
            self.table.item(row, self.columns.index('todayEarnings')).setText(f'{earning_dict["todayEarnings"]:.2f}')
            self.table.item(row, self.columns.index('earningDays')).setText(f'{earning_dict["earningDays"]}')
            self.table.item(row, self.columns.index('totalEarnings')).setText(f'{earning_dict["totalEarnings"]:.2f}')
            self.table.item(row, self.columns.index('latestEarningDay')).setText(f'{earning_dict["latestEarningDay"]}')
            account = self.get_text(row, self.columns.index('account'))

            Globals.accounts_dict[account].update(earning_dict)
            earning_dict['id'] = Globals.accounts_dict[account]['id']

            Globals._WS.database_operation_signal.emit('upsert',{
                'table_name': 'accounts',
                'columns': list(earning_dict.keys()),
                'values': list(earning_dict.values()),
                'unique_columns': ['id']
            }, None)

        except Exception as e:
            Globals._Log.error(self.user, f'{e}')

        finally:
            Globals._MUTEX_ACDICT.unlock()
            self.table.setSortingEnabled(True)

    @pyqtSlot(dict)
    def update_account_table(self, datas):
        self.table.setSortingEnabled(False)
        Globals._MUTEX_ACDICT.lock()
        try:
            for account, data in datas.items():
                row = self.find_row_by_account(account)
                for field, value in data.items():
                    if field not in self.columns:
                        continue
                    col = self.columns.index(field)
                    self.table.item(row, col).setText(str(value))
                if 'updateTime' in data:
                    self.table.item(row, self.columns.index('update')).setText(self.format_update_intervals(0))
                Globals.accounts_dict[account].update(data)

        except Exception as e:
            Globals._Log.error(self.user, f'update_account_table: {e}')

        finally:
            Globals._MUTEX_ACDICT.unlock()
            self.table.setSortingEnabled(True)