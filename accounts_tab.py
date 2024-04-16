from bidict import bidict
from datetime import date
from PyQt6.QtCore import (
    QMutex,
    pyqtSignal,
    pyqtSlot,
    Qt,
    QTimer
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
    update_table_signal = pyqtSignal(dict)

    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        self.field_mapping = bidict({
            "team": "小组",
            "customer": "昵稱",
            "account": "帳號驗證",
            "position": "職位",
            "status": "狀態",
            "region": "區域",
            "rsAccount": "RS帳號",
            "todayEarnings": "今日收益",
            "earningDays": "收益天数",
            "totalEarnings": "总收益",
            "clientRemark": "客戶端備註",
            "referralCode": "推薦碼",
            "tkAccount": "TK帳號",
            "referralLink": "推薦連結",
            "latestEarningDay": "最新收益日期"
        })
        self.addition_set = set()
        self.field_columns = list(self.field_mapping.keys())
        self.google_accounts = []
        self.mutex_sorted = QMutex()
        self.pending_changes = {}
        Globals._WS.update_account_earnings_signal.connect(self.update_earnings)
        self.update_table_signal.connect(self.update_table)
        self.user = 'AccountsTab'
        self.setup_ui()
        self.reload()

        self.timer = QTimer()
        self.timer.timeout.connect(lambda: Globals.run_async_task(self.fetch_accounts))
        self.timer.start(60000)

        Globals._Log.info(self.user, 'Successfully initialized.')

    async def fetch_accounts(self):

        def _get_sheet_data_range(service, spreadsheet_id, sheet_name):
            sheet_metadata = service.get(spreadsheetId=spreadsheet_id).execute()
            sheets = sheet_metadata.get('sheets', '')
            for sheet in sheets:
                if sheet['properties']['title'] != sheet_name:
                    continue
                grid_properties = sheet['properties']['gridProperties']
                return f"{sheet_name}!A1:K{grid_properties['rowCount']}"

        Globals._Log.info(self.user, "Attempting to fetch account data from Google Sheets.")
        try:
            sheet_name = '匯總'
            range_name = _get_sheet_data_range(Globals._SERVER_SHEET, Globals._SPREADSHEET_ID, sheet_name)

            result = Globals._SERVER_SHEET.values().get(
                spreadsheetId=Globals._SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            google_datas = result.get('values', [])

            if not google_datas:
                return
            
            headers = google_datas[0]
            values = [row + [''] * (len(headers) - len(row)) for row in google_datas[1:]]

            field_en = [self.field_mapping.inverse[header] for header in headers]
            google_accounts_dict = {}
            for row in values:
                account_data = {field: value for field, value in zip(field_en, row)}
                account = account_data['account']
                if account in google_accounts_dict:
                    Globals._Log.error(self.user, f'Duplicate account detected: {account_data}')
                else:
                    google_accounts_dict[account] = account_data
            google_accounts_set = set(google_accounts_dict.keys())
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')
            return
        
        Globals._SQL_MUTEX.lock()
        try:
            current_accounts_dict = Globals.accounts_dict
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')
            return
        finally:
            Globals._SQL_MUTEX.unlock()

        try:
            current_accounts_set = set(current_accounts_dict)
            self.addition_set = google_accounts_set - current_accounts_set
            for account in self.addition_set:
                self.update_table_signal.emit({
                    'account': account,
                    'color': 'yellow',
                    'action_type': 'addition',
                    'datas': google_accounts_dict[account]
                })

            deletion_set = current_accounts_set - google_accounts_set
            for account in deletion_set:
                self.update_table_signal.emit({
                    'account': account,
                    'color': 'red',
                    'action_type': 'deletion'
                })

            for account in google_accounts_set & current_accounts_set:
                for key, value in google_accounts_dict[account].items():
                    if str(current_accounts_dict[account][key]) == value:
                        continue
                    self.update_table_signal.emit({
                        'account': account,
                        'color': 'yellow',
                        'action_type': 'modification',
                        'datas': [key, value]
                    })
            Globals._Log.info(self.user, 'Account data fetched and processed successfully.')
        except Exception as e:
            Globals._Log.error(self.user, f'Failed to fetch or process account data: {e}')

    async def fetch_earnings_details(self, userId, region):
        url_region = 'america' if region == '美洲' else 'asia'
        earnings_data = await Globals.request_with_admin(
            url_region,
            'GET',
            f'/sqx_fast/moneyDetails/queryUserMoneyDetails?page=1&limit=1000&userId={userId}'
        )
        if earnings_data:
            return earnings_data.get('data', {}).get('records', [])
        return []
    
    def add_account(self, account, data):
        Globals._SQL_MUTEX.lock()
        self.mutex_sorted.lock()
        try:
            row = self.find_row_by_account(account)
            columns = list(data.keys())
            Globals._WS.database_operation_signal.emit('upsert',{
                'table_name': 'accounts',
                'columns': columns,
                'values': [data[key] for key in columns],
                'unique_columns': ['account']
            }, None)
            Globals.accounts_dict[account] = data
            col = self.field_columns.index('account')
            self.table.item(row, col).setData(256, None)
            self.table.item(row, col).setBackground(QColor('white'))
            self.addition_set.discard(account)
            Globals._Log.info(self.user, f'Successfully deleted account {account}.')
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')
        finally:
            Globals._SQL_MUTEX.unlock()
            self.mutex_sorted.unlock()
    
    def cell_was_double_clicked(self):
        current_index = self.table.currentIndex()
        self.show_user_details(current_index.row())

    def delete_account(self, account):
        Globals._SQL_MUTEX.lock()
        self.mutex_sorted.lock()
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
            Globals._SQL_MUTEX.unlock()
            self.mutex_sorted.unlock()

    def fill_row(self, row, account_data):
        for field, value in account_data.items():
            column = self.field_columns.index(field)
            item = QTableWidgetItem(str(value))
            self.table.setItem(row, column, item)

    def find_row_by_account(self, account):
        account_col = list(self.field_mapping.keys()).index('account')
        for row in range(self.table.rowCount()):
            if self.safe_get_text(row, account_col) == account:
                return row
        return -1
    
    def modify_account(self, account, data):
        Globals._SQL_MUTEX.lock()
        self.mutex_sorted.lock()
        try:
            Globals._WS.database_operation_signal.emit('update',{
                'table_name': 'accounts',
                'updates': {data[0]: data[1]},
                'condition': f'account="{account}"'
            }, None)
            Globals.accounts_dict[account][data[0]] = data[1]
            row = self.find_row_by_account(account)
            col = self.field_columns.index(data[0])
            self.table.item(row, col).setText(data[1])
            self.table.item(row, col).setData(256, None)
            self.table.item(row, col).setBackground(QColor('white'))

            Globals._Log.info(self.user, f'')
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')
        finally:
            Globals._SQL_MUTEX.unlock()
            self.mutex_sorted.unlock()

    def reload(self):
        fields = Globals._SQL.get_table_fields('accounts')
        datas = Globals._SQL.read('accounts')
        today_date = date.today().isoformat()

        if not datas:
            self.table.setRowCount(0)
            return
        
        Globals._SQL_MUTEX.lock()
        self.mutex_sorted.lock()
        try:
            Globals.accounts_dict.clear()
            self.table.setSortingEnabled(False)
            self.table.clearContents()
            self.table.setRowCount(len(datas))
            for row, data in enumerate(datas):
                account = data[0]
                Globals.accounts_dict[account] = {}
                for idx, field in enumerate(fields):
                    Globals.accounts_dict[account][field] = data[idx]
                if Globals.accounts_dict[account]['latestEarningDay'] != today_date:
                    Globals.accounts_dict[account]['todayEarnings'] = 0
                self.fill_row(row, Globals.accounts_dict[account])
            self.table.setSortingEnabled(True)
        except Exception as e:
            print(str(e))
            Globals._Log.error(self.user, f'{e}')
        finally:
            Globals._SQL_MUTEX.unlock()
            self.mutex_sorted.unlock()

        Globals.run_async_task(self.fetch_accounts)

    def safe_get_text(self, row, column):
        try:
            return self.table.item(row, column).text()
        except:
            return ""

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
        self.table = QTableWidget(0, len(self.field_mapping))
        middle_layout.addWidget(self.table)
        header_labels = list(self.field_mapping.keys())
        self.table.setHorizontalHeaderLabels(header_labels)

        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 10)

        self.table.mousePressEvent = self.table_mouse_press_event
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
        account = self.table.item(row, self.field_columns.index('account')).text()

        action_show_user_details = menu.addAction('Show Details')
        action_show_user_details.triggered.connect(lambda _, r=row: self.show_user_details(r))
        
        action_show_videos = menu.addAction('Show Videos')
        action_show_videos.triggered.connect(lambda _, a=account: Globals._WS.list_videos_right_signal.emit(a))

        tk_link = f'https://www.tiktok.com/@{account}'
        action_copy_tk_link = menu.addAction('Copy TK Link')
        action_copy_tk_link.triggered.connect(lambda: QApplication.clipboard().setText(tk_link))

        rs_link = self.table.item(row, self.field_columns.index('referralLink')).text()
        if rs_link:
            if not rs_link.startswith('https://'):
                rs_link = 'https://' + rs_link
            action_copy_rs_link = menu.addAction('Copy Invitation Link')
            action_copy_rs_link.triggered.connect(lambda: QApplication.clipboard().setText(rs_link))

        menu.exec(self.table.viewport().mapToGlobal(pos))

    def show_user_details(self, row):
        referralLink = self.safe_get_text(row, self.field_columns.index('referralLink'))
        region = self.safe_get_text(row, self.field_columns.index('region'))
        rsAccount = self.safe_get_text(row, self.field_columns.index('rsAccount'))

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
            Globals._Log.error(self.user, f'Error retrieving user ID for rsAccount {rsAccount}: {e}')

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
    def update_table(self, change):
        Globals._Log.warning(self.user, f"Change detected: {change}")
        self.mutex_sorted.lock()
        try:
            color = QColor(change.get('color'))
            account = change.get('account')
            data = change.get('datas')
            action_type = change.get('action_type')

            sorting_enabled = self.table.isSortingEnabled()
            self.table.setSortingEnabled(False)
            account_col = self.field_columns.index('account')

            if action_type == 'addition':
                row = self.find_row_by_account(account)
                if row == -1:
                    row = self.table.rowCount()
                    self.table.insertRow(row)
                    self.fill_row(row, data)
                self.table.item(row, account_col).setBackground(color)
                self.table.item(row, account_col).setData(256, change)
            elif action_type == 'deletion':
                row = self.find_row_by_account(account)
                self.table.item(row, account_col).setBackground(color)
                self.table.item(row, account_col).setData(256, change)
            elif action_type == 'modification':
                row = self.find_row_by_account(account)
                self.table.item(row, self.field_columns.index(data[0])).setBackground(color)
                self.table.item(row, self.field_columns.index(data[0])).setData(256, change)

            self.table.setSortingEnabled(sorting_enabled)
        except Exception as e:
            Globals._Log.error(self.user, f'Failed to apply change to the table: {e}')
        finally:
            self.mutex_sorted.unlock()

    @pyqtSlot(int, list)
    def update_earnings(self, row, earnings):
        Globals._SQL_MUTEX.lock()
        self.mutex_sorted.lock()
        try:
            sortEnabled = self.table.isSortingEnabled()
            self.table.setSortingEnabled(False)
            self.table.item(row, self.field_columns.index('todayEarnings')).setText(f'{earnings[0]:.2f}')
            self.table.item(row, self.field_columns.index('earningDays')).setText(f'{earnings[1]}')
            self.table.item(row, self.field_columns.index('totalEarnings')).setText(f'{earnings[2]:.2f}')
            self.table.item(row, self.field_columns.index('latestEarningDay')).setText(f'{earnings[3]}')
            account = self.table.item(row, self.field_columns.index('account')).text()
            Globals._WS.database_operation_signal.emit('update',{
                'table_name': 'accounts',
                'updates': {
                    'todayEarnings': earnings[0],
                    'earningDays': earnings[1],
                    'totalEarnings': earnings[2],
                },
                'condition': f'account="{account}"'
            }, None)
            self.table.setSortingEnabled(sortEnabled)
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')
        finally:
            Globals._SQL_MUTEX.unlock()
            self.mutex_sorted.unlock()