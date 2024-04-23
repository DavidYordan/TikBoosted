import time
from PyQt6.QtCore import (
    QRunnable
)
from queue import (
    Empty,
    PriorityQueue
)

from globals import Globals

class GoogleSync(QRunnable):
    def __init__(self):
        super().__init__()

        self.is_running = False
        self.accounts_mapping = {
            '帳號驗證': 'account',
            '職位': 'position',
            '狀態': 'status',
            '客戶端備註': 'clientRemark',
            '服務端備註': 'serverRemark',
            'RS帳號': 'rsAccount',
            '最新收益日期': 'latestEarningDay',
            '今日收益': 'todayEarnings',
            '收益天数': 'earningDays',
            '总收益': 'totalEarnings',
            '區域': 'region',
            '小组': 'team',
            '昵稱': 'customer',
            '推薦碼': 'referralCode',
            'TK帳號': 'tkAccount',
            '推薦連結': 'referralLink'
        }
        self.queue = PriorityQueue()
        self.setAutoDelete(False)
        self.user = 'GoogleSync'

        self.queue.put((int(time.time())+5, ('sync_google_accounts', {})))

        Globals._Log.info(self.user, 'Successfully initialized.')

    def sync_google_accounts(self, params=None):
        Globals._Log.info(self.user, "Attempting to fetch account data from Google Sheets.")
        try:
            sheet_name = '匯總'
            range_name = self.get_sheet_data_range(Globals._SERVER_SHEET, Globals._SPREADSHEET_ID, sheet_name, 'A1:L')

            result = Globals._SERVER_SHEET.values().get(
                spreadsheetId=Globals._SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            google_datas = result.get('values', [])

            if not google_datas:
                return
            
            headers = google_datas[0]
            keys = [self.accounts_mapping[header] for header in headers]
            values = [row + [''] * (len(headers) - len(row)) for row in google_datas[1:]]

            google_accounts_dict = {}
            for row in values:
                account_data = {field: value for field, value in zip(keys, row)}
                account = account_data['account']
                if account in google_accounts_dict:
                    Globals._Log.error(self.user, f'Duplicate account detected: {account_data}')
                else:
                    google_accounts_dict[account] = account_data
            google_accounts_set = set(google_accounts_dict.keys())
        
            current_accounts_set = set(Globals.accounts_dict)
            self.addition_set = google_accounts_set - current_accounts_set
            for account in self.addition_set:
                Globals._WS.update_account_changed_signal.emit({
                    'account': account,
                    'color': 'yellow',
                    'action_type': 'addition',
                    'datas': google_accounts_dict[account]
                })

            deletion_set = current_accounts_set - google_accounts_set
            for account in deletion_set:
                Globals._WS.update_account_changed_signal.emit({
                    'account': account,
                    'color': 'red',
                    'action_type': 'deletion'
                })

            for account in google_accounts_set & current_accounts_set:
                for key, value in google_accounts_dict[account].items():
                    if str(Globals.accounts_dict[account][key]) == value:
                        continue
                    Globals._WS.update_account_changed_signal.emit({
                        'account': account,
                        'color': 'yellow',
                        'action_type': 'modification',
                        'datas': [key, value]
                    })
            Globals._Log.info(self.user, 'Account data fetched and processed successfully.')
        except Exception as e:
            Globals._Log.error(self.user, f'Failed to fetch or process account data: {e}')
        finally:
            self.queue.put((int(time.time()+300), ('sync_google_accounts', {})))

    def get_sheet_data_range(self, service, spreadsheet_id, sheet_name, range):
        sheet_metadata = service.get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        for sheet in sheets:
            if sheet['properties']['title'] != sheet_name:
                continue
            grid_properties = sheet['properties']['gridProperties']
            return f"{sheet_name}!{range}{grid_properties['rowCount']}"

    def insert_task(self, t):
        weight, (func, params) = t
        if func == 'sync_google_accounts':
            self.jump_task(func, params)
        else:
            self.queue.put(t)

    def jump_task(self, func, params):
        temp = []
        for _ in range(self.queue.qsize()):
            weight, (f, p) = self.queue.get()
            if f == func:
                self.queue.put((5, (f, params)))
                break
            else:
                temp.append((weight, (f, p)))
        [self.queue.put(item) for item in temp]

    def run(self):
        self.is_running = True
        while self.is_running and Globals.is_app_running:
            try:
                weight, (func, params) = self.queue.get(timeout=3)
                if weight > int(time.time()):
                    self.queue.put((weight, (func, params)))
                    time.sleep(3)
                    continue
                getattr(self, func)(params)
            except Empty:
                # self.get_orders()
                time.sleep(3)
                continue
        self.is_running = False

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool.start(self)

    def stop_task(self):
        self.is_running = False