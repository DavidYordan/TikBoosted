import asyncio
import base64
import datetime
import re
import requests
import time
from collections import defaultdict
from playwright.async_api import async_playwright
from PyQt6.QtCore import (
    pyqtSignal,
    pyqtSlot,
    QRunnable,
    Qt
)
from PyQt6.QtGui import (
    QCursor
)
from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QMenu,
    QScrollArea,
    QWidget,
    QPushButton,
    QTextEdit,
    QVBoxLayout
)
from twocaptcha import TwoCaptcha

from globals import Globals

class Worker(QRunnable):
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.func(*self.args, **self.kwargs))
        loop.close()

class AccountDialog(QDialog):
    update_income_signal = pyqtSignal(list)
    update_team_signal = pyqtSignal(list, list)
    update_user_details_signal = pyqtSignal(dict)

    def __init__(
            self,
            parent,
            row,
            region,
            userId,
            rsAccount,
            is_agent
        ):
        super().__init__(parent)

        if region == '美洲':
            self.url_base = Globals._BASE_URL_AMERICA
            self.token_name = 'america_token'
            self.products_table = 'products_america'
            self.users_table = 'agents_america' if is_agent else 'users_america'
        else:
            self.url_base = Globals._BASE_URL_ASIA
            self.token_name = 'asia_token'
            self.products_table = 'products_asia'
            self.users_table = 'agents_asia' if is_agent else 'users_asia'
        self.headers = {'Token': ''}
        self.is_agent = is_agent
        self.row = row
        self.rsAccount = rsAccount
        self.session_admin = requests.Session()
        self.update_income_signal.connect(self.update_income)
        self.update_team_signal.connect(self.update_team)
        self.update_user_details_signal.connect(self.update_user_details)
        self.userId = userId
        self.user_details = ''
        self.user = 'AccountDialog'

        self.setWindowTitle(self.user)
        self.setModal(True)
        self.resize(960, 320)
        self.setup_ui()
        self.load_datas()

        self.exec()

    async def get_token_with_playwright(self):
        Globals._WS.toggle_components_signal.emit(False)
        Globals._Log.info(self.user, 'Starting token acquisition process.')
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                page = await browser.new_page()
                await page.goto(self.url_base + '/login')

                captcha_selector = '.login-captcha img'
                await page.wait_for_selector(captcha_selector)

                captcha_element = await page.query_selector(captcha_selector)
                captcha_src = await captcha_element.get_attribute('src')
                captcha_src = self.url_base + captcha_src

                uuid_match = re.search(r'uuid=([\w-]+)', captcha_src)
                if uuid_match:
                    uuid = uuid_match.group(1)

                captcha_element = await page.query_selector(captcha_selector)
                captcha_image_data = await captcha_element.screenshot()
                captcha_image_base64 = base64.b64encode(captcha_image_data).decode('utf-8')

                solver = TwoCaptcha(Globals._TO_CAPTCHA_KEY)
                captcha_result = solver.normal(captcha_image_base64, numeric=3, minLength=1, maxLength=1)

                captcha_code = captcha_result.get('code')

                login_data = {
                    "username": Globals._ADMIN_USER,
                    "password": Globals._ADMIN_PASSWORD,
                    "uuid": uuid,
                    "captcha": captcha_code
                }

                token_response = await page.evaluate("""loginData => {
                    return fetch('/sqx_fast/sys/login', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(loginData)
                    }).then(response => response.json());
                }""", login_data)

                token = token_response.get('token', '')

                if token:
                    Globals._Log.info(self.user, f'Token acquired successfully.')
                    self.session_admin.headers.update({'Token': token})
                    Globals._WS.database_operation_signal.emit('upsert',{
                        'table_name': 'tokens',
                        'columns': ['name', 'token', 'expire'],
                        'values': [self.token_name, token, int(time.time()+token_response.get('expire', 0))],
                        'unique_columns': ['name']
                    }, None)
                else:
                    Globals._Log.error(self.user, 'Failed to acquire token.')

                await browser.close()

        except Exception as e:
            Globals._Log.error(self.user, f'Error in token acquisition: {e}')
        finally:
            Globals._WS.toggle_components_signal.emit(True)

    async def request_with_admin(self, method, url, **kwargs):
        retry_count = 3
        while retry_count:
            try:
                response = self.session_admin.request(method, url, **kwargs)
                if response.status_code != 200:
                    retry_count -= 1
                    Globals._Log.error(self.user, f'Failed to fetch data, status code: {response.status_code}')
                if response.json().get('code', 401) == 401:
                    Globals._Log.error(self.user, 'Authentication failed, invalid token.')
                    await self.get_token_with_playwright()
                    continue
                return response.json()
            except Exception as e:
                Globals._Log.error(self.user, f'{e}')
                retry_count -= 1
            time.sleep(3)
        return None

    async def update_money_worker(self):
        Globals._Log.info(self.user, 'Starting products update process.')
        Globals._WS.toggle_components_signal.emit(False)
        page = 1
        totalPage = 0
        totalCount = 0
        currentCount = 0
        money_records = []

        try:
            if self.is_agent:
                response = await self.request_with_admin('get', f'{self.url_base}/sqx_fast/agent/agent/list?page=1&limit=10&mobile={self.rsAccount}')
                if response and 'page' in response:
                    self.update_user_details_signal.emit(response['page']['list'][0])
            else:
                response = await self.request_with_admin('get', f'{self.url_base}/sqx_fast/user/{self.userId}')
                if response and 'data' in response:
                    self.update_user_details_signal.emit(response['data']['userEntity'])
        except Exception as e:
            Globals._Log.error(self.user, f'Failed to fetch user details: {e}')

        while True:
            Globals._Log.info(self.user, f'Fetching data for page {page}/{totalPage}')
            if self.is_agent:
                res = await self.request_with_admin(
                    'get',
                    f'{self.url_base}/sqx_fast//agent/money/page?page={page}&limit=1000&ausername={self.rsAccount}'
                )
                datas = res.get('page', {})
                money_records += datas.get('list', [])
            else:
                res = await self.request_with_admin(
                    'get',
                    f'{self.url_base}/sqx_fast/moneyDetails/queryUserMoneyDetails?page={page}&limit=1000&userId={self.userId}'
                )
                datas = res.get('data', {})
                money_records += datas.get('records', [])
            totalPage = datas.get('pages', 0)
            totalCount = datas.get('total', 0)
            Globals._Log.info(self.user, f'Page {page} data processed successfully.')
            page += 1
            if page > totalPage:
                break
        self.update_income_signal.emit(money_records)

        page = 1
        totalPage = 0
        totalCount = 0
        currentCount = 0
        invite_records = []
        agent_invite_records = []

        while True:
            Globals._Log.info(self.user, f'Fetching data for page {page}/{totalPage}')
            if self.is_agent:
                res = await self.request_with_admin(
                    'get',
                    f'{self.url_base}/sqx_fast/user/selectUserList?page={page}&limit=1000&member=-1&inviterCode={self.userId}'
                )
                datas = res.get('data', {})
                agent_invite_records += datas.get('list', [])
            else:
                res = await self.request_with_admin(
                    'get',
                    f'{self.url_base}/sqx_fast/invite/selectInviteByUserIdLists?page={page}&limit=1000&userId={self.userId}'
                )
                datas = res.get('data', {}).get('pageUtils', {})
                invite_records += datas.get('list', [])
            totalPage = datas.get('totalPage', 0)
            totalCount = datas.get('totalCount', 0)
            Globals._Log.info(self.user, f'Page {page} data processed successfully.')
            page += 1
            if page > totalPage:
                break
        
        if self.is_agent:
            money_dict = defaultdict(float)
            for record in money_records:
                money_dict[record['umobile']] += record['money']
            for record in agent_invite_records:
                record['money'] = money_dict.get(record['phone'], 0)
                invite_records.append(record)

        res = await self.request_with_admin(
            'get',
            f'{self.url_base}/sqx_fast/vipDetails/selectVipDetailsList?page=1&limit=100'
        )
        vips = res.get('data', {}).get('list', '')

        self.update_team_signal.emit(invite_records, vips)
        
        Globals._WS.toggle_components_signal.emit(True)

    def get_token_from_database(self):
        res = Globals._SQL.read('tokens', condition=f'name="{self.token_name}"')
        if not res:
            Globals._Log.error(self.user, 'Token not found in the database.')
        if res[0][2] < time.time():
            Globals._Log.error(self.user, 'Token has expired.')
        self.session_admin.headers.update({'Token': res[0][1]})

    def load_datas(self):
        if not self.session_admin.headers.get('Token'):
            Globals._Log.info(self.user, 'Get token from database.')
            self.get_token_from_database()
        Globals.thread_pool.start(Worker(self.update_money_worker))

    async def on_product_selected(self, product, userId, phone):
        productId, productName, productPrice = product.split('|')

        await self.request_with_admin(
            'get',
            f'{self.url_base}/sqx_fast/user/{userId}'
        )
        res = await self.request_with_admin(
            'get',
            f'{self.url_base}/sqx_fast/moneyDetails/selectUserMoney?userId={userId}'
        )
        money_diff = float(productPrice) - res.get('data', {}).get('money', 0)
        if money_diff > 0:
            res = await self.request_with_admin(
                'post',
                f'{self.url_base}/sqx_fast/user/addCannotMoney/{userId}/{productPrice}'
            )
            res = await self.request_with_admin(
                'get',
                f'{self.url_base}/sqx_fast/user/{userId}'
            )
            phone = res['data']['userEntity']['phone']
            res = await self.request_with_admin(
                'get',
                f'{self.url_base}/sqx_fast/moneyDetails/selectUserMoney?userId={userId}'
            )
        
        if not phone:
            res = await self.request_with_admin(
                'get',
                f'{self.url_base}/sqx_fast/user/{userId}'
            )
            phone = res['data']['userEntity']['phone']

        session = requests.Session()
        res = session.post(f'{self.url_base}/sqx_fast/app/Login/registerCode?password=d135246&phone={phone}')
        try:
            token = res.json()['token']
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')
            return
        session.headers.update({'Token': token})
        if productName == 'vip':
            res = session.get(f'{self.url_base}/sqx_fast/app/order/insertVipOrders?vipDetailsId={productId}&time={int(time.time()*1000)}')
            try:
                orderId = res.json()['data']['ordersId']
            except Exception as e:
                Globals._Log.error(self.user, f'{e}')
                return
        else:
            res = session.get(f'{self.url_base}/sqx_fast/app/order/insertCourseOrders?courseId={productId}&time={int(time.time()*1000)}')
            try:
                orderId = res.json()['data']['orders']['ordersId']
            except Exception as e:
                Globals._Log.error(self.user, f'{e}')
                return
        time.sleep(1)
        res = session.post(
            f'{self.url_base}/sqx_fast/app/order/payOrders',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=f'orderId={orderId}'
        )
        await self.update_money_worker()

    def setup_ui(self):
        main_layout = QHBoxLayout(self)

        user_details_layout = QVBoxLayout()
        user_details_label = QLabel('User Details')
        user_details_layout.addWidget(user_details_label)
        self.user_details_text_edit = QTextEdit()
        self.user_details_text_edit.setReadOnly(True)
        user_details_scroll = QScrollArea()
        user_details_scroll.setWidget(self.user_details_text_edit)
        user_details_scroll.setWidgetResizable(True)
        user_details_layout.addWidget(user_details_scroll)
        main_layout.addLayout(user_details_layout)

        income_details_layout = QVBoxLayout()
        self.income_details_label = QLabel('Earning Details')
        income_details_layout.addWidget(self.income_details_label)
        self.income_details_scroll_widget = QWidget()
        self.income_details_layout = QVBoxLayout(self.income_details_scroll_widget)
        income_details_scroll = QScrollArea()
        income_details_scroll.setWidget(self.income_details_scroll_widget)
        income_details_scroll.setWidgetResizable(True)
        income_details_layout.addWidget(income_details_scroll)
        main_layout.addLayout(income_details_layout)

        team_members_layout = QVBoxLayout()
        self.team_members_label = QLabel('Team')
        team_members_layout.addWidget(self.team_members_label)
        self.team_members_scroll_widget = QWidget()
        self.team_members_layout = QVBoxLayout(self.team_members_scroll_widget)
        team_members_scroll = QScrollArea()
        team_members_scroll.setWidget(self.team_members_scroll_widget)
        team_members_scroll.setWidgetResizable(True)
        team_members_layout.addWidget(team_members_scroll)
        main_layout.addLayout(team_members_layout)

        self.setLayout(main_layout)

    def show_menu(self, pos, button, vips):
        products = Globals._SQL.read(self.products_table)
        sorted_products = sorted(products, key=lambda x: x[12])
        menu = QMenu()
        for vip in vips:
            text = f'{vip["id"]}|vip|{vip["money"]}'
            action = menu.addAction(text)
            action.triggered.connect(lambda _, p=text, b=button: self.trigger_product_selected(p, b.text()))
        for product in sorted_products:
            text = f'{product[0]}|{product[9]}|{product[12]}'
            action = menu.addAction(text)
            action.triggered.connect(lambda _, p=text, b=button: self.trigger_product_selected(p, b.text()))
        menu.exec(QCursor.pos())

    def trigger_product_selected(self, product, button_text):
        userId, _, _ = button_text.split('|')
        res = Globals._SQL.read(self.users_table, ['phone'], f'userId="{userId}"')
        try:
            phone = res[0][0]
        except:
            phone = None
        Globals.thread_pool.start(Worker(self.on_product_selected, product, userId, phone))

    @pyqtSlot(list)
    def update_income(self, datas):
        money_total = 0
        money_sum_by_date = defaultdict(float)
        details_by_date = defaultdict(list)
        today_date = datetime.date.today().isoformat()
        latest_create_time = ''

        for item in datas:
            date = item['createTime'].split(' ')[0]
            if date > latest_create_time:
                latest_create_time = date
            details_by_date[date].append(f"{item['createTime']}: {item['money']:.2f} - {item['title']}")
            if self.is_agent:
                money_sum_by_date[date] += item['money']
                money_total += item['money']
                continue
            if 'commission' in item['title']:
                money_sum_by_date[date] += item['money']
                money_total += item['money']

        Globals._WS.update_account_earnings_signal.emit(int(self.row), {
            'todayEarnings': money_sum_by_date.get(today_date, 0),
            'earningDays': len(money_sum_by_date),
            'totalEarnings': money_total,
            'latestEarningDay': latest_create_time
        })
        self.income_details_label.setText(f'Earning Details: {money_total}/{len(money_sum_by_date)}')

        sorted_money_sum = sorted(money_sum_by_date.items(), key=lambda x: x[0], reverse=True)
        while self.income_details_layout.count():
            child = self.income_details_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()

        for date, total_sum in sorted_money_sum[:50]:
            button_text = f"{date}: {total_sum:.2f}"
            button = QPushButton(button_text)
            tooltip_text = '\n'.join(details_by_date[date])
            button.setToolTip(tooltip_text)
            self.income_details_layout.addWidget(button)

        self.income_details_layout.addStretch()

    @pyqtSlot(list, list)
    def update_team(self, datas, vips):
        sorted_datas = sorted(datas, key=lambda x: x['createTime'], reverse=True)
        self.team_members_label.setText(f'Team ({len(sorted_datas)})')

        while self.team_members_layout.count():
            child = self.team_members_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()

        for item in sorted_datas[:50]:
            button_text = f"{item['userId']}|{item['money']}|{item['createTime']}"
            button = QPushButton(button_text)
            button.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            button.customContextMenuRequested.connect(lambda pos, b=button, v=vips: self.show_menu(pos, b, v))
            self.team_members_layout.addWidget(button)
        
        self.team_members_layout.addStretch()

    @pyqtSlot(dict)
    def update_user_details(self, user_data):
        details = ''
        title = ''

        for key in ['userId', 'phone']:
            value = user_data.get(key, 'N/A')
            details += f"{key}: {value}\n"
            title += f' {value} -'
        self.setWindowTitle(f"{title} {'agent' if self.is_agent else 'user'}")

        for key, value in user_data.items():
            if key not in ['userId', 'phone']:
                details += f"{key}: {value}\n"

        self.user_details_text_edit.setText(details)