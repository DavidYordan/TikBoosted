import asyncio
import base64
import re
import requests
import time
from playwright.async_api import async_playwright
from PyQt6.QtCore import (
    pyqtSignal,    
    QMutex,
    QObject,
    QRunnable,
    QThreadPool
)
from PyQt6.QtWidgets import (
    QLabel,
    QTextEdit
)
from twocaptcha import TwoCaptcha

from dylogging import Logging

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

class WorkerSignals(QObject):
    database_operation_signal = pyqtSignal(str, dict, object)
    googlesync_insert_task_signal = pyqtSignal(tuple)
    list_videos_right_signal = pyqtSignal(str)
    insert_account_to_tiktok_spider = pyqtSignal(str)
    insert_orderIssuer_task_signal = pyqtSignal(object)
    set_progress_bar_title_signal = pyqtSignal(str)
    show_user_details_signal = pyqtSignal(str)
    orderIssuer_binding_check_signal = pyqtSignal(list)
    telegram_bot_signal = pyqtSignal(str, dict)
    toggle_components_signal = pyqtSignal(bool)
    toggle_progress_bar_signal = pyqtSignal(bool)
    update_account_changed_signal = pyqtSignal(dict)
    update_account_earnings_signal = pyqtSignal(int, dict)
    update_account_table_signal = pyqtSignal(dict)
    update_progress_signal = pyqtSignal(str, int)
    update_orderIssuer_order_signal = pyqtSignal(list)
    update_video_color_signal = pyqtSignal(dict)
    update_right_video_color_signal = pyqtSignal(dict)

class Globals(QObject):
    _ADMIN_USER = ''
    _ADMIN_PASSWORD = ''
    _BASE_URL_ASIA = ''
    _BASE_URL_AMERICA = ''
    _Bot = None
    _CLIENT_ID = ''
    _CLIENT_UUID = ''
    _log_textedit = QTextEdit()
    _log_label = QLabel()
    _Log = Logging(_log_textedit, _log_label)
    _MUTEX_ACDICT = QMutex()
    _MUTEX_BINDING = QMutex()
    _ORDERISSUER_PARAMS = {}
    _PROXY_TW = ''
    _PROXY_US = ''
    _SERVER_SHEET = None
    _SPREADSHEET_ID = ''
    _SQL = None
    _SYNC_ORDERS_SERVER = ''
    _SYNC_ORDERS_UUID = ''
    _TELEGRAM_BOT_TOKEN = ''
    _TELEGRAM_CHATID = ''
    _TO_CAPTCHA_KEY = ''
    _WS = WorkerSignals()
    accounts_dict = {}
    binging_dict = {}
    components = []
    is_app_running = True
    orders_dict = {}
    session_admin_america = requests.Session()
    session_admin_aisa = requests.Session()
    thread_pool = QThreadPool.globalInstance()

    @classmethod
    async def get_token_with_playwright(cls, region):
        url_base = cls._BASE_URL_AMERICA if region == 'america' else cls._BASE_URL_ASIA
        session_admin = cls.session_admin_america if region == 'america' else cls.session_admin_aisa
        cls._WS.toggle_components_signal.emit(False)
        cls._Log.info('Globals', f'Starting token acquisition process for {region}')
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                page = await browser.new_page()
                await page.goto(f'{url_base}/login')

                captcha_selector = '.login-captcha img'
                await page.wait_for_selector(captcha_selector)

                captcha_element = await page.query_selector(captcha_selector)
                captcha_src = await captcha_element.get_attribute('src')
                captcha_src = f'{url_base}{captcha_src}'

                uuid_match = re.search(r'uuid=([\w-]+)', captcha_src)
                uuid = uuid_match.group(1) if uuid_match else None

                captcha_element = await page.query_selector(captcha_selector)
                captcha_image_data = await captcha_element.screenshot()
                captcha_image_base64 = base64.b64encode(captcha_image_data).decode('utf-8')

                solver = TwoCaptcha(cls._To_CAPTCHA_KEY)
                captcha_result = solver.normal(captcha_image_base64, numeric=3, minLength=1, maxLength=1)

                captcha_code = captcha_result.get('code')

                login_data = {
                    "username": cls._ADMIN_USER,
                    "password": cls._ADMIN_PASSWORD,
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
                    cls._Log.info('Globals', f'Token acquired successfully for {region}')
                    session_admin.headers.update({'Token': token})
                    token_name = 'america_token' if region == 'america' else 'asia_token'
                    cls._WS.database_operation_signal.emit('upsert',{
                        'table_name': 'tokens',
                        'columns': ['name', 'token', 'expire'],
                        'values': [token_name, token, int(time.time()+token_response.get('expire', 0))],
                        'unique_columns': ['name']
                    })
                    cls._Log.info('Globals', f'successed to acquire token for {region}')
                else:
                    cls._Log.error('Globals', f'Failed to acquire token for {region}')

                await browser.close()

        except Exception as e:
            cls._Log.error('Globals', f'Error in token acquisition for {region}: {e}')
        finally:
            cls._WS.toggle_components_signal.emit(True)
    
    @classmethod
    def get_token_from_database(cls, region):
        token_name = 'america_token' if region == 'america' else 'asia_token'
        res = cls._SQL.read('tokens', condition=f'name="{token_name}"')
        if not res:
            cls._Log.error('Globals', f'Token not found in the database for {region}.')
            return
        if res[0][2] < time.time():
            cls._Log.error('Globals', f'Token has expired for {region}.')
            return
        session_admin = cls.session_admin_america if region == 'america' else cls.session_admin_aisa
        session_admin.headers.update({'Token': res[0][1]})
    
    @classmethod
    async def request_with_admin(cls, region, method, url, **kwargs):
        session_admin = cls.session_admin_america if region == 'america' else cls.session_admin_aisa
        if not session_admin.headers.get('Token'):
            cls.get_token_from_database(region)

        url_base = Globals._BASE_URL_AMERICA if region == 'america' else Globals._BASE_URL_ASIA
        session_admin = cls.session_admin_america if region == 'america' else cls.session_admin_aisa
        if not session_admin.headers.get('Token'):
            await cls.get_token_with_playwright(region)

        retry_count = 3
        while retry_count:
            try:
                response = session_admin.request(method, f'{url_base}{url}', **kwargs)
                if response.status_code != 200:
                    retry_count -= 1
                    Globals._Log.error('Globals', f'Failed to fetch data for {region}, status code: {response.status_code}')
                response_data = response.json()
                if response_data.get('code', 401) == 401:
                    Globals._Log.error('Globals', 'Authentication failed, invalid token.')
                    await cls.get_token_with_playwright(region)
                    continue
                return response_data
            except Exception as e:
                Globals._Log.error('Globals', f'Error in request_with_admin for {region}: {e}')
                retry_count -= 1
                time.sleep(3)
        return {}
    
    @classmethod
    def run_async_task(cls, func, *args, **kwargs):
        cls.thread_pool.start(Worker(func, *args, **kwargs))