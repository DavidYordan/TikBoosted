import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from globals import Globals
from operate_sqlite import OperateSqlite

def config_init():
    with open('config/config.json', 'r') as file:
        config = json.load(file)

    Globals._ADMIN_USER = config['ADMIN_USER']
    Globals._ADMIN_PASSWORD = config['ADMIN_PASSWORD']
    Globals._BASE_URL_ASIA = config['BACKEND_TW']
    Globals._BASE_URL_AMERICA = config['BACKEND_US']
    Globals._PROXY_TW = config['PROXY_TW']
    Globals._PROXY_US = config['PROXY_US']
    Globals._SMMSKY_KEY = config['SMMSKY_KEY']
    Globals._SMMSKY_URL = config['SMMSKY_URL']
    Globals._SPREADSHEET_ID = config['SPREADSHEET_ID']
    Globals._TELEGRAM_BOT_TOKEN = config['TELEGRAM_BOT_TOKEN']
    Globals._TELEGRAM_CHATID = config['TELEGRAM_CHATID']
    Globals._TO_CAPTCHA_KEY = config['TO_CAPTCHA_KEY']

    credentials = Credentials.from_service_account_file(
        config['SERVICE_ACCOUNT_FILE'],
        scopes=config['SCOPES']
    )

    service = build('sheets', 'v4', credentials=credentials)

    Globals._SERVER_SHEET = service.spreadsheets()
    Globals._SQL = OperateSqlite(config['db_path'])