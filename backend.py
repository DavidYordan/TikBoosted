import requests

from PyQt6.QtCore import QRunnable

from globals import Globals

class Backend(QRunnable):
    def __init__(self):
        super().__init__()

        self.proxies_tw = {
            'http': Globals._PROXY_TW,
            'https': Globals._PROXY_TW
        }
        self.proxies_us = {
            'http': Globals._PROXY_US,
            'https': Globals._PROXY_US
        }
        self.user = 'Backend'

        Globals._Log.info(self.user, 'Successfully initialized.')

    def refresh_token(self, area):
        if area =='TW':
            res = requests.get()
        elif area == 'US':
            pass
        else:
            return False