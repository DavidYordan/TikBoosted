from twocaptcha import TwoCaptcha

from PyQt6.QtCore import QRunnable

from globals import Globals

class ToCaptcha(QRunnable):
    def __init__(self):
        super().__init__()

        self.user = 'ToCaptcha'

        Globals._Log.info(self.user, 'Successfully initialized.')