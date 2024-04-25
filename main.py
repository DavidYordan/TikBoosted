import os
import sys
from PyQt6.QtCore import (
    pyqtSlot,
    Qt
)
from PyQt6.QtGui import (
    QAction,
    QIcon,
    QKeySequence,
    QShortcut
)
from PyQt6.QtWidgets import (
    QApplication,
    QDialog,
    QHBoxLayout,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QStyleFactory,
    QTableWidget,
    QTabWidget,
    QVBoxLayout,
    QWidget
)

if __name__ == '__main__':
    app = QApplication(sys.argv)

from accounts_tab import AccountsTab
from agents_america_tab import AgentsAmericaTab
# from agents_asia_tab import AgentsAsiaTab
from config_dialog import ConfigDialog
from globals import Globals
from google_sync import GoogleSync
from products_america_tab import ProductsAmericaTab
# from products_asia_tab import ProductsAsiaTab
from progress_dialog import ProgressDialog
from orderIssuer import OrderIssuer
from telegram_bot import TelegramBot
from tiktok_spider import TikTokSpider
from users_america_tab import UsersAmericaTab
# from users_asia_tab import UsersAsiaTab
from videos_tab import VideosTab
from videos_tab_right import VideosTabRight
from xrayProcessor import XrayProcessor

class SearchDialog(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        self.table_widget = parent
        self.current_index = 0
        self.last_search_text = ''
        self.search_results = []
        self.setModal(True)
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("Search")
        self.main_layout = QVBoxLayout(self)

        self.top_layout = QHBoxLayout()
        self.search_field = QLineEdit(self)
        self.search_button = QPushButton("Search", self)
        self.top_layout.addWidget(self.search_field)
        self.top_layout.addWidget(self.search_button)

        self.bottom_layout = QHBoxLayout()
        self.prev_button = QPushButton("Previous(F1)", self)
        self.next_button = QPushButton("Next(F3)", self)
        self.bottom_layout.addWidget(self.prev_button)
        self.bottom_layout.addWidget(self.next_button)

        self.main_layout.addLayout(self.top_layout)
        self.main_layout.addLayout(self.bottom_layout)

        self.search_button.clicked.connect(self.on_search_clicked)
        self.prev_button.clicked.connect(self.prev_result)
        self.next_button.clicked.connect(self.next_result)
        self.prev_shortcut = QShortcut(QKeySequence('F1'), self)
        self.prev_shortcut.activated.connect(self.prev_result)
        self.next_shortcut = QShortcut(QKeySequence('F3'), self)
        self.next_shortcut.activated.connect(self.next_result)
        self.show()

    def highlight_result(self):
        row, col = self.search_results[self.current_index]
        self.table_widget.clearSelection()
        self.table_widget.setCurrentCell(row, col)
        self.table_widget.scrollToItem(self.table_widget.item(row, col), QTableWidget.ScrollHint.PositionAtCenter)

    def next_result(self):
        if self.search_results:
            self.current_index = (self.current_index + 1) % len(self.search_results)
            self.highlight_result()

    def on_search_clicked(self):
        search_text = self.search_field.text().lower()
        if search_text == self.last_search_text and self.search_results:
            self.next_result()
        else:
            self.last_search_text = search_text
            self.search_in_table()

    def prev_result(self):
        if self.search_results:
            self.current_index = (self.current_index - 1) % len(self.search_results)
            self.highlight_result()

    def search_in_table(self):
        self.search_results.clear()
        search_text = self.search_field.text().lower()
        for i in range(self.table_widget.rowCount()):
            for j in range(self.table_widget.columnCount()):
                item = self.table_widget.item(i, j)
                if item and search_text in item.text().lower():
                    self.search_results.append((i, j))
        
        if not self.search_results:
            Globals._Log.warning('Search', 'No matches found.')
            return

        self.current_index = 0
        self.highlight_result()

class TikBoosted(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle('TikBoosted')
        if hasattr(sys, '_MEIPASS'):
            icon_path = os.path.join(sys._MEIPASS, 'img/TikBoosted.ico')
        else:
            icon_path = ('img/TikBoosted.ico')
        self.setWindowIcon(QIcon(icon_path))
        self.resize(1080, 640)
        self.showMaximized()

        import config
        config.config_init()

        self.create_menu()
        self.create_main_panel()

        Globals._WS.toggle_components_signal.connect(self.toggle_components)

        self.progress_bar = ProgressDialog(self)

        self.googleSync = GoogleSync()
        Globals._WS.googlesync_insert_task_signal.connect(self.googleSync_insert_task)

        self.telegramBot = TelegramBot()
        Globals._WS.telegram_bot_signal.connect(self.telegram_bot_task)

        self.tiktokSpider = TikTokSpider()
        Globals._WS.insert_account_to_tiktok_spider.connect(self.insert_account_to_tiktok_spider)

        self.orderIssuer = OrderIssuer()
        Globals._WS.insert_orderIssuer_task_signal.connect(self.insert_orderIssuer_task)
        Globals._WS.orderIssuer_binding_check_signal.connect(self.orderIssuer_binding_check)
        Globals._WS.update_orderIssuer_order_signal.connect(self.videos_to_orderIssuer)

        self.xrayProcessor = XrayProcessor()

        self.user = 'TikBoosted'

        Globals._Log.info(self.user, 'Successfully initialized.')

    def activate_search(self):
        current_table = self.tab_left.currentWidget()
        if not hasattr(current_table, 'table'):
            return
        self.search_dialog = SearchDialog(current_table.table)

    def closeEvent(self, event):
        Globals.is_app_running = False
        Globals.thread_pool.waitForDone()
        super().closeEvent(event)

    def create_menu(self):
        menubar = self.menuBar()

        menu_menu = menubar.addMenu('Menu')
        Globals.components.append(menu_menu)
        action_create_users = QAction('Create Users', self)
        action_create_users.triggered.connect(self.create_users)
        action_create_users = QAction('Get Withdraw', self)
        action_create_users.triggered.connect(self.get_withdraw)
        menu_menu.addActions([action_create_users])

        menu_config = menubar.addMenu('Config')
        action_config = QAction('Config', self)
        action_config.triggered.connect(self.open_config_dialog)
        menu_config.addActions([action_config])

        menu_googleSync = menubar.addMenu('Google')
        Globals.components.append(menu_googleSync)
        self.action_googleSync_run = QAction('Run', self)
        self.action_googleSync_run.triggered.connect(self.googleSync_run)
        self.action_googleSync_stop = QAction('Stop', self)
        self.action_googleSync_stop.triggered.connect(self.googleSync_stop)
        self.action_googleSync_stop.setEnabled(False)
        self.action_googleSync_jump_load = QAction('Jump Load', self)
        self.action_googleSync_jump_load.triggered.connect(self.googleSync_jump_load)
        menu_googleSync.addActions([self.action_googleSync_run, self.action_googleSync_stop, self.action_googleSync_jump_load])

        menu_orderIssuer = menubar.addMenu('OrderIssuer')
        Globals.components.append(menu_orderIssuer)
        self.action_orderIssuer_run = QAction('Run', self)
        self.action_orderIssuer_run.triggered.connect(self.orderIssuer_run)
        self.action_orderIssuer_stop = QAction('Stop', self)
        self.action_orderIssuer_stop.triggered.connect(self.orderIssuer_stop)
        self.action_orderIssuer_stop.setEnabled(False)
        self.action_orderIssuer_update_services = QAction('Update Services', self)
        self.action_orderIssuer_update_services.triggered.connect(self.orderIssuer_update_services)
        menu_orderIssuer.addActions([self.action_orderIssuer_run, self.action_orderIssuer_stop, self.action_orderIssuer_update_services])

        menu_spider = menubar.addMenu('Spider')
        Globals.components.append(menu_spider)
        self.action_spider_run = QAction('Run', self)
        self.action_spider_run.triggered.connect(self.spider_run)
        self.action_spider_stop = QAction('Stop', self)
        self.action_spider_stop.setEnabled(False)
        self.action_spider_stop.triggered.connect(self.spider_stop)
        menu_spider.addActions([self.action_spider_run, self.action_spider_stop])

        menu_update = menubar.addMenu('Update')
        Globals.components.append(menu_update)
        action_update_users = QAction('Update Users', self)
        action_update_users.triggered.connect(self.update_users)
        action_update_products = QAction('Update Products', self)
        action_update_products.triggered.connect(self.update_products)
        menu_update.addActions([action_update_users, action_update_products])

        menu_test = menubar.addMenu('Test')
        self.action_telegram_bot_run = QAction('Run Bot', self)
        self.action_telegram_bot_run.triggered.connect(self.telegram_bot_run)
        self.action_telegram_bot_stop = QAction('Run Bot', self)
        self.action_telegram_bot_stop.setEnabled(False)
        self.action_telegram_bot_stop.triggered.connect(self.telegram_bot_stop)
        action_telegram_bot_test = QAction('Send Menu Test', self)
        action_telegram_bot_test.triggered.connect(lambda :Globals._WS.telegram_bot_signal.emit('send_text', {}))
        action_telegram_bot_file = QAction('Send Photo Test', self)
        action_telegram_bot_file.triggered.connect(lambda :Globals._WS.telegram_bot_signal.emit('send_photo', {}))
        self.action_xray_run = QAction('Run Xray', self)
        self.action_xray_run.triggered.connect(self.xray_run)
        self.action_xray_stop = QAction('Stop Xray', self)
        self.action_xray_stop.setEnabled(False)
        self.action_xray_stop.triggered.connect(self.xray_stop)
        menu_test.addActions([
            self.action_telegram_bot_run, self.action_telegram_bot_stop, action_telegram_bot_test, action_telegram_bot_file,
            self.action_xray_run, self.action_xray_stop
        ])

    def create_main_panel(self):
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)
        layout_main = QVBoxLayout(central_widget)
        layout_tabs = QHBoxLayout()
        layout_main.addLayout(layout_tabs, 100)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        layout_tabs.addWidget(splitter)

        splitter.setStyleSheet('''
            QSplitter::handle {
                background-color: #FFA500;
                border: 1px solid #E69500;
            }
            QSplitter::handle:hover {
                background-color: #FFB733;
            }
            QSplitter::handle:pressed {
                background-color: #CC8400;
            }
        ''')

        self.search_shortcut = QShortcut(QKeySequence('Ctrl+F'), self)
        self.search_shortcut.activated.connect(self.activate_search)

        self.tab_left = QTabWidget()
        splitter.addWidget(self.tab_left)

        self.accounts_tab = AccountsTab(
            self.tab_left
        )
        self.tab_left.addTab(self.accounts_tab, 'Ac')

        self.videos_tab = VideosTab(
            self.tab_left
        )
        self.tab_left.addTab(self.videos_tab, 'Videos')

        self.users_america_tab = UsersAmericaTab(
            self.tab_left
        )

        log_widget = QWidget()
        self.tab_left.addTab(log_widget, 'Log')
        log_vlayout = QVBoxLayout(log_widget)
        Globals._log_textedit.setReadOnly(True)
        Globals._log_textedit.document().setMaximumBlockCount(200)
        log_vlayout.addWidget(Globals._log_textedit, 11)

        self.tab_left.addTab(self.users_america_tab, 'UserAmer')

        # self.users_asia_tab = UsersAsiaTab(
        #     self.tab_left
        # )
        # self.tab_left.addTab(self.users_asia_tab, 'UserAsia')

        self.agents_america_tab = AgentsAmericaTab(
            self.tab_left
        )
        self.tab_left.addTab(self.agents_america_tab, 'AgenAmer')

        # self.agents_asia_tab = AgentsAsiaTab(
        #     self.tab_left
        # )
        # self.tab_left.addTab(self.agents_asia_tab, 'AgenAsia')

        # self.products_asia_tab = ProductsAsiaTab(
        #     self.tab_left
        # )
        # self.tab_left.addTab(self.products_asia_tab, 'ProAsia')

        self.products_america_tab = ProductsAmericaTab(
            self.tab_left
        )
        self.tab_left.addTab(self.products_america_tab, 'ProAmer')

        self.tab_right = QTabWidget()
        splitter.addWidget(self.tab_right)

        self.videos_tab = VideosTabRight(self.tab_right)
        self.tab_right.addTab(self.videos_tab, 'Videos')

        splitter.setSizes([300, 300])

        layout_main.addWidget(Globals._log_label, 1)
        Globals._log_label.setWordWrap(True)
        Globals._log_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        Globals._log_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        # Globals._log_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)

    def create_users(self):
        pass

    def get_withdraw(self):
        pass

    @pyqtSlot(tuple)
    def googleSync_insert_task(self, t):
        self.googleSync.insert_task(t)

    def googleSync_run(self):
        if not self.googleSync.is_running:
            self.googleSync.start_task()
        self.action_googleSync_run.setEnabled(False)
        self.action_googleSync_stop.setEnabled(True)

    def googleSync_stop(self):
        self.googleSync.stop_task()
        self.action_googleSync_run.setEnabled(True)
        self.action_googleSync_stop.setEnabled(False)

    def googleSync_jump_load(self):
        self.googleSync.queue.put((1, ('sync_google_accounts', {})))

    @pyqtSlot(str)
    def insert_account_to_tiktok_spider(self, account):
        self.tiktokSpider.insert_account(account)

    @pyqtSlot(object)
    def insert_orderIssuer_task(self, ob):
        self.orderIssuer.queue.put(ob)

    def open_config_dialog(self):
        dialog = ConfigDialog()
        dialog.exec()

    @pyqtSlot(list)
    def orderIssuer_binding_check(self, videos):
        self.orderIssuer.safe_put((6, ('binding_check', {'videos': videos})))

    def orderIssuer_run(self):
        if not self.orderIssuer.is_running:
            self.orderIssuer.start_task()
        self.action_orderIssuer_run.setEnabled(False)
        self.action_orderIssuer_stop.setEnabled(True)

    def orderIssuer_stop(self):
        self.orderIssuer.stop_task()
        self.action_orderIssuer_run.setEnabled(True)
        self.action_orderIssuer_stop.setEnabled(False)

    def orderIssuer_update_services(self):
        self.orderIssuer.queue.put((1, ('update_services', {'action': 'services'})))

    def spider_run(self):
        if not self.tiktokSpider.is_running:
            self.tiktokSpider.start_task()
        self.action_spider_run.setEnabled(False)
        self.action_spider_stop.setEnabled(True)

    def spider_stop(self):
        self.tiktokSpider.stop_task()
        self.action_spider_run.setEnabled(True)
        self.action_spider_stop.setEnabled(False)

    def telegram_bot_run(self):
        if not self.telegramBot.is_running:
            self.telegramBot.start_task()
        self.action_telegram_bot_run.setEnabled(False)
        self.action_telegram_bot_stop.setEnabled(True)

    def telegram_bot_stop(self):
        self.telegramBot.stop_task()
        self.action_telegram_bot_run.setEnabled(True)
        self.action_telegram_bot_stop.setEnabled(False)

    @pyqtSlot(str, dict)
    def telegram_bot_task(self, func, params):
        self.telegramBot.queue.put((func, params))

    @pyqtSlot(bool)
    def toggle_components(self, visible):
        if visible:
            for component in Globals.components:
                component.setDisabled(False)
        else:
            for component in Globals.components:
                component.setDisabled(True)

    def update_products(self):
        pass

    def update_users(self):
        pass

    @pyqtSlot(list)
    def videos_to_orderIssuer(self, videos):
        self.orderIssuer.safe_put((5, ('calculate_orders', {'videos': videos})))

    def xray_run(self):
        if not self.xrayProcessor.is_running:
            self.xrayProcessor.start_task()
        self.action_xray_run.setEnabled(False)
        self.action_xray_stop.setEnabled(True)

    def xray_stop(self):
        self.xrayProcessor.stop_task()
        self.action_xray_run.setEnabled(True)
        self.action_xray_stop.setEnabled(False)

if __name__ == '__main__':
    os.environ['PLAYWRIGHT_BROWSERS_PATH'] = './temp'
    os.environ['XDG_CACHE_HOME'] = './temp'
    os.environ['LOCALAPPDATA'] = './temp'
    os.environ['TEMP'] = './temp'
    os.environ['TMP'] = './temp'
    # app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create('Fusion'))
    app.setStyleSheet("""
        QTableWidget::item:selected {
            background-color: #3498db;
            color: #ffffff;
        }
    """)
    main_win = TikBoosted()
    main_win.show()
    sys.exit(app.exec())
