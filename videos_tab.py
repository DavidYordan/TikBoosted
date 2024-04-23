import random
import time
from queue import Queue
from PyQt6.QtCore import (
    Qt
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QMenu,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget
)

from globals import Globals

class VideosTab(QWidget):
    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        self.columns = [
            'clientRemark', 'serverRemark', 'status', 'account', 'playCount', 'diggCount', 'update', 'create', 'title',
            'videoId', 'collectCount', 'commentCount', 'shareCount', 'updateTime', 'createTime'
        ]
        self.user = 'VideosTab'
        self.setup_ui()

        self.reload()

        Globals._Log.info(self.user, 'Successfully initialized.')

    def cell_was_clicked(self):
        current_index = self.table.currentIndex()
        text = self.table.item(current_index.row(), current_index.column()).text()
        Globals._log_label.setText(text)

    def cell_was_double_clicked(self):
        current_index = self.table.currentIndex()
        row = current_index.row()
        account = self.table.item(row, self.columns.index('account')).text()
        Globals._WS.list_videos_right_signal.emit(account)

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
    
    def list_videos(self, columns, videos):
        self.table.setSortingEnabled(False)
        Globals._MUTEX_ACDICT.lock()
        self.table.setRowCount(0)
        try:
            current_time = int(time.time())
            row = 0
            for video in videos:
                account = video[columns.index('account')]
                status = Globals.accounts_dict.get(account, {}).get('status', '')
                if status == '5.退學':
                    continue
                self.table.insertRow(row)
                clientRemark = Globals.accounts_dict.get(account, {}).get('clientRemark', '')
                serverRemark = Globals.accounts_dict.get(account, {}).get('serverRemark', '')
                self.set_item(row, self.columns.index('status'), status)
                self.set_item(row, self.columns.index('clientRemark'), clientRemark)
                self.set_item(row, self.columns.index('serverRemark'), serverRemark)
                for idx, value in enumerate(video):
                    field = columns[idx]
                    if field not in self.columns:
                        continue
                    if field == 'createTime':
                        intervals = self.format_create_intervals(current_time - int(value))
                        self.set_item(row, self.columns.index('create'), intervals)
                    elif field == 'updateTime':
                        intervals = self.format_update_intervals(current_time - int(value))
                        self.set_item(row, self.columns.index('update'), intervals)
                    self.set_item(row, self.columns.index(field), value)
                row += 1
            video_id_index = self.columns.index('videoId')
            self.table.sortItems(video_id_index, Qt.SortOrder.DescendingOrder)

        except Exception as e:
            Globals._Log.error(self.user, f'list_videos: {e}')

        finally:
            Globals._MUTEX_ACDICT.unlock()
            self.table.setSortingEnabled(True)

    def reload(self):
        self.button_reload.setEnabled(False)
        q = Queue()
        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'videos'}, q)
        columns = q.get()
        Globals._WS.database_operation_signal.emit('read', {'table_name': 'videos'}, q)
        videos = q.get()
        self.list_videos(columns, videos)
        self.button_reload.setEnabled(True)

    def set_item(self, row, col, value):
        self.table.setItem(row, col, QTableWidgetItem(str(value)))

    def setup_ui(self):
        layout = QVBoxLayout(self)

        top_layout = QHBoxLayout()
        layout.addLayout(top_layout)
        button_update = QPushButton('Update')
        button_update.setEnabled(False)
        Globals.components.append(button_update)
        # button_update.clicked.connect(self.update_agents)
        top_layout.addWidget(button_update)
        button_delete = QPushButton('Delete')
        button_delete.setEnabled(False)
        # button_delete.clicked.connect(self.delete_selected_rows)
        top_layout.addWidget(button_delete)
        self.button_reload = QPushButton('Reload')
        self.button_reload.clicked.connect(self.reload)
        top_layout.addWidget(self.button_reload)
        top_layout.addStretch()
        button_save = QPushButton('Save')
        button_save.setEnabled(False)
        # button_save.clicked.connect(self.save_data_to_file)
        top_layout.addWidget(button_save)

        middle_layout = QHBoxLayout()
        layout.addLayout(middle_layout)
        self.table = QTableWidget(0, len(self.columns))
        middle_layout.addWidget(self.table)
        self.table.setSortingEnabled(True)
        self.table.setHorizontalHeaderLabels(self.columns)
        
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
        videoId = self.table.item(row, self.columns.index('videoId')).text()
        account_link = f'https://www.tiktok.com/@{account}'
        video_link = f'https://www.tiktok.com/@{account}/video/{videoId}'

        action_add_playCount2830 = menu.addAction('Add PlayCount(2830)')
        action_add_playCount2830.triggered.connect(lambda _, l=video_link: self.show_play_dialog2830(l))

        action_add_playCount2787 = menu.addAction('Add PlayCount(2787)')
        action_add_playCount2787.triggered.connect(lambda _, l=video_link: self.show_play_dialog2787(l))
        
        action_add_diggCount = menu.addAction('Add DiggCount')
        action_add_diggCount.triggered.connect(lambda _, l=video_link: self.show_digg_dialog(l))

        action_add_followerCount = menu.addAction('Add FlowerCount')
        action_add_followerCount.triggered.connect(lambda _, l=account_link: self.show_follower_dialog(l))

        action_show_videos = menu.addAction('Show Videos')
        action_show_videos.triggered.connect(lambda _, a=account: Globals._WS.list_videos_right_signal.emit(a))

        action_copy_tk_link = menu.addAction('Copy TK Link')
        action_copy_tk_link.triggered.connect(lambda: QApplication.clipboard().setText(account_link))

        action_copy_rs_link = menu.addAction('Copy Video Link')
        action_copy_rs_link.triggered.connect(lambda: QApplication.clipboard().setText(video_link))

        menu.exec(self.table.viewport().mapToGlobal(pos))

    def show_play_dialog2830(self, link):
        text, ok = QInputDialog.getText(self, 'Add PlayCount(2830)', 'Enter:', text=str(random.randint(500, 600)))
        if ok:
            Globals._WS.insert_orderIssuer_task_signal.emit((int(time.time()), ('add_order', {'action': 'add', 'service': 2830, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 2830: {text}')

    def show_play_dialog2787(self, link):
        text, ok = QInputDialog.getText(self, 'Add PlayCount(2787)', 'Enter:', text=str(random.randint(500, 600)))
        if ok:
            Globals._WS.insert_orderIssuer_task_signal.emit((int(time.time()), ('add_order', {'action': 'add', 'service': 2787, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 2787: {text}')

    def show_digg_dialog(self, link):
        text, ok = QInputDialog.getText(self, 'Add DiggCount', 'Enter:', text=str(random.randint(10, 40)))
        if ok:
            Globals._WS.insert_orderIssuer_task_signal.emit((int(time.time()), ('add_order', {'action': 'add', 'service': 3558, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 3558: {text}')

    def show_follower_dialog(self, link):
        text, ok = QInputDialog.getText(self, 'Add FlowerCount', 'Enter:', text=str(random.randint(100, 500)))
        if ok:
            Globals._WS.insert_orderIssuer_task_signal.emit((int(time.time()), ('add_order', {'action': 'add', 'service': 4057, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 4057: {text}')

    def table_mouse_press_event(self, event):
        if event.button() == Qt.MouseButton.RightButton:
            index = self.table.indexAt(event.pos())
            if not index.isValid():
                return
            self.show_context_menu(event.pos(), index)
        else:
            QTableWidget.mousePressEvent(self.table, event)