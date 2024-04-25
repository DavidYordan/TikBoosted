import random
import time
from PyQt6.QtCore import (
    pyqtSlot,
    Qt
)
from PyQt6.QtGui import (
    QIntValidator
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget
)
from queue import Queue

from globals import Globals

class VideosTabRight(QWidget):
    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        self.account = ''
        self.columns = ["videoId", "playCount","diggCount", "update", "create", "createTime", "updateTime", "commentCount",  "shareCount", "collectCount", "title"]
        self.user = 'VideosTabRight'
        self.setup_ui()
        Globals._WS.list_videos_right_signal.connect(self.list_videos)

        Globals._Log.info(self.user, 'Successfully initialized.')

    def cell_was_double_clicked(self):
        res = Globals._SQL.read('video_targets', condition=f"uniqueId='{self.account}' AND finished IS NOT TRUE")
        if res:
            QMessageBox.warning(None, 'Warning!', f'{self.account} have unfinished tasks.')
            return
        current_index = self.table.currentIndex()
        row = current_index.row()
        videoId = self.table.item(row, self.columns.index('videoId')).text()
        title = self.table.item(row, self.columns.index('title')).text()
        playCount = int(self.table.item(row, self.columns.index('playCount')).text())
        diggCount = int(self.table.item(row, self.columns.index('diggCount')).text())
        Globals._MUTEX_ACDICT.lock()
        followerCount = Globals.accounts_dict.get(self.account, {}).get('followerCount', 0)
        Globals._MUTEX_ACDICT.unlock()
        TargetDialog(self.account, videoId, title, followerCount, playCount, diggCount, self)

    def format_create_intervals(self, seconds):
        days, seconds = divmod(seconds, 86400)
        hours, seconds = divmod(seconds, 3600)
        minutes = seconds // 60
        return f"{days}d:{hours}h:{minutes}m"

    def format_update_intervals(self, seconds):
        days, seconds = divmod(seconds, 86400)
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        return f"{hours}h:{minutes}m:{seconds}s"

    @pyqtSlot(str)
    def list_videos(self, account):
        self.account = account
        if not account:
            return
        videos = Globals._SQL.read('videos', condition=f'account="{account}"')
        fields_sql = Globals._SQL.get_table_fields('videos')
        current_time = int(time.time())
        self.table.setRowCount(len(videos))
        for row, video in enumerate(videos):
            for idx, value in enumerate(video):
                field = fields_sql[idx]
                if field not in self.columns:
                    continue
                if field == 'createTime':
                    intervals = self.format_create_intervals(current_time - int(value))
                    self.table.setItem(row, self.columns.index('create'), QTableWidgetItem(intervals))
                elif field == 'updateTime':
                    intervals = self.format_update_intervals(current_time - int(value))
                    self.table.setItem(row, self.columns.index('update'), QTableWidgetItem(intervals))
                self.table.setItem(row, self.columns.index(field), QTableWidgetItem(str(value)))
        video_id_index = self.columns.index('videoId')
        self.table.sortItems(video_id_index, Qt.SortOrder.DescendingOrder)

    def reload(self):
        self.button_update_videos.setEnabled(False)
        self.list_videos(self.account)
        self.button_update_videos.setEnabled(True)

    def setup_ui(self):
        layout = QVBoxLayout(self)

        top_layout = QHBoxLayout()
        layout.addLayout(top_layout)
        self.button_update_videos = QPushButton('Reload')
        self.button_update_videos.clicked.connect(self.reload)
        Globals.components.append(self.button_update_videos)
        top_layout.addWidget(self.button_update_videos)
        top_layout.addStretch()

        middle_layout = QHBoxLayout()
        layout.addLayout(middle_layout)
        self.table = QTableWidget(0, len(self.columns))
        middle_layout.addWidget(self.table)
        header_labels = self.columns
        self.table.setHorizontalHeaderLabels(header_labels)
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 50)
        for column in range(0, self.table.columnCount()):
            self.table.horizontalHeader().setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(self.table.columnCount(), QHeaderView.ResizeMode.Fixed)

        self.table.doubleClicked.connect(self.cell_was_double_clicked)
        self.table.mousePressEvent = self.table_mouse_press_event

    def show_context_menu(self, pos, index):
        menu = QMenu()

        row = index.row()
        videoId = self.table.item(row, self.columns.index('videoId')).text()
        account_link = f'https://www.tiktok.com/@{self.account}'
        video_link = f'https://www.tiktok.com/@{self.account}/video/{videoId}'

        action_add_target = menu.addAction('Target Binding')
        action_add_target.triggered.connect(lambda _, r=row, v=videoId: self.show_target_dialog(r, v))

        action_add_playCount2830 = menu.addAction('Add PlayCount(2830)')
        action_add_playCount2830.triggered.connect(lambda _, l=video_link: self.show_play_dialog2830(l))

        action_add_playCount2787 = menu.addAction('Add PlayCount(2787)')
        action_add_playCount2787.triggered.connect(lambda _, l=video_link: self.show_play_dialog2787(l))
        
        action_add_diggCount = menu.addAction('Add DiggCount')
        action_add_diggCount.triggered.connect(lambda _, l=video_link: self.show_digg_dialog(l))

        action_add_followerCount = menu.addAction('Add FollowerCount')
        action_add_followerCount.triggered.connect(lambda _, l=account_link: self.show_follower_dialog(l))
        
        action_copy_tk_link = menu.addAction('Copy TK Link')
        action_copy_tk_link.triggered.connect(lambda: QApplication.clipboard().setText(account_link))

        action_copy_rs_link = menu.addAction('Copy Video Link')
        action_copy_rs_link.triggered.connect(lambda: QApplication.clipboard().setText(video_link))

        menu.exec(self.table.viewport().mapToGlobal(pos))

    def show_play_dialog2830(self, link):
        text, ok = QInputDialog.getText(self, 'Add PlayCount(2830)', 'Enter:', text=str(random.randint(500, 600)))
        if ok:
            Globals._WS.insert_orderIssuer_task_signal.emit((time.time(), ('add_order', {'action': 'add', 'service': 2830, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 2830: {text}')

    def show_play_dialog2787(self, link):
        text, ok = QInputDialog.getText(self, 'Add PlayCount(2787)', 'Enter:', text=str(random.randint(500, 600)))
        if ok:
            Globals._WS.insert_orderIssuer_task_signal.emit((time.time(), ('add_order', {'action': 'add', 'service': 2787, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 2787: {text}')

    def show_digg_dialog(self, link):
        text, ok = QInputDialog.getText(self, 'Add DiggCount', 'Enter:', text=str(random.randint(10, 40)))
        if ok:
            Globals._WS.insert_orderIssuer_task_signal.emit((time.time(), ('add_order', {'action': 'add', 'service': 2327, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 2327: {text}')

    def show_follower_dialog(self, link):
        text, ok = QInputDialog.getText(self, 'Add FollowerCount', 'Enter:', text=str(random.randint(100, 200)))
        if ok:
            Globals._WS.insert_orderIssuer_task_signal.emit((time.time(), ('add_order', {'action': 'add', 'service': 4057, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 4057: {text}')

    def show_target_dialog(self, row, videoId):
        res = Globals._SQL.read('video_targets', condition=f"uniqueId='{self.account}' AND finished IS NOT TRUE")
        if res:
            QMessageBox.warning(None, 'Warning!', f'{self.account} have unfinished tasks111.')
            return
        Globals._MUTEX_ACDICT.lock()
        followerCount = Globals.accounts_dict.get(self.account, {}).get('followerCount', 0)
        Globals._MUTEX_ACDICT.unlock()
        title = self.table.item(row, self.columns.index('title')).text()
        playCount = int(self.table.item(row, self.columns.index('playCount')).text())
        diggCount = int(self.table.item(row, self.columns.index('diggCount')).text())
        
        TargetDialog(self.account, videoId, title, followerCount, playCount, diggCount, self)

    def table_mouse_press_event(self, event):
        if event.button() == Qt.MouseButton.RightButton:
            index = self.table.indexAt(event.pos())
            if not index.isValid():
                return
            else:
                self.show_context_menu(event.pos(), index)
        else:
            QTableWidget.mousePressEvent(self.table, event)

class TargetDialog(QDialog):
    def __init__(
            self,
            account,
            videoId,
            videoTitle,
            followerCount,
            playCount,
            diggCount,
            parent=None
        ):
        super().__init__(parent)

        self.account = account
        self.init_followerCount = followerCount
        self.init_playCount = playCount
        self.init_diggCount = diggCount
        self.videoId = videoId
        self.videoTitle = videoTitle
        self.account_link = f'https://www.tiktok.com/@{self.account}'
        self.video_link = f'https://www.tiktok.com/@{self.account}/video/{self.videoId}'
        self.user = 'TargetDialog'

        self.setModal(True)
        self.setWindowTitle('Target Binding')
        self.init_ui()
        self.set_random_values()

        self.exec()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        title_label = QLabel(f'{self.account}: {self.videoId}')
        title_text = QLabel(self.videoTitle)
        title_text.setWordWrap(True)
        main_layout.addWidget(title_label)
        main_layout.addWidget(title_text)

        form_layout = QFormLayout()

        self.followerCount = QLineEdit()
        self.diggCount = QLineEdit()
        self.playCount = QLineEdit()

        self.followerCount.setValidator(QIntValidator(10, 2000, self.followerCount))
        self.diggCount.setValidator(QIntValidator(10, 10000, self.diggCount))
        self.playCount.setValidator(QIntValidator(100, 100000, self.playCount))

        self.followerCount.textChanged.connect(self.validate_counts)
        self.diggCount.textChanged.connect(self.validate_counts)
        self.playCount.textChanged.connect(self.validate_counts)

        form_layout.addRow("Follower:", self.followerCount)
        form_layout.addRow("Digg:", self.diggCount)
        form_layout.addRow("Play:", self.playCount)

        main_layout.addLayout(form_layout)

        button_layout = QHBoxLayout()
        self.btn_accept = QPushButton("Confirm")
        btn_random = QPushButton("Random")

        self.btn_accept.clicked.connect(self.on_accept)
        btn_random.clicked.connect(self.set_random_values)

        button_layout.addWidget(self.btn_accept)
        button_layout.addWidget(btn_random)
        main_layout.addLayout(button_layout)

        self.btn_accept.setEnabled(False)

    def on_accept(self):
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'video_targets',
            'columns': ['MAX(id)']
        }, q)
        max_id = q.get()[0][0] + 1
        data = {
            'client': Globals._CLIENT_ID,
            'id': max_id,
            'uniqueId': self.account,
            'videoId': self.videoId,
            'followerInit': self.init_followerCount,
            'followerTarget': int(self.followerCount.text()),
            'diggInit': self.init_diggCount,
            'diggTarget': int(self.diggCount.text()),
            'playInit': self.init_playCount,
            'playTarget': int(self.playCount.text()),
            'createTime': int(time.time()),
            'finished': False
        }
        if self.validate_final_counts():
            Globals._WS.database_operation_signal.emit('insert', {
                'table_name': 'video_targets',
                'columns': ['client', 'id', 'uniqueId', 'videoId', 'followerInit', 'followerTarget', 'diggInit', 'diggTarget', 'playInit', 'playTarget', 'createTime', 'finished'],
                'data': data
            }, None)
            Globals._MUTEX_BINDING.lock()
            Globals.binging_dict[self.account] = data
            Globals._MUTEX_BINDING.unlock()
            Globals._Log.info(self.user, f'Successfully submitted the binding task.')
            self.accept()
        else:
            Globals._Log.error(self.user, 'Validation failed. Please correct the data.')

    def set_random_values(self):
        if self.init_followerCount < 1000:
            start = 1000 - self.init_followerCount
            end = 1200 - self.init_followerCount
        else:
            start = 100
            end = 300
        follower = random.randint(max(start, 100), end)
        diggCount =random.randint(follower * 4, follower * 5)
        playCount = random.randint(diggCount * 12, diggCount * 18)
        self.followerCount.setText(str(follower))
        self.diggCount.setText(str(diggCount))
        self.playCount.setText(str(playCount))

    def validate_counts(self):
        if self.validate_final_counts():
            self.followerCount.setStyleSheet("")
            self.diggCount.setStyleSheet("")
            self.playCount.setStyleSheet("")
            self.btn_accept.setEnabled(True)
        else:
            self.followerCount.setStyleSheet("background-color: #ffcccc")
            self.diggCount.setStyleSheet("background-color: #ffcccc")
            self.playCount.setStyleSheet("background-color: #ffcccc")
            self.btn_accept.setEnabled(False)

    def validate_final_counts(self):
        try:
            follower = int(self.followerCount.text() or 0)
            digg = int(self.diggCount.text() or 0)
            play = int(self.playCount.text() or 0)

            return ((digg >= 4 * follower) and (play >= 10 * digg) and (play >= 100))
        except ValueError:
            return False