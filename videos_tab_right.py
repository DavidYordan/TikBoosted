import random
import time
from PyQt6.QtCore import (
    pyqtSlot,
    Qt
)
from PyQt6.QtGui import (
    QIntValidator,
    QValidator
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
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget
)

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
        current_index = self.table.currentIndex()
        row = current_index.row()
        videoId = self.table.item(row, self.columns.index('videoId')).text()
        title = self.table.item(row, self.columns.index('title')).text()
        playCount = int(self.table.item(row, self.columns.index('playCount')).text())
        diggCount = int(self.table.item(row, self.columns.index('diggCount')).text())
        ServersDialog(self.account, videoId, title, playCount, diggCount, self)

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

        action_add_playCount1757 = menu.addAction('Add PlayCount(1757)')
        action_add_playCount1757.triggered.connect(lambda _, l=video_link: self.show_play_dialog1757(l))

        # action_add_playCount1757 = menu.addAction('Add PlayCount(3713)')
        # action_add_playCount1757.triggered.connect(lambda _, l=video_link: self.show_play_dialog3713(l))
        
        action_add_diggCount = menu.addAction('Add DiggCount')
        action_add_diggCount.triggered.connect(lambda _, l=video_link: self.show_digg_dialog(l))

        action_add_followerCount = menu.addAction('Add FlowerCount')
        action_add_followerCount.triggered.connect(lambda _, l=account_link: self.show_follower_dialog(l))
        
        action_copy_tk_link = menu.addAction('Copy TK Link')
        action_copy_tk_link.triggered.connect(lambda: QApplication.clipboard().setText(account_link))

        action_copy_rs_link = menu.addAction('Copy Video Link')
        action_copy_rs_link.triggered.connect(lambda: QApplication.clipboard().setText(video_link))

        menu.exec(self.table.viewport().mapToGlobal(pos))

    def show_play_dialog1757(self, link):
        text, ok = QInputDialog.getText(self, 'Add PlayCount(1757)', 'Enter:', text=str(random.randint(500, 600)))
        if ok:
            Globals._WS.insert_smmsky_task_signal.emit((int(time.time()), ('add_order', {'action': 'add', 'service': 1757, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 1757: {text}')

    def show_play_dialog3713(self, link):
        text, ok = QInputDialog.getText(self, 'Add PlayCount(3713)', 'Enter:', text=str(random.randint(500, 600)))
        if ok:
            Globals._WS.insert_smmsky_task_signal.emit((int(time.time()), ('add_order', {'action': 'add', 'service': 3713, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 3713: {text}')

    def show_digg_dialog(self, link):
        text, ok = QInputDialog.getText(self, 'Add DiggCount', 'Enter:', text=str(random.randint(10, 40)))
        if ok:
            Globals._WS.insert_smmsky_task_signal.emit((int(time.time()), ('add_order', {'action': 'add', 'service': 3558, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 3558: {text}')

    def show_follower_dialog(self, link):
        text, ok = QInputDialog.getText(self, 'Add FlowerCount', 'Enter:', text=str(random.randint(100, 500)))
        if ok:
            Globals._WS.insert_smmsky_task_signal.emit((int(time.time()), ('add_order', {'action': 'add', 'service': 4057, 'link': link, 'quantity': int(text)})))
            Globals._Log.info(self.user, f'Successfully inserted task 4057: {text}')

    def table_mouse_press_event(self, event):
        if event.button() == Qt.MouseButton.RightButton:
            index = self.table.indexAt(event.pos())
            if not index.isValid():
                return
            else:
                self.show_context_menu(event.pos(), index)
        else:
            QTableWidget.mousePressEvent(self.table, event)

class CustomIntValidator(QValidator):
    def __init__(self, minimum, maximum, zero_allowed, parent=None):
        super().__init__(parent)
        self.minimum = minimum
        self.maximum = maximum
        self.zero_allowed = zero_allowed

    def validate(self, input, pos):
        if input == "":
            return QValidator.State.Intermediate, input, pos
        try:
            int_input = int(input)
            if (self.minimum <= int_input <= self.maximum) or (self.zero_allowed and int_input == 0):
                return QValidator.State.Acceptable, input, pos
            return QValidator.State.Intermediate, input, pos
        except ValueError:
            return QValidator.State.Intermediate, input, pos

class ServersDialog(QDialog):
    def __init__(
            self,
            uniqueId,
            videoId,
            videoTitle,
            playCount,
            diggCount,
            parent=None
        ):
        super().__init__(parent)

        self.init_playCount = playCount
        self.init_diggCount = diggCount
        self.videoId = videoId
        self.videoTitle = videoTitle
        self.uniqueId = uniqueId
        self.user = 'ServersDialog'

        self.setModal(True)
        self.setWindowTitle('Servers Dialog')
        self.init_ui()
        self.set_random_values()

        self.exec()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        title_label = QLabel(f'{self.uniqueId}: {self.videoId}')
        title_text = QLabel(self.videoTitle)
        title_text.setWordWrap(True)
        main_layout.addWidget(title_label)
        main_layout.addWidget(title_text)

        form_layout = QFormLayout()

        self.flowerCount = QLineEdit()
        self.diggCount = QLineEdit()
        self.playCount = QLineEdit()

        self.flowerCount.setValidator(CustomIntValidator(100, 1000, True, self.flowerCount))
        self.diggCount.setValidator(CustomIntValidator(10, 10000, True, self.diggCount))
        self.playCount.setValidator(CustomIntValidator(100, 100000, True, self.playCount))

        self.flowerCount.textChanged.connect(self.validate_counts)
        self.diggCount.textChanged.connect(self.validate_counts)
        self.playCount.textChanged.connect(self.validate_counts)

        form_layout.addRow("Flower Count:", self.flowerCount)
        form_layout.addRow("Digg Count:", self.diggCount)
        form_layout.addRow("Play Count:", self.playCount)

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
        flower = self.flowerCount.text()
        digg = self.diggCount.text()
        play = self.playCount.text()

        if self.validate_final_counts():
            print("Flower Count:", flower)
            print("Digg Count:", digg)
            print("Play Count:", play)
            Globals._WS.insert_smmsky_task_signal.emit()
            Globals._Log.info(self.user, 'Susscess')
            self.accept()
        else:
            Globals._Log.error(self.user, 'Validation failed. Please correct the data.')

    def set_random_values(self):
        if self.init_diggCount * 10 > self.init_playCount:
            diggCount = 0
        else:
            diggCount = random.randint(10, 40)
        self.flowerCount.setText(str(0))
        self.diggCount.setText(str(diggCount))
        self.playCount.setText(str(random.randint(500, 600)))

    def validate_counts(self):
        if self.validate_final_counts():
            self.flowerCount.setStyleSheet("")
            self.diggCount.setStyleSheet("")
            self.playCount.setStyleSheet("")
            self.btn_accept.setEnabled(True)
        else:
            self.flowerCount.setStyleSheet("background-color: #ffcccc")
            self.diggCount.setStyleSheet("background-color: #ffcccc")
            self.playCount.setStyleSheet("background-color: #ffcccc")
            self.btn_accept.setEnabled(False)

    def validate_final_counts(self):
        try:
            flower = int(self.flowerCount.text() or 0)
            digg = int(self.diggCount.text() or 0)
            play = int(self.playCount.text() or 0)

            return ((digg >= 3 * flower or flower == 0) and 
                    (play >= 10 * digg or digg == 0) and 
                    (flower == 0 or (100 <= flower <= 1000)) and 
                    (digg == 0 or (10 <= digg <= 10000)) and 
                    (play == 0 or (100 <= play <= 100000)))
        except ValueError:
            return False