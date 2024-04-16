from PyQt6.QtCore import (
    pyqtSlot,
    Qt
)
from PyQt6.QtWidgets import QDialog, QLabel, QVBoxLayout, QProgressBar

from globals import Globals

class ProgressDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Progress')
        self.setModal(True)

        layout = QVBoxLayout(self)
        self.label = QLabel('Waiting...')
        self.progressBar = QProgressBar()
        layout.addWidget(self.label)
        layout.addWidget(self.progressBar)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.WindowCloseButtonHint)

        Globals._WS.set_progress_bar_title_signal.connect(self.set_title)
        Globals._WS.toggle_progress_bar_signal.connect(self.toggle)
        Globals._WS.update_progress_signal.connect(self.update_progress)

    @pyqtSlot(str)
    def set_title(self, title):
        self.setWindowTitle(title)

    @pyqtSlot(bool)
    def toggle(self, visible):
        if visible:
            self.show()
        else:
            self.hide()

    @pyqtSlot(str, int)
    def update_progress(self, message, value):
        self.label.setText(message)
        self.progressBar.setValue(value)
