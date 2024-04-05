# import os

from qtpy import QtWidgets

from ayon_core.lib import Logger
from ayon_core.modules.ingest.tray.batch_ingester import BatchIngester
from ayon_core.modules.ingest.tray.texture_publisher import TexturePublisher


class IngestTrayWrapper:
    def __init__(self, module):
        self.module = module
        self.log = Logger.get_logger(self.__class__.__name__)

        self.ingest_dialog = BatchIngester(module)
        self.texture_publisher = TexturePublisher(module)

    def show_ingest_dialog(self):
        self.ingest_dialog.show()
        self.ingest_dialog.activateWindow()
        self.ingest_dialog.raise_()

    def show_texture_dialog(self):
        self.texture_publisher.show()
        self.texture_publisher.activateWindow()
        self.texture_publisher.raise_()

    def tray_menu(self, parent_menu):
        tray_menu = QtWidgets.QMenu("Ingest", parent_menu)

        batch_ingest_action = QtWidgets.QAction("Batch Ingester", tray_menu)
        batch_ingest_action.triggered.connect(self.show_ingest_dialog)
        tray_menu.addAction(batch_ingest_action)

        texture_publish_action = QtWidgets.QAction("Texture Publisher", tray_menu)
        texture_publish_action.triggered.connect(self.show_texture_dialog)
        tray_menu.addAction(texture_publish_action)

        parent_menu.addMenu(tray_menu)
