# import os

from qtpy import QtWidgets

from ayon_core.lib import (
    Logger,
    run_detached_process,
    get_ayon_launcher_args
)


class IngestTrayWrapper:
    def __init__(self, module):
        self.module = module
        self.log = Logger.get_logger(self.__class__.__name__)

    def launch_batch_ingester(self):
        args = get_ayon_launcher_args(
            "addon", "ingest", "launch_batch_ingester"
        )
        run_detached_process(args)

    def launch_texture_publisher(self):
        args = get_ayon_launcher_args(
            "addon", "ingest", "launch_texture_publisher"
        )
        run_detached_process(args)

    def tray_menu(self, parent_menu):
        tray_menu = QtWidgets.QMenu("Ingest", parent_menu)

        batch_ingest_action = QtWidgets.QAction("Batch Ingester", tray_menu)
        batch_ingest_action.triggered.connect(self.launch_batch_ingester)
        tray_menu.addAction(batch_ingest_action)

        texture_publish_action = QtWidgets.QAction("Texture Publisher", tray_menu)
        texture_publish_action.triggered.connect(self.launch_texture_publisher)
        tray_menu.addAction(texture_publish_action)

        parent_menu.addMenu(tray_menu)
