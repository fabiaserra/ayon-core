# import os

from qtpy import QtWidgets

from ayon_core.lib import (
    Logger,
    run_detached_process,
    get_ayon_launcher_args
)


class DeliveryTrayWrapper:
    def __init__(self, module):
        self.module = module
        self.log = Logger.get_logger(self.__class__.__name__)

    def launch_sg_delivery_dialog(self):
        args = get_ayon_launcher_args(
            "addon", "delivery", "launch_sg_delivery"
        )
        run_detached_process(args)

    def launch_outsource_dialog(self):
        args = get_ayon_launcher_args(
            "addon", "delivery", "launch_outsource"
        )
        run_detached_process(args)

    def tray_menu(self, parent_menu):
        tray_menu = QtWidgets.QMenu("Delivery", parent_menu)

        show_delivery_action = QtWidgets.QAction(
            "Deliver SG Entities", tray_menu
        )
        show_delivery_action.triggered.connect(self.launch_sg_delivery_dialog)
        tray_menu.addAction(show_delivery_action)

        show_delivery_outsource_action = QtWidgets.QAction(
            "Deliver for Outsource", tray_menu
        )
        show_delivery_outsource_action.triggered.connect(self.launch_outsource_dialog)
        tray_menu.addAction(show_delivery_outsource_action)

        parent_menu.addMenu(tray_menu)
