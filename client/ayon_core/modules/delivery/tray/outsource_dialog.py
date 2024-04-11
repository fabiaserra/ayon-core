import re
import sys
import platform
import traceback
from qtpy import QtCore, QtWidgets, QtGui

from ayon_shotgrid.lib import credentials

from ayon_core import style
from ayon_core import resources
from ayon_core.lib import Logger
from ayon_core.tools.utils import lib as tools_lib
from ayon_core.modules.delivery.scripts import sg_delivery
from ayon_core.tools.utils import ProjectsCombobox
from ayon_core.tools.context_dialog.window import (
    ContextDialogController,
)

logger = Logger.get_logger(__name__)


class OutsourceDialog(QtWidgets.QDialog):
    """Interface to control deliverying SG entities for outsource"""

    tool_title = "Outsource Deliver SG Entities"
    tool_name = "outsource_sg_entity_delivery"

    SIZE_W = 800
    SIZE_H = 400

    def __init__(self, parent=None):
        super(OutsourceDialog, self).__init__(parent)

        self.setWindowTitle(self.tool_title)

        icon = QtGui.QIcon(resources.get_openpype_icon_filepath())
        self.setWindowIcon(icon)

        self.setWindowFlags(
            QtCore.Qt.Window
            | QtCore.Qt.WindowTitleHint
            | QtCore.Qt.WindowCloseButtonHint
            | QtCore.Qt.WindowMaximizeButtonHint
            | QtCore.Qt.WindowMinimizeButtonHint
        )

        self.setMinimumSize(QtCore.QSize(self.SIZE_W, self.SIZE_H))

        self._first_show = True
        self._ignore_project_change = False

        # Short code name for currently selected project
        self._current_proj_code = None

        self._controller = ContextDialogController()

        self.ui_init()

    def ui_init(self):

        main_layout = QtWidgets.QVBoxLayout(self)

        input_widget = QtWidgets.QWidget()

        # Common input widgets for delivery and republish features
        input_layout = QtWidgets.QFormLayout(input_widget)
        input_layout.setContentsMargins(5, 5, 5, 5)

        # Project combobox
        projects_combobox = ProjectsCombobox(self._controller, input_widget)
        projects_combobox.set_select_item_visible(True)
        projects_combobox.set_active_filter_enabled(True)
        projects_combobox.selection_changed.connect(self.on_project_change)
        input_layout.addRow("Project", projects_combobox)

        # SG input widgets
        sg_input_widget = QtWidgets.QWidget()
        input_group = QtWidgets.QButtonGroup(sg_input_widget)
        input_group.setExclusive(True)

        sg_playlist_id_input = QtWidgets.QComboBox()
        sg_playlist_id_input.setMaxVisibleItems(30)
        # NOTE: this is required because the stylesheet otherwise doesn't show
        # the scrollable combobox
        sg_playlist_id_input.setStyleSheet("combobox-popup: 0;")
        sg_playlist_id_input.setToolTip("Integer id of the SG Playlist (i.e., '3909')")
        playlist_radio_btn = QtWidgets.QRadioButton("SG Playlist Id")
        playlist_radio_btn.setChecked(True)
        input_group.addButton(playlist_radio_btn)
        input_layout.addRow(playlist_radio_btn, sg_playlist_id_input)

        sg_version_id_input = QtWidgets.QLineEdit()
        sg_version_id_input.setToolTip("Integer id of the SG Version (i.e., '314726')")
        sg_version_id_input.textEdited.connect(self._version_id_edited)
        version_radio_btn = QtWidgets.QRadioButton("SG Version Id")
        input_group.addButton(version_radio_btn)
        input_layout.addRow(version_radio_btn, sg_version_id_input)

        main_layout.addWidget(sg_input_widget)

        main_layout.addWidget(input_widget)

        # Add button to generate delivery media
        outsource_delivery_btn = QtWidgets.QPushButton(
            "Deliver for outsource"
        )
        outsource_delivery_btn.setToolTip(
            "Run the outsource delivery pipeline so it copies the SG entities into"
            " a package in the '/proj/<proj_code>/io/outsource/ready_to_deliver' folder."
        )
        outsource_delivery_btn.clicked.connect(
            self._on_outsource_delivery_clicked
        )

        main_layout.addWidget(outsource_delivery_btn)

        #### REPORT ####
        text_area = QtWidgets.QTextEdit()
        text_area.setReadOnly(True)
        text_area.setVisible(False)

        main_layout.addWidget(text_area)

        # Assign widgets we want to reuse to class instance

        self._projects_combobox = projects_combobox
        self._sg_playlist_id_input = sg_playlist_id_input
        self._sg_playlist_btn = playlist_radio_btn
        self._sg_version_id_input = sg_version_id_input
        self._sg_version_btn = version_radio_btn
        self._text_area = text_area

    def keyPressEvent(self, event: QtGui.QKeyEvent):
        # Ignore enter key
        if event.key() == QtCore.Qt.Key_Enter or event.key() == QtCore.Qt.Key_Return:
            event.ignore()
        else:
            super().keyPressEvent(event)

    def showEvent(self, event):
        super(OutsourceDialog, self).showEvent(event)
        if self._first_show:
            self._first_show = False
            self.setStyleSheet(style.load_stylesheet())
            tools_lib.center_window(self)

        self._projects_combobox.refresh()

    def _version_id_edited(self, text):
        # If there's a comma in the text, remove it and set the modified text
        text = text.replace("\t", "")
        text = text.replace(" ", "")
        text = text.replace(",", "")
        self._sg_version_id_input.setText(text)
        self._sg_version_btn.setChecked(True)

    def on_project_change(self):
        if self._ignore_project_change:
            return

        project_name = self._controller.get_selected_project_name()

        sg = credentials.get_shotgrid_session()
        sg_project = sg.find_one(
            "Project",
            [["name", "is", project_name]],
            ["sg_code"]
        )

        title = "{} - {}".format(self.tool_title, project_name)
        self.setWindowTitle(title)

        # Store project code as class variable so we can reuse it throughout
        self._current_proj_code = sg_project.get("sg_code")

        # Add existing playlists from project
        sg_playlists = sg.find(
            "Playlist",
            [["project", "is", sg_project]],
            ["id", "code"]
        )
        self._sg_playlist_id_input.clear()
        if sg_playlists:
            playlist_items = [
                "{} ({})".format(p["code"], p["id"])
                for p in sg_playlists
            ]
            # Using reversed so they are ordered from newer to older
            self._sg_playlist_id_input.addItems(reversed(playlist_items))

    def _format_report(self, report_items, success):
        """Format final result and error details as html."""
        msg = "Delivery finished"
        if success:
            msg += " successfully"
        else:
            msg += " with errors"
        txt = "<h2>{}</h2>".format(msg)
        for header, data in report_items.items():
            txt += "<h3>{}</h3>".format(header)
            for item in data:
                txt += "{}<br>".format(item)

        return txt

    def _on_outsource_delivery_clicked(self):

        self._text_area.setText("Deliver in progress...")
        self._text_area.setVisible(True)

        QtWidgets.QApplication.processEvents()

        try:
            if self._sg_playlist_btn.isChecked():
                playlist_id_str = self._sg_playlist_id_input.currentText()
                playlist_id = re.search(r"\((\d+)\)$", playlist_id_str).group(1)
                report_items, success = sg_delivery.deliver_playlist_id(
                    playlist_id
                )
            else:
                report_items, success = sg_delivery.deliver_version_id(
                    self._sg_version_id_input.text()
                )
        except Exception:
            logger.error(traceback.format_exc())
            report_items = {
                "Error": [traceback.format_exc()]
            }
            success = False

        self._text_area.setText(self._format_report(report_items, success))


def main():
    app_instance = QtWidgets.QApplication.instance()
    if app_instance is None:
        app_instance = QtWidgets.QApplication([])

    if platform.system().lower() == "windows":
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("sg_outsource_delivery")

    window = OutsourceDialog()
    window.show()

    sys.exit(app_instance.exec_())
