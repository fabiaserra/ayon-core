import os
import re
import sys
import platform
import json
import traceback
from qtpy import QtCore, QtWidgets, QtGui

from ayon_shotgrid.lib import delivery, credentials

from ayon_core import style
from ayon_core import resources
from ayon_core.lib import Logger
from ayon_core.tools.utils import lib as tools_lib
from ayon_core.modules.delivery.scripts import media
from ayon_core.tools.utils import ProjectsCombobox
from ayon_core.tools.context_dialog.window import (
    ContextDialogController,
)


logger = Logger.get_logger(__name__)


class DeliveryDialog(QtWidgets.QDialog):
    """Interface to control SG deliveries"""

    tool_title = "Deliver SG Entities"
    tool_name = "sg_entity_delivery"

    SIZE_W = 1200
    SIZE_H = 800

    # File path to json file that contains defaults for the Delivery dialog inputs
    PROJ_DELIVERY_CONFIG = "/proj/{proj_code}/config/delivery/defaults.json"

    # Default string to use to identify ourselves to vendors
    VENDOR_DEFAULT = "ALKX"

    # Default string to use for package name override
    PACKAGE_NAME_DEFAULT = "{yyyy}{mm}{dd}_{vendor}"

    TOKENS_HELP = """
        {project[name]}: Project's full name
        {project[code]}: Project's code
        {seq}: Sequence entity name
        {episode}: Episode entity name
        {shot}: Shot entity name
        {shotnum}: The integer part of a shot name (eg. "uni_pg_0010" -> "0010")
        {asset_type}: Type of asset (eg. "Char", "Prop", "Environment")
        {folder[name]}: Name of the folder where the product lives.
        {task[name]}: Name of task
        {task[type]}: Type of task
        {task[short]}: Short name of task type (eg. 'Modeling' > 'mdl')
        {parent}: Name of hierarchical parent
        {version}: Version number
        {product[name]}: Product name
        {product[type]}: Product type
        {ext}: File extension
        {representation}: Representation name
        {frame}: Frame number for sequence files.
        {delivery_type}: Type of delivery output ("review" or "final")
    """

    def __init__(self, parent=None):
        super(DeliveryDialog, self).__init__(parent)

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

        delivery_outputs = DeliveryOutputsWidget()
        input_layout.addRow("Outputs {output}", delivery_outputs)

        # Add checkbox to choose whether we want to force the media to be
        # regenerated or not
        force_delivery_media_cb = QtWidgets.QCheckBox()
        force_delivery_media_cb.setChecked(False)
        force_delivery_media_cb.setToolTip(
            "Whether we want to force the generation of the delivery media "\
            "representations regardless if that version already exists or not " \
            "(i.e., need to create new slates)"
        )
        input_layout.addRow(
            "Force regeneration of media", force_delivery_media_cb
        )

        force_override_files_cb = QtWidgets.QCheckBox()
        force_override_files_cb.setChecked(False)
        force_override_files_cb.setToolTip(
            "Whether we want to force the generation of the media and override"\
            " the existing files if the destination path is the same."
        )
        input_layout.addRow(
            "Force override of files", force_override_files_cb
        )

        vendor_input = QtWidgets.QLineEdit(self.VENDOR_DEFAULT)
        vendor_input.setToolTip(
            "Template string used as a replacement of {vendor} on the path template."
        )
        input_layout.addRow("Vendor {vendor}", vendor_input)

        package_name_input = QtWidgets.QLineEdit(self.PACKAGE_NAME_DEFAULT)
        package_name_input.setToolTip(
            "Template string used as a replacement of {package_name} on the path template."
        )
        input_layout.addRow("Package name {package_name}", package_name_input)

        version_input = QtWidgets.QLineEdit("")
        version_input.setToolTip(
            "Override the version number of the delivery media. If left empty, " \
            "the version will just be increased from the last existing version. "
        )
        # Set the validator for the QLineEdit to QIntValidator
        version_input.setValidator(QtGui.QIntValidator())
        input_layout.addRow(
            "Version override {version}", version_input
        )

        task_override_combo = QtWidgets.QComboBox()
        task_override_combo.addItems(
            [
                media.USE_SOURCE_VALUE,
                "blockvis",
                "previs",
                "techvis",
                "postvis",
                "color",
                "dev",
                "layout",
                "anim",
                "comp",
                "precomp",
                "prod",
                "howto",
            ]
        )
        task_override_combo.setEditable(True)
        input_layout.addRow("Task short {task[short]}", task_override_combo)

        submission_notes_input = QtWidgets.QLineEdit("")
        submission_notes_input.setToolTip(
            "Override the 'Submission Notes' field of the SG versions. If left empty, " \
            "it will just be picked up from the SG version 'Submission Notes'."
        )
        input_layout.addRow(
            "Submission Notes override {submission_notes}",
            submission_notes_input
        )

        submit_for_input = QtWidgets.QLineEdit("")
        submit_for_input.setToolTip(
            "Override the 'Submit For' of the SG versions. If left empty, " \
            "it will just be picked up from the SG version 'Submit For'. "
        )
        input_layout.addRow(
            "Submit For override {submit_for}",
            submit_for_input
        )

        custom_tokens = KeyValueWidget()
        custom_tokens.setToolTip(
            "Key value pairs of new tokens to create so they can be used on "
            "template path. If you prefix the key with an output name, that "
            " key will only exist for that output (i.e., 'prores422_final:suffix')"
        )
        input_layout.addRow("Custom tokens", custom_tokens)

        filename_input = QtWidgets.QLineEdit(media.FILENAME_TEMPLATE_DEFAULT)
        filename_input.setToolTip(
            "Template string used as a replacement of {filename} on the path template."
        )
        input_layout.addRow("File name {filename}", filename_input)

        template_input = QtWidgets.QLineEdit(media.DELIVERY_TEMPLATE_DEFAULT)
        template_input.setToolTip(
            "Template string used as a replacement for where the delivery media "
            "will be written to.\n\nYou can make any of the tokens values capitalize"
            " by changing the format of the token.\n"
            "i.e., {{seq}} will keep the original value as is. {{Seq}} will capitalize "
            "the first letter of its value, {{SEQ}} will capitalize each letter."
            "\n\nTo make a token optional so it's ignored if it's not "
            "available on the entity you can just wrap it with '<' and '>'.\n"
            "i.e., <{{frame}}> will only be added in the case where {{frame}} "
            "doesn't exist on that specific output.\n\nYou can also nest optional tokens"
            "so if the first token doesn't exist, the rest don't get added.\n"
            "i.e.,'<{{is_sequence}}<{{filename}}/>>' the filename will only be added if"
            "the {{is_sequence}} token exists.\n\nAvailable tokens: {}".format(
                self.TOKENS_HELP
            )
        )

        input_layout.addRow("Path template", template_input)

        main_layout.addWidget(input_widget)

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

        # Add button to save defaults as config for project
        save_project_config_btn = QtWidgets.QPushButton(
            "Save settings to project as default"
        )
        save_project_config_btn.setToolTip(
            "Saves the current settings on the dialog as default on the project so next"
            " time the delivery dialog is launched with this project the defaults are "
            "populated"
        )
        save_project_config_btn.clicked.connect(
            self._on_save_config_clicked
        )

        main_layout.addWidget(save_project_config_btn)

        # Add button to generate delivery media
        generate_delivery_media_btn = QtWidgets.QPushButton(
            "Generate delivery media"
        )
        generate_delivery_media_btn.setToolTip(
            "Run the delivery media pipeline and ensure delivery media exists for all " \
            "outputs (Final Output, Review Output in ShotGrid)"
        )
        generate_delivery_media_btn.clicked.connect(
            self._on_generate_delivery_media_clicked
        )

        main_layout.addWidget(generate_delivery_media_btn)

        #### REPORT ####
        text_area = QtWidgets.QTextEdit()
        text_area.setReadOnly(True)
        text_area.setVisible(False)

        main_layout.addWidget(text_area)

        # Assign widgets we want to reuse to class instance

        self._projects_combobox = projects_combobox
        self._delivery_outputs = delivery_outputs
        self._force_delivery_media_cb = force_delivery_media_cb
        self._force_override_files_cb = force_override_files_cb
        self._vendor_input = vendor_input
        self._package_name_input = package_name_input
        self._filename_input = filename_input
        self._version_input = version_input
        self._task_override_combo = task_override_combo
        self._submission_notes_input = submission_notes_input
        self._submit_for_input = submit_for_input
        self._custom_tokens = custom_tokens
        self._template_input = template_input
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
        super(DeliveryDialog, self).showEvent(event)
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
            delivery.SG_DELIVERY_OUTPUT_FIELDS + ["sg_code"]
        )

        delivery_types = ["review", "final"]
        project_overrides = delivery.get_entity_overrides(
            sg,
            sg_project,
            delivery_types,
            query_fields=delivery.SG_DELIVERY_OUTPUT_FIELDS,
            query_ffmpeg_args=True
        )

        logger.debug("Found project overrides: %s", project_overrides)
        # Create list of tuples of output name and its extension
        outputs_name_ext = []
        for delivery_type in delivery_types:
            out_data_types = project_overrides.get(
                f"sg_{delivery_type}_output_type", {}
            )
            for data_type_name, data_type_args in out_data_types.items():
                out_name = f"{data_type_name.lower().replace(' ', '')}_{delivery_type}"
                out_extension = data_type_args["sg_extension"]
                outputs_name_ext.append((out_name, out_extension))

        logger.debug("Found outputs: %s", outputs_name_ext)
        self._delivery_outputs.update(outputs_name_ext)

        title = "{} - {}".format(self.tool_title, project_name)
        self.setWindowTitle(title)

        # Find project code from SG project and load config file if it exists
        proj_code = sg_project.get("sg_code")
        self._load_project_config(proj_code)

        # Store project code as class variable so we can reuse it throughout
        self._current_proj_code = proj_code

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

    def _save_project_config(self):
        proj_code = self._current_proj_code
        if not proj_code:
            logger.warning("No current project selected, can't save config")
            return

        config_path = self.PROJ_DELIVERY_CONFIG.format(proj_code=proj_code)

        config_path_dir = os.path.dirname(config_path)
        if not os.path.exists(config_path_dir):
            os.makedirs(config_path_dir)

        delivery_data = self._get_delivery_data()
        with open(config_path, "w") as f:
            logger.info(
                "Delivery config file for project created at '%s'",
                config_path
            )
            json.dump(delivery_data, f)

    def _load_project_config(self, proj_code):
        delivery_data = {}
        config_path = self.PROJ_DELIVERY_CONFIG.format(proj_code=proj_code)

        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                delivery_data = json.load(f)
        else:
            logger.info(
                "Delivery config file for project doesn't exist at '%s'",
                config_path
            )

        # TODO: abstract this away so it's simpler to add more widgets
        # that need to get preserved across sessions
        vendor_override = delivery_data.get("vendor_override")
        if vendor_override:
            self._vendor_input.setText(vendor_override)
        else:
            self._vendor_input.setText(self.VENDOR_DEFAULT)

        package_name_override = delivery_data.get("package_name_override")
        if package_name_override:
            self._package_name_input.setText(package_name_override)
        else:
            self._package_name_input.setText(self.PACKAGE_NAME_DEFAULT)

        custom_token_pairs = delivery_data.get("custom_tokens")
        self._custom_tokens.clear_pairs()
        if custom_token_pairs:
            for key, value in custom_token_pairs.items():
                self._custom_tokens.add_pair(key, value)

        filename_override = delivery_data.get("filename_override")
        if filename_override:
            self._filename_input.setText(filename_override)
        else:
            self._filename_input.setText(media.FILENAME_TEMPLATE_DEFAULT)

        template_override = delivery_data.get("template_path")
        if template_override:
            self._template_input.setText(template_override)
        else:
            self._template_input.setText(media.DELIVERY_TEMPLATE_DEFAULT)

        # Clear other widgets that aren't being loaded from the config
        self._submit_for_input.setText("")
        self._submission_notes_input.setText("")
        self._sg_version_id_input.setText("")
        self._text_area.setText("")
        self._text_area.setVisible(False)

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

    def _get_delivery_data(self):
        """Get all relevant data for the delivery"""
        delivery_data = {}
        delivery_data["output_names_ext"] = self._delivery_outputs.get_selected_outputs()
        delivery_data["force_delivery_media"] = self._force_delivery_media_cb.isChecked()
        delivery_data["force_override_files"] = self._force_override_files_cb.isChecked()
        delivery_data["vendor_override"] = self._vendor_input.text()
        delivery_data["package_name_override"] = self._package_name_input.text()
        delivery_data["version_override"] = self._version_input.text()
        delivery_data["task[short]_override"] = self._task_override_combo.currentText()
        delivery_data["submission_notes_override"] = self._submission_notes_input.text()
        delivery_data["submit_for_override"] = self._submit_for_input.text()
        delivery_data["custom_tokens"] = self._custom_tokens.get_pairs()
        delivery_data["filename_override"] = self._filename_input.text()
        delivery_data["template_path"] = self._template_input.text()

        proj_code = self._current_proj_code
        template_script = media.NUKE_DELIVERY_SCRIPT_DEFAULT
        if proj_code:
            proj_template_script = media.PROJ_NUKE_DELIVERY_SCRIPT.format(
                proj_code=proj_code
            )
            if os.path.exists(proj_template_script):
                template_script = proj_template_script
            else:
                logger.warning(
                    "Project Nuke template not found at '%s'",
                    proj_template_script
                )

        delivery_data["nuke_template_script"] = template_script

        return delivery_data

    def _on_generate_delivery_media_clicked(self):

        self._text_area.setText("Deliver in progress...")
        self._text_area.setVisible(True)

        QtWidgets.QApplication.processEvents()

        try:
            delivery_data = self._get_delivery_data()
            if self._sg_playlist_btn.isChecked():
                playlist_id_str = self._sg_playlist_id_input.currentText()
                playlist_id = re.search(r"\((\d+)\)$", playlist_id_str).group(1)
                report_items, success = media.generate_delivery_media_playlist_id(
                    playlist_id,
                    delivery_data=delivery_data,
                )
            else:
                report_items, success = media.generate_delivery_media_version_id(
                    self._sg_version_id_input.text(),
                    delivery_data=delivery_data,
                )
        except Exception:
            logger.error(traceback.format_exc())
            report_items = {
                "Error": [traceback.format_exc()]
            }
            success = False

        self._text_area.setText(self._format_report(report_items, success))
        self._text_area.setVisible(True)

    def _on_save_config_clicked(self):
        self._save_project_config()


class DeliveryOutputsWidget(QtWidgets.QWidget):
    """A widget for selecting delivery outputs.

    Attributes:
        delivery_widgets (dict): A dictionary of delivery widgets, keyed by
            output name.
        delivery_extensions (dict): A dictionary of delivery extensions, keyed
            by output name.
    """
    def __init__(self):
        super().__init__()

        # Create the layout
        self.layout = QtWidgets.QFormLayout(self)
        self.setLayout(self.layout)

        self.delivery_widgets = {}
        self.delivery_extensions = {}

    def update(self, outputs_name_ext):
        # Remove all existing rows
        for i in reversed(range(self.layout.count())):
            item = self.layout.itemAt(i)
            if item.widget() is not None:
                item.widget().deleteLater()
            self.layout.removeItem(item)

        self.delivery_widgets = {}
        self.delivery_extensions = {}
        if not outputs_name_ext:
            return

        # Add the new rows
        for name_ext in outputs_name_ext:
            name, ext = name_ext
            label = QtWidgets.QLabel(f"{name}")
            label.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
            checkbox = QtWidgets.QCheckBox(self)
            checkbox.setChecked(True)
            self.delivery_widgets[name] = checkbox
            self.delivery_extensions[name] = ext
            self.layout.addRow(label, checkbox)

    def get_selected_outputs(self):
        return [
            (output_name, self.delivery_extensions[output_name])
            for output_name, checkbox in self.delivery_widgets.items()
            if checkbox.isChecked()
        ]


class KeyValueWidget(QtWidgets.QWidget):
    """Widget to define key value pairs of strings."""
    def __init__(self):
        super().__init__()

        # Create the layout
        self.layout = QtWidgets.QVBoxLayout(self)
        self.setLayout(self.layout)

        # Create the add button
        self.add_button = QtWidgets.QPushButton("Add")
        self.add_button.clicked.connect(self.add_pair)
        self.layout.addWidget(self.add_button)

        # Create the scroll area
        self.scroll_area = QtWidgets.QScrollArea(self)
        self.scroll_area.setWidgetResizable(True)
        self.layout.addWidget(self.scroll_area)

        # Create the scroll area widget
        self.scroll_widget = QtWidgets.QWidget(self.scroll_area)
        self.scroll_area.setWidget(self.scroll_widget)

        # Create the scroll area layout
        self.scroll_layout = QtWidgets.QVBoxLayout(self.scroll_widget)
        self.scroll_widget.setLayout(self.scroll_layout)

        # Create the key-value pairs list
        self.pairs = []

    def clear_layout(self, layout):
        for i in reversed(range(layout.count())):
            widget = layout.itemAt(i).widget()
            if widget is not None:
                # Remove the widget from the layout
                widget.setParent(None)
                # Delete the widget
                widget.deleteLater()

    def clear_pairs(self):
        for pair in self.pairs:
            self.clear_layout(pair[-1])

        self.pairs.clear()

    def add_pair(self, key="", value=""):
        # Create the key-value pair widgets
        key_input = QtWidgets.QLineEdit(key)
        value_input = QtWidgets.QLineEdit(value)
        delete_button = QtWidgets.QPushButton("Delete")
        delete_button.clicked.connect(lambda: self.delete_pair(delete_button))

        # Add the key-value pair widgets to the layout
        pair_layout = QtWidgets.QHBoxLayout()
        pair_layout.addWidget(key_input)
        pair_layout.addWidget(value_input)
        pair_layout.addWidget(delete_button)
        self.scroll_layout.addLayout(pair_layout)

        # Add the key-value pair to the list
        self.pairs.append((key_input, value_input, delete_button, pair_layout))

    def delete_pair(self, delete_button):
        # Find the key-value pair that corresponds to the delete button
        for pair in self.pairs:
            if pair[2] == delete_button:
                key_input, value_input, delete_button, pair_layout = pair
                break

        # Remove the key-value pair from the layout and the list
        # pair_layout = delete_button.layout()
        self.clear_layout(pair_layout)
        self.pairs.remove((key_input, value_input, delete_button, pair_layout))

    def get_pairs(self):
        # Return the key-value pairs as a dictionary
        return {
            key_input.text(): value_input.text()
            for key_input, value_input, _, _ in self.pairs
        }


def main():
    app_instance = QtWidgets.QApplication.instance()
    if app_instance is None:
        app_instance = QtWidgets.QApplication([])

    if platform.system().lower() == "windows":
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("sg_delivery")

    window = DeliveryDialog()
    window.show()

    sys.exit(app_instance.exec_())
