import os
import attr
import sys
import platform
import traceback

from qtpy import QtCore, QtWidgets, QtGui
import qtawesome

import ayon_api
from ayon_shotgrid.lib import credentials

from ayon_core import style
from ayon_core import resources
from ayon_core.lib import Logger
from ayon_core.lib.transcoding import IMAGE_EXTENSIONS
from ayon_core.tools.utils import lib as tools_lib
from ayon_core.modules.ingest.lib import textures
from ayon_core.modules.ingest.scripts import ingest


logger = Logger.get_logger(__name__)

HEADER_NAME_ROLE = QtCore.Qt.UserRole + 510
EDIT_ICON_ROLE = QtCore.Qt.UserRole + 511


class TexturePublisher(QtWidgets.QDialog):
    """Interface to control SG deliveries"""

    tool_title = "Texture Publisher"
    tool_name = "texture_publisher"

    SIZE_W = 1800
    SIZE_H = 800

    DEFAULT_WIDTHS = (
        ("path", 1000),
        ("in_colorspace", 120),
        ("folder_path", 120),
        ("product_name", 120),
        ("version", 120)
    )

    def __init__(self, module, parent=None):
        super(TexturePublisher, self).__init__(parent)

        self.setWindowTitle(self.tool_title)

        self._module = module

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
        self._initial_refresh = False
        self._ignore_project_change = False

        self._current_proj_name = None
        self._current_proj_code = None

        self.ui_init()

    def ui_init(self):

        main_layout = QtWidgets.QVBoxLayout(self)

        input_widget = QtWidgets.QWidget()

        # Common input widgets for delivery and republish features
        input_layout = QtWidgets.QFormLayout(input_widget)
        input_layout.setContentsMargins(5, 5, 5, 5)

        # Project combobox
        projects_combobox = QtWidgets.QComboBox()
        combobox_delegate = QtWidgets.QStyledItemDelegate(self)
        projects_combobox.setItemDelegate(combobox_delegate)
        projects_combobox.currentTextChanged.connect(self.on_project_change)
        input_layout.addRow("Project", projects_combobox)

        file_browser = FileBrowserWidget()
        file_browser.filepath_changed.connect(self.on_filepath_changed)

        input_layout.addRow("Textures folder", file_browser)

        main_layout.addWidget(input_widget)

        overwrite_version_cb = QtWidgets.QCheckBox()
        overwrite_version_cb.setChecked(False)
        overwrite_version_cb.setToolTip(
            "Whether we want to overwrite the version if it has already been published"
        )

        input_layout.addRow(
            "Overwrite existing versions", overwrite_version_cb
        )

        # Table with all the products we find in the given folder
        table_view = QtWidgets.QTableView()
        headers = [item[0] for item in self.DEFAULT_WIDTHS]

        model = TexturesTableModel(headers, parent=self)

        table_view.setModel(model)
        table_view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)

        table_view.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        table_view.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        table_view.horizontalHeader().setSortIndicator(-1, QtCore.Qt.AscendingOrder)
        table_view.setAlternatingRowColors(True)
        table_view.verticalHeader().hide()
        table_view.viewport().setAttribute(QtCore.Qt.WA_Hover, True)

        table_view.setSortingEnabled(True)
        table_view.setTextElideMode(QtCore.Qt.ElideLeft)

        header = table_view.horizontalHeader()
        for column_name, width in self.DEFAULT_WIDTHS:
            idx = model.get_header_index(column_name)
            header.setSectionResizeMode(idx, QtWidgets.QHeaderView.Interactive)
            table_view.setColumnWidth(idx, width)

        header.setStretchLastSection(True)

        # Add delegates to automatically fill possible options on columns
        task_delegate = ComboBoxDelegate(textures.IN_COLORSPACES, parent=self)
        column = model.get_header_index("in_colorspace")
        table_view.setItemDelegateForColumn(column, task_delegate)

        main_layout.addWidget(table_view)

        # Add button to validate textures
        validate_btn = QtWidgets.QPushButton(
            "Validate Textures"
        )
        validate_btn.setToolTip(
            "Do a dry-run validation that texture publish won't error out on submission"
        )
        validate_btn.clicked.connect(self._on_validate_clicked)

        main_layout.addWidget(validate_btn)

        # Add button to publish textures
        publish_btn = QtWidgets.QPushButton(
            "Publish Textures"
        )
        publish_btn.setToolTip(
            "Submit all textures to publish in Deadline"
        )
        publish_btn.clicked.connect(self._on_publish_clicked)

        main_layout.addWidget(publish_btn)

        #### REPORT ####
        message_label = QtWidgets.QLabel("")
        message_label.setWordWrap(True)
        message_label.hide()
        main_layout.addWidget(message_label)

        text_area = QtWidgets.QTextEdit()
        text_area.setReadOnly(True)
        text_area.setVisible(False)

        main_layout.addWidget(text_area)

        # Assign widgets we want to reuse to class instance
        self._projects_combobox = projects_combobox
        self._overwrite_version_cb = overwrite_version_cb
        self._file_browser = file_browser
        self._table_view = table_view
        self._model = model
        self._message_label = message_label
        self._text_area = text_area

    def keyPressEvent(self, event: QtGui.QKeyEvent):
        if event.key() == QtCore.Qt.Key_Delete or event.key() == QtCore.Qt.Key_Backspace:
            # Get the selected rows
            selected_indexes = self._table_view.selectedIndexes()

            # Get unique rows
            unique_rows = set(index.row() for index in selected_indexes)

            # Delete the selected rows
            for row in sorted(unique_rows, reverse=True):
                self._model.removeRow(row)
        # Ignore enter key
        elif event.key() == QtCore.Qt.Key_Enter or event.key() == QtCore.Qt.Key_Return:
            event.ignore()
        else:
            super().keyPressEvent(event)

    def showEvent(self, event):
        super(TexturePublisher, self).showEvent(event)
        if self._first_show:
            self._first_show = False
            self.setStyleSheet(style.load_stylesheet())
            tools_lib.center_window(self)

        if not self._initial_refresh:
            self._initial_refresh = True
            self.refresh()

    def _refresh(self):
        if not self._initial_refresh:
            self._initial_refresh = True
        self._set_projects()

    def _set_projects(self):
        # Store current project
        old_project_name = self.current_project

        self._ignore_project_change = True

        # Cleanup
        self._projects_combobox.clear()

        # Fill combobox with projects
        select_project_item = QtGui.QStandardItem("< Select project >")
        select_project_item.setData(None, QtCore.Qt.UserRole + 1)

        combobox_items = [select_project_item]

        project_names = self.get_filtered_projects()

        for project_name in sorted(project_names):
            item = QtGui.QStandardItem(project_name)
            item.setData(project_name, QtCore.Qt.UserRole + 1)
            combobox_items.append(item)

        root_item = self._projects_combobox.model().invisibleRootItem()
        root_item.appendRows(combobox_items)

        index = 0
        self._ignore_project_change = False

        if old_project_name:
            index = self._projects_combobox.findText(
                old_project_name, QtCore.Qt.MatchFixedString
            )

        self._projects_combobox.setCurrentIndex(index)

    @property
    def current_project(self):
        return self.dbcon.active_project() or None

    def get_filtered_projects(self):
        projects = list()
        for project in ayon_api.get_projects(fields=["name", "data.active", "data.library_project"]):
            is_active = project.get("data", {}).get("active", False)
            is_library = project.get("data", {}).get("library_project", False)
            if is_active or is_library:
                projects.append(project["name"])

        return projects

    def on_project_change(self):
        if self._ignore_project_change:
            return

        row = self._projects_combobox.currentIndex()
        index = self._projects_combobox.model().index(row, 0)
        project_name = index.data(QtCore.Qt.UserRole + 1)

        sg = credentials.get_shotgrid_session()
        sg_project = sg.find_one(
            "Project",
            [["name", "is", project_name]],
            fields=["sg_code"]
        )

        project_name = self.dbcon.active_project() or "No project selected"
        title = "{} - {}".format(self.tool_title, project_name)
        self.setWindowTitle(title)

        # Store project name and code as class variable so we can reuse it throughout
        self._current_proj_name = project_name
        proj_code = sg_project.get("sg_code")
        self._current_proj_code = proj_code

        self._file_browser.set_default_directory(f"/proj/{proj_code}/")

    def set_message(self, msg):
        self._message_label.setText(msg)
        self._message_label.show()

    def on_filepath_changed(self, filepath):
        filepath = filepath.strip()
        if not os.path.exists(filepath):
            msg = f"Filepath '{filepath}' does not exist!"
            logger.error(msg)
            self.set_message(msg)
            return

        row = self._projects_combobox.currentIndex()
        index = self._projects_combobox.model().index(row, 0)
        project_name = index.data(QtCore.Qt.UserRole + 1)
        if not project_name:
            msg = "Must select a project first."
            logger.error(msg)
            self.set_message(msg)
            return

        products = ingest.get_products_from_filepath(
            filepath,
            project_name,
            self._current_proj_code
        )
        self._model.set_products(products)

    def _on_validate_clicked(self):
        self._text_area.setText("Validate in progress...")
        self._text_area.setVisible(True)

        QtWidgets.QApplication.processEvents()

        try:
            products_data = self._model.get_products()
            report_items, success = ingest.validate_products(
                self._current_proj_name,
                products_data,
                self._overwrite_version_cb.isChecked(),
                force_task_creation=True,
            )

        except Exception:
            logger.error(traceback.format_exc())
            report_items = {
                "Error": [traceback.format_exc()]
            }
            success = False

        self._text_area.setText(self._format_report(report_items, success, label="Validation"))

    def _on_publish_clicked(self):
        self._text_area.setText("Publish in progress...")
        self._text_area.setVisible(True)

        QtWidgets.QApplication.processEvents()

        try:
            products_data = self._model.get_products()
            report_items, success = ingest.publish_products(
                self._current_proj_name,
                products_data,
                self._overwrite_version_cb.isChecked(),
                force_task_creation=True,
            )

        except Exception:
            logger.error(traceback.format_exc())
            report_items = {
                "Error": [traceback.format_exc()]
            }
            success = False

        self._text_area.setText(self._format_report(report_items, success))

    def _format_report(self, report_items, success, label="Ingest"):
        """Format final result and error details as html."""
        msg = "{} finished".format(label)
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

    # -------------------------------
    # Delay calling blocking methods
    # -------------------------------

    def refresh(self):
        tools_lib.schedule(self._refresh, 50, channel="mongo")


class FileBrowserWidget(QtWidgets.QWidget):

    filepath_changed = QtCore.Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.filepath_edit = QtWidgets.QLineEdit()
        self.browse_button = QtWidgets.QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse_file)
        self.filepath_edit.textChanged.connect(self.emit_filepath_changed)

        self.default_directory = ""

        layout = QtWidgets.QHBoxLayout(self)
        layout.addWidget(self.filepath_edit)
        layout.addWidget(self.browse_button)

    def browse_file(self):
        options = QtWidgets.QFileDialog.Options()
        options |= QtWidgets.QFileDialog.DontUseNativeDialog
        options |= QtWidgets.QFileDialog.ShowDirsOnly

        directory = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            "Select a directory with textures to publish",
            self.default_directory,
            options=options
        )
        if directory:
            self.filepath_edit.setText(directory)

    def emit_filepath_changed(self, text):
        self.filepath_changed.emit(text)

    def get_filepath(self):
        return self.filepath_edit.text()

    def set_default_directory(self, path):
        self.default_directory = path


class TexturesTableModel(QtCore.QAbstractTableModel):

    COLUMN_LABELS = [
        ("path", "Filepath"),
        ("in_colorspace", "In Colorspace"),
        ("folder_path", "Asset"),
        ("product_name", "Subset"),
        ("version", "Version"),
    ]

    EDITABLE_COLUMNS = ["folder_path", "in_colorspace", "product_name", "version"]

    UNNECESSARY_COLUMNS = ["version"]

    _tooltips = [
        "Source path of the texture",
        "Input colorspace of the texture",
        "Folder path to publish product to (i.e., '160/tsc_160_0010')",
        "Name of the product.",
        "Version number to use for publising. If left empty it will simply publish as the next version available."
    ]

    @attr.s
    class TextureRepresentation:
        path = attr.ib()
        folder_path = attr.ib()
        task = attr.ib()
        family = attr.ib()
        product_name = attr.ib()
        in_colorspace = attr.ib()
        rep_name = attr.ib()
        version = attr.ib(type=int)

    def __init__(self, header, parent=None):
        super().__init__(parent=parent)
        self._header = header
        self._data = []

        self.edit_icon = qtawesome.icon("fa.edit", color="white")

    def rowCount(self, parent=None):
        return len(self._data)

    def columnCount(self, parent=None):
        return len(self._header)

    def get_column(self, index):
        """Return info about column

        Args:
            index (QModelIndex)

        Returns:
            (tuple): (COLUMN_NAME: COLUMN_LABEL)
        """
        return self.COLUMN_LABELS[index]

    def get_header_index(self, value):
        """Return index of 'value' in headers

        Args:
            value (str): header name value

        Returns:
            (int)
        """
        return self._header.index(value)

    def flags(self, index):
        default_flags = super(TexturesTableModel, self).flags(index)
        header_value = self._header[index.column()]

        # Make some columns editable
        if header_value in self.EDITABLE_COLUMNS:
            return default_flags | QtCore.Qt.ItemIsEditable
        return default_flags

    def set_value_in_data(self, column_index, row_index, value):
        if column_index == 1:
            self._data[row_index].in_colorspace = value
        elif column_index == 2:
            self._data[row_index].folder_path = value
        elif column_index == 3:
            self._data[row_index].product_name = value
        elif column_index == 4:
            self._data[row_index].version = int(value)

    def setData(self, index, value, role=QtCore.Qt.EditRole):
        if not index.isValid():
            return False

        if role == QtCore.Qt.EditRole:
            self.set_value_in_data(index.column(), index.row(), value)
            self.dataChanged.emit(index, index)  # Emit data changed signal

            return True

        return False

    def removeRow(self, row, parent=QtCore.QModelIndex()):
        self.beginRemoveRows(parent, row, row)
        success = self._data.pop(row)  # Assuming self._data is the list storing your data
        self.endRemoveRows()
        return success

    def data(self, index, role=QtCore.Qt.DisplayRole):
        """Return data depending on index, Qt::ItemDataRole and data type of the column.

        Args:
            index (QtCore.QModelIndex): Index to define column and row you want to return
            role (Qt::ItemDataRole): Define which data you want to return.

        Returns:
            None if index is invalid
            None if role is none of: DisplayRole, EditRole, CheckStateRole, DATAFRAME_ROLE
        """
        if not index.isValid():
            return

        if index.column() >= len(self.COLUMN_LABELS):
            return

        prod_item = self._data[index.row()]
        header_value = self._header[index.column()]

        if role in (QtCore.Qt.DisplayRole, QtCore.Qt.EditRole):
            return attr.asdict(prod_item)[self._header[index.column()]]

        if role == EDIT_ICON_ROLE:
            if self.can_edit and header_value in self.EDITABLE_COLUMNS:
                return self.edit_icon

        # Change the color if the row has missing data that's required to publish
        if role == QtCore.Qt.ForegroundRole:
            product_dict = attr.asdict(prod_item)
            publishable = all(
                value
                for key, value in product_dict.items()
                if key not in self.UNNECESSARY_COLUMNS
            )
            if not publishable:
                return QtGui.QColor(QtCore.Qt.yellow)
        return None

    def headerData(self, section, orientation, role=QtCore.Qt.DisplayRole):
        if section >= len(self.COLUMN_LABELS):
            return

        if role == QtCore.Qt.DisplayRole:
            if orientation == QtCore.Qt.Horizontal:
                return self.COLUMN_LABELS[section][1]

        elif role == HEADER_NAME_ROLE:
            if orientation == QtCore.Qt.Horizontal:
                return self.COLUMN_LABELS[section][0]  # return name

        elif role == QtCore.Qt.ToolTipRole:
            if orientation == QtCore.Qt.Horizontal:
                return self._tooltips[section]

    def sort(self, column, order):
        self.layoutAboutToBeChanged.emit()

        if column == 0:
            self._data.sort(key=lambda x: x.path)

        # For the columns that could be empty, we need to make sure we
        # sort None type values
        if column == 1:
            self._data.sort(key=lambda x: (x.in_colorspace is not None, x.in_colorspace))

        if column == 2:
            self._data.sort(key=lambda x: (x.folder_path is not None, x.folder_path))

        if column == 3:
            self._data.sort(key=lambda x: (x.product_name is not None, x.product_name))

        if column == 4:
            self._data.sort(key=lambda x: (x.version is not None, x.version))

        if order == QtCore.Qt.DescendingOrder:
            self._data.reverse()

        self.layoutChanged.emit()

    def set_products(self, products):

        self.beginResetModel()

        self._data = []

        for filepath, publish_data in products.items():
            # Ignore everything that's not an image
            extension = os.path.splitext(filepath)[-1]
            if extension not in IMAGE_EXTENSIONS:
                continue

            item = self.TextureRepresentation(
                filepath,
                publish_data.get("folder_path_name", ""),
                "ldev",
                "textures",
                publish_data.get("product_name_name", ""),
                publish_data.get("in_colorspace", ""),
                "tx", # hard-code so we only ingest the tx representation for now
                publish_data.get("version", ""),
            )
            self._data.append(item)

        self.endResetModel()

    def get_products(self):
        return self._data


class ComboBoxDelegate(QtWidgets.QStyledItemDelegate):

    def __init__(self, items, parent=None):
        self.items = items
        super().__init__(parent)

    def createEditor(self, parent, option, index):
        editor = QtWidgets.QComboBox(parent)
        editor.addItems(self.items)
        return editor

    def setEditorData(self, editor, index):
        value = index.model().data(index, QtCore.Qt.EditRole)
        editor.setCurrentText(value)

    def setModelData(self, editor, model, index):
        model.setData(index, editor.currentText(), QtCore.Qt.EditRole)


def main():
    app_instance = QtWidgets.QApplication.instance()
    if app_instance is None:
        app_instance = QtWidgets.QApplication([])

    if platform.system().lower() == "windows":
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("batch_ingester")

    window = TexturePublisher()
    window.show()

    # Trigger on project change every time the tool loads
    window.on_project_change()

    sys.exit(app_instance.exec_())
