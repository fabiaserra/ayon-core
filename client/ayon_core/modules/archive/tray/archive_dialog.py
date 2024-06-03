import sys
import re
import platform
import pandas as pd

import qtawesome
from qtpy import QtCore, QtWidgets, QtGui
from datetime import datetime

import ayon_api

from ayon_core import style
from ayon_core import resources
from ayon_core.lib import Logger
from ayon_core.pipeline import AvalonMongoDB
from ayon_core.tools.utils import lib as tools_lib
from ayon_core.modules.archive.lib import expunge, utils
from ayon_core.tools.utils.constants import (
    HEADER_NAME_ROLE,
)

from ayon_shotgrid.lib import credentials


logger = Logger.get_logger(__name__)


class ArchiveDialog(QtWidgets.QDialog):
    """Interface to control the archive pipeline"""

    tool_title = "Archive Paths"
    tool_name = "archive_status"

    SIZE_W = 1800
    SIZE_H = 800

    DEFAULT_WIDTHS = (
        ("path", 700),
        ("delete_time", 100),
        ("marked_time", 100),
        ("size", 80),
        ("is_deleted", 50),
        ("publish_dir", 150),
        ("publish_id", 50),
        ("reason", 150),
        ("paths", 200),
    )

    def __init__(self, parent=None):
        super(ArchiveDialog, self).__init__(parent)

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

        self.sg = credentials.get_shotgrid_session()

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

        # Filter line edit to filter by regex
        text_filter = QtWidgets.QLineEdit()
        text_filter.setPlaceholderText("Type to filter...")
        text_filter.textChanged.connect(self.on_filter_text_changed)
        input_layout.addRow("Filter", text_filter)

        filters_widget = QtWidgets.QWidget()
        horizontal_layout = QtWidgets.QHBoxLayout()
        filters_widget.setLayout(horizontal_layout)

        # Checkbox to choose whether to show deleted files or not
        show_deleted = QtWidgets.QCheckBox("Only deleted")
        show_deleted.setChecked(False)
        show_deleted.setToolTip(
            "Whether we want to show the already deleted paths or not."
        )
        show_deleted.stateChanged.connect(self.on_filter_deleted_changed)
        horizontal_layout.addWidget(show_deleted)

        # Checkbox to choose whether to show temp files or not
        show_temp_files = QtWidgets.QCheckBox("Only temp")
        show_temp_files.setChecked(False)
        show_temp_files.setToolTip(
            "Whether we want to show temp files like scene backups, temp transcodes or autosaves."
        )
        show_temp_files.stateChanged.connect(self.on_filter_show_temp_files)
        horizontal_layout.addWidget(show_temp_files)

        # Checkbox to choose whether to show IO files or not
        show_io_files = QtWidgets.QCheckBox("Only IO")
        show_io_files.setChecked(False)
        show_io_files.setToolTip(
            "Whether we want to show io files like scene backups or autosaves."
        )
        show_io_files.stateChanged.connect(self.on_filter_show_io_files)
        horizontal_layout.addWidget(show_io_files)

        # Add stretch so filter toggles are aligned to the left
        horizontal_layout.addStretch()

        input_layout.addRow("Show", filters_widget)

        main_layout.addWidget(input_widget)

        # Table with all the products we find in the given folder
        table_view = QtWidgets.QTableView()
        model = ArchivePathsTableModel(parent=self)
        proxy_model = FilterProxyModel()
        proxy_model.setSourceModel(model)
        table_view.setModel(proxy_model)
        table_view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)

        table_view.horizontalHeader().setSortIndicator(-1, QtCore.Qt.AscendingOrder)
        table_view.setAlternatingRowColors(True)
        table_view.verticalHeader().hide()
        table_view.viewport().setAttribute(QtCore.Qt.WA_Hover, True)

        table_view.setSortingEnabled(True)
        table_view.setTextElideMode(QtCore.Qt.ElideLeft)

        header = table_view.horizontalHeader()
        for column_name, width in self.DEFAULT_WIDTHS:
            idx = model.get_column_index(column_name)
            header.setSectionResizeMode(idx, QtWidgets.QHeaderView.Interactive)
            table_view.setColumnWidth(idx, width)

        header.setStretchLastSection(True)

        main_layout.addWidget(table_view)

        # Assign widgets we want to reuse to class instance
        self._projects_combobox = projects_combobox
        self._table_view = table_view
        self._model = model
        self._proxy_model = proxy_model

    def keyPressEvent(self, event: QtGui.QKeyEvent):
        # Ignore enter key
        if event.key() == QtCore.Qt.Key_Enter or event.key() == QtCore.Qt.Key_Return:
            event.ignore()
        else:
            super().keyPressEvent(event)

    def showEvent(self, event):
        super(ArchiveDialog, self).showEvent(event)
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
            if is_active and not is_library:
                projects.append(project["name"])

        return projects

    def on_project_change(self):
        if self._ignore_project_change:
            return

        row = self._projects_combobox.currentIndex()
        index = self._projects_combobox.model().index(row, 0)
        project_name = index.data(QtCore.Qt.UserRole + 1)

        sg_project = self.sg.find_one(
            "Project",
            [["name", "is", project_name]],
            fields=["sg_code"]
        )
        if not sg_project:
            return

        project_name = self.dbcon.active_project() or "No project selected"
        title = "{} - {}".format(self.tool_title, project_name)
        self.setWindowTitle(title)

        # Store project name and code as class variable so we can reuse it throughout
        self._current_proj_name = project_name
        proj_code = sg_project.get("sg_code")
        self._current_proj_code = proj_code

        archive_proj = expunge.ArchiveProject(proj_code)
        archive_data = archive_proj.get_archive_data()
        self._model.set_archive_data(archive_data)

    def on_filter_text_changed(self, text):
        self._proxy_model.setFilterRegExp(text)

    def on_filter_deleted_changed(self, state):
        self._proxy_model.set_show_only_deleted(state == QtCore.Qt.Checked)

    def on_filter_show_temp_files(self, state):
        self._proxy_model.set_show_only_temp_files(state == QtCore.Qt.Checked)

    def on_filter_show_io_files(self, state):
        self._proxy_model.set_show_only_io_files(state == QtCore.Qt.Checked)

    # -------------------------------
    # Delay calling blocking methods
    # -------------------------------

    def refresh(self):
        tools_lib.schedule(self._refresh, 50, channel="mongo")


class FilterProxyModel(QtCore.QSortFilterProxyModel):

    TEMP_FILE_PATTERNS = expunge.TEMP_FILE_PATTERNS.copy()
    # Add some extra files that we want to consider as temporary
    TEMP_FILE_PATTERNS.add(
        re.compile(".*/temp_transcode/.*")
    )

    def __init__(self, parent=None):
        super(FilterProxyModel, self).__init__(parent)
        # 0 is the path index
        # 5 the publish dir index
        # 7 the reason index
        self._path_idx = 0
        self._filter_columns = [self._path_idx, 5, 7]
        self._deleted_idx = 4
        self._show_only_deleted = False
        self._show_only_temp_files = False
        self._show_only_io_files = False

    def set_show_only_deleted(self, state):
        self._show_only_deleted = state
        self.invalidateFilter()

    def set_show_only_temp_files(self, state):
        self._show_only_temp_files = state
        self.invalidateFilter()

    def set_show_only_io_files(self, state):
        self._show_only_io_files = state
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent):
        """Override to filter rows based on the text in the specified column."""

        # First, check the regular expression filter
        if self.filterRegExp():
            regex_matches = False
            for column_idx in self._filter_columns:
                index = self.sourceModel().index(source_row, column_idx, source_parent)
                try:
                    if self.filterRegExp().indexIn(self.sourceModel().data(index)) >= 0:
                        regex_matches = True
                except TypeError:
                    continue

            if not regex_matches:
                return False

        # Then, check the toggle filters

        row_accepted = True

        # Hide temp files, unless we are only showing them
        path_index = self.sourceModel().index(
            source_row, self._path_idx, source_parent
        )
        filepath = self.sourceModel().data(path_index)

        # Check if the file path matches any of the patterns
        is_temp_file = False
        for pattern in self.TEMP_FILE_PATTERNS:
            if pattern.match(filepath):
                is_temp_file = True
                break

        if self._show_only_temp_files:
            return is_temp_file
        else:
            row_accepted &= not is_temp_file

        is_io_file = "/io/" in filepath
        if self._show_only_io_files:
            return is_io_file
        else:
            row_accepted &= not is_io_file

        # Hide deleted rows, unless we are only showing deleted
        deleted_index = self.sourceModel().index(
            source_row, self._deleted_idx, source_parent
        )
        is_deleted_data = self.sourceModel().data(deleted_index)

        # Convert to boolean if not inherently boolean
        is_deleted = (is_deleted_data == 'True') if isinstance(
            is_deleted_data, str
        ) else bool(is_deleted_data)

        if self._show_only_deleted:
            return is_deleted
        else:
            row_accepted &= not is_deleted

        # If none of the above conditions block the row, accept it
        return row_accepted


class ArchivePathsTableModel(QtCore.QAbstractTableModel):
    """Model for the archive paths table"""

    _column_data = {
        "path": ("Path", "Archived path"),
        "delete_time": ("Delete Time", "Time when the path will be deleted"),
        "marked_time": ("Marked Time", "Time when the path was marked for ready to be deleted"),
        "size": ("Size", "Size of path"),
        "is_deleted": ("Deleted", "Whether the path has been deleted already or not"),
        "publish_dir": ("Publish Path", "Path where the file was published to"),
        "publish_id": ("id", "Publish version entity ID"),
        "reason": ("Reason", "Reason why the path was marked for deletion"),
        "paths": ("Full paths", "All the filepaths included under the entry")
    }

    DATE_FORMAT = "%Y-%m-%d"

    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self._data = pd.DataFrame({
            "path": [],
            "delete_time": [],
            "marked_time": [],
            "size": [],
            "is_deleted": [],
            "publish_dir": [],
            "publish_id": [],
            "reason": [],
            "paths": [],
        })
        self.edit_icon = qtawesome.icon("fa.edit", color="white")

    def rowCount(self, parent=None):
        return len(self._data)

    def columnCount(self, parent=None):
        return len(self._data.columns)

    def get_column_index(self, column_name):
        """Return index of column

        Args:
            column_name (str): Name of column

        Returns:
            int: Index of column in data
        """
        return self._data.columns.get_loc(column_name)

    def get_color_by_time(self, target_datetime, hours_before_turning_red=168):
        """Return a QColor object ranging from yellow to red based on time proximity.

        This function calculates the difference in time between the current moment and
        a target datetime. It returns a QColor object that gradually changes from yellow
        to red as the current time approaches the target datetime. The color is green
        at or past the target time, and fully yellow if the current time is more than
        the specified hours away from the target time.

        Args:
            target_datetime (datetime.datetime): The target datetime to compare against.
            hours_before_turning_red (int): The number of hours before the target_datetime
                                            when the color starts changing from yellow to red.

        Returns:
            QtGui.QColor: The color corresponding to the time difference, ranging from yellow to red.

        """
        current_time = datetime.now()
        time_diff = (target_datetime - current_time).total_seconds() / 3600  # convert difference to hours

        if time_diff <= 0:
            # Current time is at or past the target time
            return QtGui.QColor(15)  # Light gray
        elif time_diff > hours_before_turning_red:
            # Current time is more than specified hours away from target time
            return QtGui.QColor(255, 255, 0)  # Yellow

        # Calculate color based on linear interpolation
        red_value = 255
        green_value = utils.interp(time_diff, 0, 0, hours_before_turning_red, 255)
        blue_value = 0  # Constant, as we're moving between red and yellow (no blue component)

        return QtGui.QColor(red_value, green_value, blue_value)

    def data(self, index, role=QtCore.Qt.DisplayRole):
        """Return data depending on index, Qt::ItemDataRole and data type of the column.

        Args:
            index (QtCore.QModelIndex): Index to define column and row you want to return
            role (Qt::ItemDataRole): Define which data you want to return.

        Returns:
            None if index is invalid
        """
        if not index.isValid():
            return

        column_name = self._data.columns[index.column()]

        if role == QtCore.Qt.DisplayRole:
            value = self._data.iat[index.row(), index.column()]
            if column_name.endswith("_time"):
                return pd.to_datetime(value).strftime(self.DATE_FORMAT)
            elif column_name == "size":
                return utils.format_bytes(value)
            elif column_name == "is_deleted":
                return str(bool(value))
            else:
                return value

        if role == QtCore.Qt.ForegroundRole:
            if column_name == "delete_time":
                value = self._data.iat[index.row(), index.column()]
                return self.get_color_by_time(pd.to_datetime(value))

        return None

    def headerData(self, section, orientation, role=QtCore.Qt.DisplayRole):
        if section >= self.columnCount():
            return

        if role == QtCore.Qt.DisplayRole:
            if orientation == QtCore.Qt.Horizontal:
                return self._column_data[self._data.columns[section]][0]

        elif role == HEADER_NAME_ROLE:
            if orientation == QtCore.Qt.Horizontal:
                return self._data.columns[section]

        elif role == QtCore.Qt.ToolTipRole:
            if orientation == QtCore.Qt.Horizontal:
                return self._column_data[self._data.columns[section]][1]

    def sort(self, column, order):
        self.layoutAboutToBeChanged.emit()

        column_name = self._data.columns[column]

        if order == QtCore.Qt.DescendingOrder:
            self._data.sort_values(column_name, ascending=False, inplace=True)
        else:
            self._data.sort_values(column_name, inplace=True)

        self.layoutChanged.emit()

    def set_archive_data(self, archive_data):
        self.beginResetModel()
        self._data = archive_data
        self.endResetModel()


def main():
    app_instance = QtWidgets.QApplication.instance()
    if app_instance is None:
        app_instance = QtWidgets.QApplication([])

    if platform.system().lower() == "windows":
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("archive_status")

    window = ArchiveDialog()
    window.show()

    sys.exit(app_instance.exec_())
