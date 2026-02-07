from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import List, Sequence, Mapping, Any

from PyQt6.QtCore import Qt, QSize, QSortFilterProxyModel
from PyQt6.QtGui import QAction, QStandardItemModel, QStandardItem
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QListWidget,
    QListWidgetItem, QTableView, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QSplitter,
    QAbstractItemView, QComboBox, QToolBar,
    QTableWidget, QTableWidgetItem, QMessageBox,
    QStatusBar
)


# --------------------------------------------------
# Data Model
# --------------------------------------------------

@dataclass(slots=True)
class ParameterSet:
    display_name: str
    parameters: List[str]


# --------------------------------------------------
# Workspace
# --------------------------------------------------

class ParameterWorkspace(QMainWindow):

    def __init__(self, table_data: Any) -> None:
        super().__init__()

        self.setWindowTitle("Parameter Configuration Workspace")
        self.resize(1400, 800)

        self._sets: List[ParameterSet] = []

        # 🚨 Duplicate protection structure
        self._selected_params: set[str] = set()

        # Explicit status bar (prevents Optional typing warnings)
        self._status_bar: QStatusBar = QStatusBar(self)
        self.setStatusBar(self._status_bar)

        self._build_models(table_data)
        self._build_ui()
        self._build_toolbar()

        self._status_bar.showMessage("Ready")

    # --------------------------------------------------
    # Models
    # --------------------------------------------------

    def _build_models(self, table_data: Any) -> None:

        self.source_model: QStandardItemModel = QStandardItemModel(self)

        columns: List[str] = []
        rows: List[Sequence[Any]] = []

        if isinstance(table_data, list) and (
            not table_data or isinstance(table_data[0], Mapping)
        ):
            seen: set[str] = set()

            for row in table_data:
                for k in row:
                    if k not in seen:
                        seen.add(k)
                        columns.append(str(k))

            for row in table_data:
                rows.append([row.get(c, "") for c in columns])

        else:
            columns = ["value"]
            rows = [[str(v)] for v in table_data]

        self.source_model.setColumnCount(len(columns))
        self.source_model.setHorizontalHeaderLabels(columns)

        for row in rows:
            items: List[QStandardItem] = [
                QStandardItem(str(v)) for v in row
            ]

            for item in items:
                item.setEditable(False)

            self.source_model.appendRow(items)

        self.proxy: QSortFilterProxyModel = QSortFilterProxyModel(self)
        self.proxy.setSourceModel(self.source_model)
        self.proxy.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.proxy.setFilterKeyColumn(-1)

    # --------------------------------------------------
    # UI
    # --------------------------------------------------

    def _build_ui(self) -> None:

        central: QWidget = QWidget(self)
        self.setCentralWidget(central)

        main_layout: QVBoxLayout = QVBoxLayout(central)

        # ---------- Controls ----------
        control_row: QHBoxLayout = QHBoxLayout()

        self.column_picker: QComboBox = QComboBox(self)
        self.column_picker.addItems(
            [
                str(self.source_model.headerData(i, Qt.Orientation.Horizontal))
                for i in range(self.source_model.columnCount())
            ]
        )

        self.search: QLineEdit = QLineEdit(self)
        self.search.setPlaceholderText("Search parameters...")
        self.search.textChanged.connect(self.proxy.setFilterFixedString)

        control_row.addWidget(QLabel("Parameter column:"))
        control_row.addWidget(self.column_picker)
        control_row.addSpacing(20)
        control_row.addWidget(self.search)

        main_layout.addLayout(control_row)

        # ---------- Split workspace ----------
        splitter: QSplitter = QSplitter(Qt.Orientation.Horizontal)

        splitter.addWidget(self._build_library())
        splitter.addWidget(self._build_builder())
        splitter.addWidget(self._build_sets())

        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 2)
        splitter.setStretchFactor(2, 3)

        main_layout.addWidget(splitter)

    # --------------------------------------------------
    # Panels
    # --------------------------------------------------

    def _build_library(self) -> QWidget:

        panel: QWidget = QWidget(self)
        layout: QVBoxLayout = QVBoxLayout(panel)

        layout.addWidget(QLabel("Parameter Library"))

        self.table: QTableView = QTableView(panel)
        self.table.setModel(self.proxy)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        vheader = self.table.verticalHeader()
        if vheader is not None:
            vheader.setDefaultSectionSize(26)

        self.table.doubleClicked.connect(self.add_selected)

        layout.addWidget(self.table)

        return panel

    def _build_builder(self) -> QWidget:

        panel: QWidget = QWidget(self)
        layout: QVBoxLayout = QVBoxLayout(panel)

        layout.addWidget(QLabel("Selected Parameters (Drag to reorder)"))

        self.selected: QListWidget = QListWidget(panel)
        self.selected.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)

        layout.addWidget(self.selected)

        btn_row: QHBoxLayout = QHBoxLayout()

        add_btn: QPushButton = QPushButton("Add", panel)
        remove_btn: QPushButton = QPushButton("Remove", panel)

        add_btn.clicked.connect(self.add_selected)
        remove_btn.clicked.connect(self.remove_selected)

        btn_row.addWidget(add_btn)
        btn_row.addWidget(remove_btn)

        layout.addLayout(btn_row)

        layout.addWidget(QLabel("Display Name"))

        self.display: QLineEdit = QLineEdit(panel)
        layout.addWidget(self.display)

        create_btn: QPushButton = QPushButton("Create Set", panel)
        create_btn.clicked.connect(self.create_set)

        layout.addWidget(create_btn)

        return panel

    def _build_sets(self) -> QWidget:

        panel: QWidget = QWidget(self)
        layout: QVBoxLayout = QVBoxLayout(panel)

        layout.addWidget(QLabel("Saved Parameter Sets"))

        self.set_table: QTableWidget = QTableWidget(0, 2, panel)
        self.set_table.setHorizontalHeaderLabels(["Display", "Parameters"])

        layout.addWidget(self.set_table)

        return panel

    # --------------------------------------------------
    # Toolbar
    # --------------------------------------------------

    def _build_toolbar(self) -> None:

        tb: QToolBar = QToolBar("Main", self)
        tb.setIconSize(QSize(16, 16))
        self.addToolBar(tb)

        clear_action: QAction = QAction("Clear Builder", self)
        clear_action.triggered.connect(self.clear_builder)

        tb.addAction(clear_action)

    # --------------------------------------------------
    # Logic
    # --------------------------------------------------

    def _current_column(self) -> int:
        return self.column_picker.currentIndex()

    def add_selected(self) -> None:

        selection = self.table.selectionModel()
        if selection is None:
            return

        column: int = self._current_column()

        for proxy_index in selection.selectedRows():

            src = self.proxy.mapToSource(proxy_index)

            value = self.source_model.data(
                self.source_model.index(src.row(), column)
            )

            if value is None:
                continue

            param = str(value)

            # 🚨 Duplicate guard
            if param in self._selected_params:
                self._status_bar.showMessage(
                    f"{param} already added",
                    1500
                )
                continue

            QListWidgetItem(param, self.selected)
            self._selected_params.add(param)

    def remove_selected(self) -> None:

        for item in self.selected.selectedItems():

            param = item.text()
            self._selected_params.discard(param)

            self.selected.takeItem(self.selected.row(item))

    def clear_builder(self) -> None:

        self.selected.clear()
        self.display.clear()

        # VERY important reset
        self._selected_params.clear()

        self._status_bar.showMessage("Builder cleared", 2000)

    def create_set(self) -> None:

        name: str = self.display.text().strip()

        if not name:
            QMessageBox.warning(self, "Missing name", "Enter a display name.")
            return

        params: List[str] = []

        for i in range(self.selected.count()):
            item = self.selected.item(i)
            if item is not None:
                params.append(item.text())

        if not params:
            QMessageBox.warning(self, "No parameters", "Add parameters first.")
            return

        ps = ParameterSet(name, params)
        self._sets.append(ps)

        row = self.set_table.rowCount()
        self.set_table.insertRow(row)

        self.set_table.setItem(row, 0, QTableWidgetItem(name))
        self.set_table.setItem(row, 1, QTableWidgetItem(", ".join(params)))

        self.clear_builder()

        self._status_bar.showMessage("Set created", 2000)


# --------------------------------------------------
# Run
# --------------------------------------------------

if __name__ == "__main__":

    app = QApplication(sys.argv)

    demo_data = [
        {"PARAM_ID": "VIN", "Category": "Vehicle"},
        {"PARAM_ID": "PART_NO", "Category": "Vehicle"},
        {"PARAM_ID": "SW_VERSION", "Category": "Software"},
        {"PARAM_ID": "HW_VERSION", "Category": "Hardware"},
    ]

    window = ParameterWorkspace(demo_data)
    window.show()

    sys.exit(app.exec())
