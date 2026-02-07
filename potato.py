from __future__ import annotations

"""
Parameter selector dialog (PyQt6) — Table-based

Features:
- Left: QTableView shows a multi-column table of "available parameters".
- Top: Combo box to choose which column acts as the parameter value.
- Top: Filter box (filters across all columns).
- Middle: Controls to add selected rows or all filtered rows to the ordered list.
- Right: QListWidget to hold ordered parameters (drag-and-drop reorder + Move Up/Down).
- Bottom: Display Name input and a collected sets table to manage multiple sets.
- OK returns all sets via result_sets().

You can pass a pandas DataFrame or a list[dict] to the dialog.
"""

from dataclasses import dataclass
from typing import List, Sequence, Mapping, Any, Optional

from PyQt6.QtCore import Qt, QSortFilterProxyModel
from PyQt6.QtGui import QStandardItemModel, QStandardItem
from PyQt6.QtWidgets import (
    QApplication, QDialog, QMainWindow, QWidget, QListWidget, QListWidgetItem,
    QLineEdit, QPushButton, QTableWidget, QTableWidgetItem, QTableView,
    QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QDialogButtonBox, QMessageBox,
    QAbstractItemView, QSizePolicy, QComboBox
)

# Optional pandas import; used only if you pass a DataFrame
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None


# --------------------------------------------------------------------
# Result model
# --------------------------------------------------------------------
@dataclass
class ParameterSet:
    display_name: str
    parameters: List[str]  # ordered


# --------------------------------------------------------------------
# Dialog that uses a multi-column table as the available source
# --------------------------------------------------------------------
class ParameterSetDialog(QDialog):
    """
    Table-based selector dialog:
      - Input: a pandas DataFrame or a list[dict] where keys are column names.
      - User chooses which column acts as the 'parameter value' via a combo box.
      - User selects rows in the table; the chosen column value is added to the ordered list.
      - User can build multiple (Display Name + Ordered Parameters) sets, shown in a table.
      - On OK, call result_sets() to get List[ParameterSet].
    """

    def __init__(
        self,
        table_data: Any,  # pandas.DataFrame | list[dict[str, Any]] | list[list/tuple]
        parent=None,
        allow_duplicates: bool = False,
        title: str = "Build Parameter Sets",
    ):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(1100, 640)

        self.allow_duplicates = allow_duplicates
        self._sets: List[ParameterSet] = []

        # ---- Build the source model (QStandardItemModel) from DataFrame or list of dicts ----
        self.source_model = QStandardItemModel(self)
        columns = self._populate_source_model(table_data)

        # ---- Filter/proxy model for searching across all columns ----
        self.proxy = QSortFilterProxyModel(self)
        self.proxy.setSourceModel(self.source_model)
        self.proxy.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.proxy.setFilterKeyColumn(-1)  # search all columns

        # ---- UI: Filter input + column chooser ----
        self.filter_edit = QLineEdit(self)
        self.filter_edit.setPlaceholderText("Filter (searches all columns)…")

        self.column_picker = QComboBox(self)
        self.column_picker.addItems(columns)
        if columns:
            self.column_picker.setCurrentIndex(0)

        # ---- Left: Table of available parameters (multi-column) ----
        self.available_table = QTableView(self)
        self.available_table.setModel(self.proxy)
        self.available_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.available_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.available_table.setSortingEnabled(True)
        self.available_table.setAlternatingRowColors(True)
        self.available_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        aheader = self.available_table.horizontalHeader()
        if aheader is not None:
            aheader.setStretchLastSection(True)

        # ---- Middle: transfer + ordering controls ----
        self.add_btn = QPushButton("Add ▶")
        self.add_all_filtered_btn = QPushButton("Add all filtered ▶▶")
        self.remove_btn = QPushButton("◀ Remove")
        self.clear_sel_btn = QPushButton("Clear selected list")
        self.up_btn = QPushButton("Move Up")
        self.down_btn = QPushButton("Move Down")

        transfer_col = QVBoxLayout()
        transfer_col.addWidget(self.add_btn)
        transfer_col.addWidget(self.add_all_filtered_btn)
        transfer_col.addWidget(self.remove_btn)
        transfer_col.addWidget(self.clear_sel_btn)
        transfer_col.addSpacing(16)
        transfer_col.addWidget(self.up_btn)
        transfer_col.addWidget(self.down_btn)
        transfer_col.addStretch(1)

        # ---- Right: Selected parameters (ordered) ----
        self.selected_list = QListWidget(self)
        self.selected_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.selected_list.setAlternatingRowColors(True)
        self.selected_list.setDragEnabled(True)
        self.selected_list.setAcceptDrops(True)
        self.selected_list.setDropIndicatorShown(True)
        self.selected_list.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)

        # ---- Display name + actions to collect sets ----
        self.display_label = QLabel("Display name:")
        self.display_edit = QLineEdit(self)
        self.display_edit.setPlaceholderText("Enter display name for this set")

        self.add_set_btn = QPushButton("Add set")
        self.update_set_btn = QPushButton("Update set")
        self.update_set_btn.setEnabled(False)
        self.delete_set_btn = QPushButton("Delete set")
        self.clear_sets_btn = QPushButton("Clear all sets")

        # ---- Table of collected sets ----
        self.sets_table = QTableWidget(0, 2, self)
        self.sets_table.setHorizontalHeaderLabels(["Display name", "Parameters (in order)"])
        self.sets_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.sets_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        vheader = self.sets_table.verticalHeader()
        if vheader is not None:
            vheader.setVisible(False)
        header = self.sets_table.horizontalHeader()
        if header is not None:
            header.setStretchLastSection(True)

        # ---- OK/Cancel ----
        self.btn_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self
        )

        # ---- Layout ----
        meta_row = QHBoxLayout()
        meta_row.addWidget(QLabel("Parameter column:"))
        meta_row.addWidget(self.column_picker, stretch=0)
        meta_row.addSpacing(16)
        meta_row.addWidget(QLabel("Filter:"))
        meta_row.addWidget(self.filter_edit, stretch=1)

        top_grid = QGridLayout()
        top_grid.addWidget(QLabel("Available (table)"), 0, 0)
        top_grid.addWidget(QLabel("Selected parameters (ordered)"), 0, 2)

        left_col = QVBoxLayout()
        left_col.addWidget(self.available_table)

        right_col = QVBoxLayout()
        right_col.addWidget(self.selected_list)

        top_grid.addLayout(left_col, 1, 0)
        top_grid.addLayout(transfer_col, 1, 1)
        top_grid.addLayout(right_col, 1, 2)

        dn_row = QHBoxLayout()
        dn_row.addWidget(self.display_label)
        dn_row.addWidget(self.display_edit)
        dn_row.addSpacing(12)
        dn_row.addWidget(self.add_set_btn)
        dn_row.addWidget(self.update_set_btn)
        dn_row.addWidget(self.delete_set_btn)
        dn_row.addWidget(self.clear_sets_btn)
        dn_row.addStretch(1)

        main = QVBoxLayout(self)
        main.addLayout(meta_row)
        main.addSpacing(6)
        main.addLayout(top_grid)
        main.addSpacing(10)
        main.addLayout(dn_row)
        main.addWidget(self.sets_table, stretch=1)
        main.addWidget(self.btn_box)

        # ---- Wiring ----
        self.filter_edit.textChanged.connect(self._on_filter_changed)
        self.add_btn.clicked.connect(self._on_add_selected_rows)
        self.add_all_filtered_btn.clicked.connect(self._on_add_all_filtered)
        self.remove_btn.clicked.connect(self._remove_selected_from_selected)
        self.clear_sel_btn.clicked.connect(self.selected_list.clear)
        self.up_btn.clicked.connect(lambda: self._move_selected(-1))
        self.down_btn.clicked.connect(lambda: self._move_selected(+1))

        self.add_set_btn.clicked.connect(self._on_add_set)
        self.update_set_btn.clicked.connect(self._on_update_set)
        self.delete_set_btn.clicked.connect(self._on_delete_set)
        self.clear_sets_btn.clicked.connect(self._on_clear_sets)

        self.sets_table.itemSelectionChanged.connect(self._on_table_selection_changed)

        self.btn_box.accepted.connect(self._on_accept)
        self.btn_box.rejected.connect(self.reject)

        # Sizing policies
        self.available_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.selected_list.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.sets_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    # ----------------- Model building -----------------
    def _populate_source_model(self, table_data: Any) -> List[str]:
        """
        Populate the QStandardItemModel from a DataFrame or list[dict] (or lists).
        Returns list of column names.
        """
        columns: List[str] = []
        rows_data: List[Sequence[Any]] = []

        # Pandas DataFrame
        if pd is not None and isinstance(table_data, pd.DataFrame):
            columns = [str(c) for c in table_data.columns.tolist()]
            for _, row in table_data.iterrows():
                rows_data.append([row[c] for c in table_data.columns])

        # List[dict]
        elif isinstance(table_data, list) and (len(table_data) == 0 or isinstance(table_data[0], Mapping)):
            # Collect union of keys across rows to preserve all columns
            keys: List[str] = []
            seen = set()
            for item in table_data:
                if not isinstance(item, Mapping):
                    continue
                for k in item.keys():
                    if k not in seen:
                        seen.add(k)
                        keys.append(str(k))
            columns = keys
            for item in table_data:
                row_vals = [item.get(k, "") if isinstance(item, Mapping) else "" for k in columns]
                rows_data.append(row_vals)

        else:
            # Fallbacks: list of lists/tuples or flat list
            if isinstance(table_data, list) and table_data:
                first = table_data[0]
                if isinstance(first, (list, tuple)):
                    col_count = len(first)
                    columns = [f"col_{i}" for i in range(col_count)]
                    rows_data = table_data  # type: ignore[assignment]
                else:
                    columns = ["value"]
                    rows_data = [[str(x)] for x in table_data]
            else:
                columns = ["value"]
                rows_data = []

        self.source_model.clear()
        self.source_model.setColumnCount(len(columns))
        self.source_model.setHorizontalHeaderLabels(columns)
        for row in rows_data:
            items = [QStandardItem("" if v is None else str(v)) for v in row]
            for it in items:
                it.setEditable(False)
            self.source_model.appendRow(items)

        return columns

    # ----------------- Filtering -----------------
    def _on_filter_changed(self, text: str) -> None:
        self.proxy.setFilterFixedString(text)

    # ----------------- Transfer logic -----------------
    def _current_param_column(self) -> Optional[int]:
        col_name = self.column_picker.currentText().strip()
        if not col_name:
            return None
        for ci in range(self.source_model.columnCount()):
            hdr = self.source_model.headerData(ci, Qt.Orientation.Horizontal)
            if str(hdr) == col_name:
                return ci
        return None

    def _selected_source_rows(self) -> List[int]:
        """Returns selected *source* row indices (map from proxy -> source)."""
        rows: List[int] = []
        sel = self.available_table.selectionModel()
        if sel is None:
            return rows
        for proxy_index in sel.selectedRows():
            if not proxy_index.isValid():
                continue
            src_index = self.proxy.mapToSource(proxy_index)
            if src_index.isValid():
                rows.append(src_index.row())
        return rows

    def _get_source_value(self, row: int, col: int) -> Optional[str]:
        idx = self.source_model.index(row, col)
        if not idx.isValid():
            return None
        val = self.source_model.data(idx)
        return None if val is None else str(val)

    def _add_values_to_selected(self, values: List[str]) -> None:
        # Build existing set to enforce uniqueness if required
        existing: set[str] = set()
        for i in range(self.selected_list.count()):
            it = self.selected_list.item(i)
            if it is None:
                continue
            existing.add(it.text())

        for v in values:
            if self.allow_duplicates or v not in existing:
                QListWidgetItem(v, self.selected_list)

    def _on_add_selected_rows(self) -> None:
        param_col = self._current_param_column()
        if param_col is None:
            QMessageBox.warning(self, "No parameter column", "Please choose the parameter column.")
            return
        rows = self._selected_source_rows()
        if not rows:
            QMessageBox.information(self, "No selection", "Select one or more rows in the table.")
            return
        vals: List[str] = []
        for r in rows:
            v = self._get_source_value(r, param_col)
            if v is not None:
                vals.append(v)
        if vals:
            self._add_values_to_selected(vals)

    def _on_add_all_filtered(self) -> None:
        param_col = self._current_param_column()
        if param_col is None:
            QMessageBox.warning(self, "No parameter column", "Please choose the parameter column.")
            return
        vals: List[str] = []
        # Iterate over filtered/proxy rows and map to source
        for proxy_row in range(self.proxy.rowCount()):
            src_index = self.proxy.mapToSource(self.proxy.index(proxy_row, 0))
            if not src_index.isValid():
                continue
            v = self._get_source_value(src_index.row(), param_col)
            if v is not None:
                vals.append(v)
        if vals:
            self._add_values_to_selected(vals)

    def _remove_selected_from_selected(self) -> None:
        for item in self.selected_list.selectedItems():
            if item is None:
                continue
            row = self.selected_list.row(item)
            self.selected_list.takeItem(row)

    def _move_selected(self, delta: int) -> None:
        sel = self.selected_list.selectedItems()
        if not sel:
            return
        item = sel[0]
        if item is None:
            return
        row = self.selected_list.row(item)
        new_row = max(0, min(self.selected_list.count() - 1, row + delta))
        if new_row == row:
            return
        taken = self.selected_list.takeItem(row)
        if taken is None:
            return
        self.selected_list.insertItem(new_row, taken)
        self.selected_list.setCurrentRow(new_row)

    # ----------------- Collect/sets management -----------------
    def _collect_current_selection(self) -> Optional[ParameterSet]:
        display_name = self.display_edit.text().strip()
        if not display_name:
            QMessageBox.warning(self, "Missing display name", "Please enter a display name.")
            return None
        params: List[str] = []
        for i in range(self.selected_list.count()):
            it = self.selected_list.item(i)
            if it is None:
                continue
            params.append(it.text())
        if not params:
            QMessageBox.warning(self, "No parameters selected", "Please select one or more parameters.")
            return None
        return ParameterSet(display_name=display_name, parameters=params)

    def _on_add_set(self) -> None:
        ps = self._collect_current_selection()
        if ps is None:
            return
        # Replace by display_name if exists
        for idx, s in enumerate(self._sets):
            if s.display_name == ps.display_name:
                r = QMessageBox.question(
                    self, "Duplicate name",
                    f"A set named '{ps.display_name}' already exists.\nReplace it?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                if r != QMessageBox.StandardButton.Yes:
                    return
                self._sets[idx] = ps
                self._refresh_table()
                self._clear_current_builder()
                return
        self._sets.append(ps)
        self._refresh_table()
        self._clear_current_builder()

    def _on_update_set(self) -> None:
        row = self._current_table_row()
        if row is None:
            return
        ps = self._collect_current_selection()
        if ps is None:
            return
        self._sets[row] = ps
        self._refresh_table()
        self._clear_current_builder()

    def _on_delete_set(self) -> None:
        row = self._current_table_row()
        if row is None:
            return
        del self._sets[row]
        self._refresh_table()
        self._clear_current_builder()

    def _on_clear_sets(self) -> None:
        if not self._sets:
            return
        r = QMessageBox.question(self, "Clear all sets", "Remove all sets?",
                                 QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if r == QMessageBox.StandardButton.Yes:
            self._sets.clear()
            self._refresh_table()

    def _current_table_row(self) -> Optional[int]:
        sm = self.sets_table.selectionModel()
        if sm is None:
            return None
        rows = sm.selectedRows()
        return rows[0].row() if rows else None

    def _on_table_selection_changed(self) -> None:
        row = self._current_table_row()
        self.update_set_btn.setEnabled(row is not None)
        if row is None:
            return
        ps = self._sets[row]
        self.display_edit.setText(ps.display_name)
        self.selected_list.clear()
        for p in ps.parameters:
            QListWidgetItem(p, self.selected_list)

    def _refresh_table(self) -> None:
        self.sets_table.setRowCount(0)
        for s in self._sets:
            r = self.sets_table.rowCount()
            self.sets_table.insertRow(r)
            self.sets_table.setItem(r, 0, QTableWidgetItem(s.display_name))
            self.sets_table.setItem(r, 1, QTableWidgetItem(", ".join(s.parameters)))

    def _clear_current_builder(self) -> None:
        self.display_edit.clear()
        self.selected_list.clear()
        self.available_table.clearSelection()

    def _on_accept(self) -> None:
        if not self._sets:
            QMessageBox.warning(self, "No sets", "Please add at least one set before clicking OK.")
            return
        self.accept()

    # ----------------- Public API -----------------
    def result_sets(self) -> List[ParameterSet]:
        """Return the collected (Display Name + Ordered Parameters) sets."""
        return list(self._sets)


# --------------------------------------------------------------------
# Example: Overview window + __main__ for local testing
# --------------------------------------------------------------------
class OverviewWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Overview (Table-based selector)")
        self.resize(720, 300)

        # Example data: list of dicts (replace with your real table)
        self.available_table_data = [
            {"PARAM_ID": "PRODUCTIONDATE", "Category": "Meta", "Desc": "Production Date"},
            {"PARAM_ID": "BARCODE", "Category": "Identity", "Desc": "Barcode text"},
            {"PARAM_ID": "PARTNUMBER", "Category": "Identity", "Desc": "Part Number"},
            {"PARAM_ID": "DIAGNOSTIC_IDENTIFIER", "Category": "Diag", "Desc": "Diagnostic ID"},
            {"PARAM_ID": "ECU_SW_VERSION", "Category": "SW", "Desc": "ECU Software Version"},
            {"PARAM_ID": "ECU_HW_VERSION", "Category": "HW", "Desc": "ECU Hardware Version"},
        ]
        # If you have a pandas DataFrame `df`, you can pass that instead:
        # dlg = ParameterSetDialog(df, ...)

        central = QWidget(self)
        self.setCentralWidget(central)
        lay = QVBoxLayout(central)
        lay.addWidget(QLabel("Click the button to define (Display Name + Ordered Parameters) using a table source."))

        btn = QPushButton("Configure parameter sets…", self)
        btn.clicked.connect(self.on_configure_clicked)
        lay.addWidget(btn)
        lay.addStretch(1)

    def on_configure_clicked(self):
        dlg = ParameterSetDialog(self.available_table_data, parent=self, allow_duplicates=False)
        # Optional: Pre-select parameter column (e.g., "PARAM_ID")
        idx = dlg.column_picker.findText("PARAM_ID")
        if idx >= 0:
            dlg.column_picker.setCurrentIndex(idx)

        if dlg.exec() == QDialog.DialogCode.Accepted:
            sets = dlg.result_sets()
            self.generate_function_from_sets(sets)

    def generate_function_from_sets(self, sets: List[ParameterSet]) -> None:
        print("Received parameter sets:")
        for s in sets:
            print(f"  - {s.display_name}: {s.parameters}")
        # TODO: integrate with your real function, e.g. my_function(sets)


if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)
    w = OverviewWindow()
    w.show()
    sys.exit(app.exec())
