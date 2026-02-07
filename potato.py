from __future__ import annotations

from dataclasses import dataclass
from typing import List

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QDialog, QListWidget, QListWidgetItem, QLineEdit, QPushButton, QTableWidget,
    QTableWidgetItem, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel,
    QDialogButtonBox, QMessageBox, QAbstractItemView, QSizePolicy
)

@dataclass
class ParameterSet:
    display_name: str
    parameters: List[str]  # ordered


class ParameterSetDialog(QDialog):
    """
    Dialog that:
      - Receives a list of parameter names (strings).
      - Lets the user select one/more parameters and reorder them.
      - Lets the user type a Display Name.
      - Allows creating multiple such (Display Name + Ordered Parameters) sets.
      - Returns all sets on OK via result_sets().
    """
    def __init__(self, parameter_list: List[str], parent=None, allow_duplicates: bool = False):
        super().__init__(parent)
        self.setWindowTitle("Build Parameter Sets")
        self.resize(900, 560)

        self.parameter_list = list(parameter_list)
        self.allow_duplicates = allow_duplicates
        self._sets: List[ParameterSet] = []

        # ---------- Left: Available parameters + filter ----------
        self.filter_edit = QLineEdit(self)
        self.filter_edit.setPlaceholderText("Filter parameters…")

        self.available_list = QListWidget(self)
        self.available_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.available_list.setAlternatingRowColors(True)
        self.available_list.setSortingEnabled(False)
        for p in self.parameter_list:
            QListWidgetItem(p, self.available_list)

        # ---------- Middle: transfer + ordering controls ----------
        self.add_btn = QPushButton(">")
        self.add_all_btn = QPushButton(">>")
        self.remove_btn = QPushButton("<")
        self.clear_sel_btn = QPushButton("Clear")
        self.up_btn = QPushButton("Up")
        self.down_btn = QPushButton("Down")

        transfer_col = QVBoxLayout()
        transfer_col.addWidget(self.add_btn)
        transfer_col.addWidget(self.add_all_btn)
        transfer_col.addWidget(self.remove_btn)
        transfer_col.addWidget(self.clear_sel_btn)
        transfer_col.addSpacing(16)
        transfer_col.addWidget(self.up_btn)
        transfer_col.addWidget(self.down_btn)
        transfer_col.addStretch(1)

        # ---------- Right: Selected (ordered) ----------
        self.selected_list = QListWidget(self)
        self.selected_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.selected_list.setAlternatingRowColors(True)
        # Drag & drop reorder
        self.selected_list.setDragEnabled(True)
        self.selected_list.setAcceptDrops(True)
        self.selected_list.setDropIndicatorShown(True)
        self.selected_list.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)

        # Double-click shortcuts
        self.available_list.itemDoubleClicked.connect(self._add_from_available)
        self.selected_list.itemDoubleClicked.connect(self._remove_from_selected)

        # ---------- Display name + actions to collect sets ----------
        self.display_label = QLabel("Display name:")
        self.display_edit = QLineEdit(self)
        self.display_edit.setPlaceholderText("Enter display name for this set")

        self.add_set_btn = QPushButton("Add set")
        self.update_set_btn = QPushButton("Update set")
        self.update_set_btn.setEnabled(False)
        self.delete_set_btn = QPushButton("Delete set")
        self.clear_sets_btn = QPushButton("Clear all sets")

        # ---------- Table of collected sets ----------
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

        # ---------- OK/Cancel ----------
        self.btn_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self
        )

        # ---------- Layout ----------
        top_grid = QGridLayout()
        top_grid.addWidget(QLabel("Available parameters"), 0, 0)
        top_grid.addWidget(QLabel("Selected parameters (ordered)"), 0, 2)

        left_col = QVBoxLayout()
        left_col.addWidget(self.filter_edit)
        left_col.addWidget(self.available_list)

        right_col = QVBoxLayout()
        right_col.addWidget(self.selected_list)

        top_grid.addLayout(left_col, 1, 0)
        top_grid.addLayout(transfer_col, 1, 1)
        top_grid.addLayout(right_col, 1, 2)

        dn_row = QHBoxLayout()
        dn_row.addWidget(self.display_label)
        dn_row.addWidget(self.display_edit)
        dn_row.addSpacing(20)
        dn_row.addWidget(self.add_set_btn)
        dn_row.addWidget(self.update_set_btn)
        dn_row.addWidget(self.delete_set_btn)
        dn_row.addWidget(self.clear_sets_btn)
        dn_row.addStretch(1)

        main = QVBoxLayout(self)
        main.addLayout(top_grid)
        main.addSpacing(10)
        main.addLayout(dn_row)
        main.addWidget(self.sets_table, stretch=1)
        main.addWidget(self.btn_box)

        # ---------- Wiring ----------
        self.filter_edit.textChanged.connect(self._apply_filter)
        self.add_btn.clicked.connect(self._add_selected_from_available)
        self.add_all_btn.clicked.connect(self._add_all_from_available)
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

        # Minor size policies
        self.available_list.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.selected_list.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.sets_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    # ---------- Helpers ----------
    def _apply_filter(self, text: str) -> None:
        t = text.strip().lower()
        for i in range(self.available_list.count()):
            item = self.available_list.item(i)
            if item is None:
                continue
            item.setHidden(t not in item.text().lower())

    def _add_from_available(self, item: QListWidgetItem | None) -> None:
        if item is None:
            return
        self._add_to_selected([item.text()])

    def _add_selected_from_available(self) -> None:
        items = [i.text() for i in self.available_list.selectedItems() if i is not None]
        if items:
            self._add_to_selected(items)

    def _add_all_from_available(self) -> None:
        items: list[str] = []
        for i in range(self.available_list.count()):
            it = self.available_list.item(i)
            if it is None:
                continue
            if not it.isHidden():
                items.append(it.text())
        if items:
            self._add_to_selected(items)

    def _add_to_selected(self, names: List[str]) -> None:
        # Build a set of existing item texts safely (guard None)
        existing: set[str] = set()
        for i in range(self.selected_list.count()):
            it = self.selected_list.item(i)
            if it is None:
                continue
            existing.add(it.text())

        for name in names:
            if self.allow_duplicates or name not in existing:
                QListWidgetItem(name, self.selected_list)

    def _remove_from_selected(self, item: QListWidgetItem | None) -> None:
        if item is None:
            return
        row = self.selected_list.row(item)
        self.selected_list.takeItem(row)

    def _remove_selected_from_selected(self) -> None:
        for item in self.selected_list.selectedItems():
            if item is None:
                continue
            self._remove_from_selected(item)

    def _move_selected(self, delta: int) -> None:
        """Move the first selected row up/down by delta (+1 or -1)."""
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

    def _collect_current_selection(self) -> ParameterSet | None:
        display_name = self.display_edit.text().strip()
        if not display_name:
            QMessageBox.warning(self, "Missing display name", "Please enter a display name.")
            return None
        params: list[str] = []
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
        # Replace if display name exists
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

    def _current_table_row(self) -> int | None:
        sm = self.sets_table.selectionModel()
        if sm is None:
            return None
        rows = sm.selectedRows()
        return rows[0].row() if rows else None

    def _on_table_selection_changed(self) -> None:
        """Load the selected set into the builder for editing."""
        row = self._current_table_row()
        self.update_set_btn.setEnabled(row is not None)
        if row is None:
            return
        ps = self._sets[row]
        # Load into editor
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
        self.available_list.clearSelection()

    def _on_accept(self) -> None:
        if not self._sets:
            QMessageBox.warning(self, "No sets", "Please add at least one set before clicking OK.")
            return
        self.accept()

    # ---------- Public API ----------
    def result_sets(self) -> List[ParameterSet]:
        """Return the collected (Display Name + Ordered Parameters) sets."""
        return list(self._sets)

if __name__ == "__main__":
    import sys
    from PyQt6.QtWidgets import QApplication, QDialog

    parameter_list = [
        "PRODUCTIONDATE",
        "BARCODE",
        "PARTNUMBER",
        "DIAGNOSTIC_IDENTIFIER",
        "ECU_SW_VERSION",
        "ECU_HW_VERSION",
    ]

    app = QApplication(sys.argv)
    dlg = ParameterSetDialog(parameter_list, parent=None, allow_duplicates=False)
    if dlg.exec() == QDialog.DialogCode.Accepted:
        sets = dlg.result_sets()  # uses the method version
        print("Received parameter sets:")
        for s in sets:
            print(f"  - {s.display_name}: {s.parameters}")
    else:
        print("Dialog cancelled.")
    sys.exit(0)
