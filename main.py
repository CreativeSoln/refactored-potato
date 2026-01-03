
# -*- coding: utf-8 -*-
from __future__ import annotations
import zipfile
import sys, os, json, traceback
from typing import Optional, List
import re

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QPushButton,
    QLineEdit, QLabel, QSplitter, QTreeWidget,
    QTreeWidgetItem, QScrollArea, QFormLayout,
    QStatusBar, QFileDialog, QMessageBox
)

from parser import ODXParser
from models import OdxService, OdxParam, OdxDatabase, OdxMessage, OdxLayer


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ODX Diagnostic Explorer - PyQt6")
        self.resize(1600, 900)

        self.parser = ODXParser()
        self.database: Optional[OdxDatabase] = None

        # Current context
        self.selectedParams: List[OdxParam] = []
        self.totalParamsCount: int = 0
        self.totalLayersCount: int = 0
        self._filter_text = ""

        self.build_ui()
        self.apply_dark_theme()

    # ----------------------- UI -----------------------
    def build_ui(self):
        root = QWidget()
        layout = QVBoxLayout(root)

        # ======== Toolbar ========
        tb = QHBoxLayout()
        self.btn_open = QPushButton("Open PDX/ODX")
        self.btn_open.clicked.connect(self.open_files)
        tb.addWidget(self.btn_open)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search ECU, Service, Parameter...")
        self.search.textChanged.connect(self.apply_filter)
        tb.addWidget(self.search, 1)

        self.lbl_layers = QLabel("0 layers")
        tb.addWidget(self.lbl_layers)

        self.lbl_selected = QLabel("0 / 0 selected")
        tb.addWidget(self.lbl_selected)

        self.btn_select_all = QPushButton("Select All Visible")
        self.btn_select_all.clicked.connect(self.select_all_visible_params)
        tb.addWidget(self.btn_select_all)

        self.btn_clear_sel = QPushButton("Clear Selection")
        self.btn_clear_sel.clicked.connect(self.clear_selection)
        tb.addWidget(self.btn_clear_sel)

        self.btn_json = QPushButton("Export JSON")
        self.btn_json.clicked.connect(self.export_json)
        tb.addWidget(self.btn_json)

        self.btn_excel = QPushButton("Export Excel")
        self.btn_excel.clicked.connect(self.export_excel)
        tb.addWidget(self.btn_excel)

        self.btn_reset = QPushButton("Reset")
        self.btn_reset.clicked.connect(self.reset_all)
        tb.addWidget(self.btn_reset)

        layout.addLayout(tb)

        # ======== Split (2 panes ONLY) ========
        split = QSplitter(Qt.Orientation.Horizontal)
        layout.addWidget(split, 1)

        # ---- LEFT: Multi-column Tree ----
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["ECU Variants & Services", "Type / Semantic", "Info"])
        self.tree.setColumnWidth(0, 420)
        self.tree.setColumnWidth(1, 260)
        self.tree.setColumnWidth(2, 160)
        self.tree.itemSelectionChanged.connect(self.tree_selected)
        self.tree.itemChanged.connect(self.tree_item_changed)
        split.addWidget(self.tree)

        # ---- RIGHT: Details pane (high contrast) ----
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.details_host = QWidget()
        self.details_host.setObjectName('DetailsPane')  # for targeted styling
        self.details = QFormLayout(self.details_host)
        scroll.setWidget(self.details_host)
        split.addWidget(scroll)

        # Stretch so right pane has room
        split.setStretchFactor(0, 3)
        split.setStretchFactor(1, 2)

        # Status bar
        self.sb = QStatusBar()
        self.setStatusBar(self.sb)

        self.setCentralWidget(root)

    def apply_dark_theme(self):
        self.setStyleSheet("""
        QMainWindow { background:#101010; color:white; }
        QTreeWidget, QScrollArea {
            background:#1b1b1b; color:white;
            border:1px solid #333;
        }
        QHeaderView::section {
            background:#222;
            padding:4px;
            border:0px;
        }
        QLineEdit {
            background:#2b2b2b;
            border:1px solid #444;
            padding:6px;
            color:white;
        }
        QPushButton {
            background:#333;
            padding:8px;
            border:1px solid #555;
            color:white;
        }
        QPushButton:hover { background:#444; }
        QLabel { color:white; }
        QGroupBox { color:#e0e0e0; }
        QStatusBar { background:#111; color:white; }

        /* Right pane high contrast */
        #DetailsPane { background:#111; }
        QScrollArea > QWidget > QWidget { background:#111; }
        #DetailsPane QLabel { color:#ffffff; }
        #DetailsPane QLabel[role='key'] { color:#cfcfcf; }
        #DetailsPane QLabel[role='header'] { color:#ffffff; font-weight:bold; font-size:14px; }
        """)

    # Utility: clear QFormLayout safely
    def _clear_form(self, form: QFormLayout):
        for i in reversed(range(form.rowCount())):
            label_item = form.itemAt(i, QFormLayout.ItemRole.LabelRole)
            field_item = form.itemAt(i, QFormLayout.ItemRole.FieldRole)
            for item in (label_item, field_item):
                if item is not None:
                    w = item.widget()
                    if w is not None:
                        w.deleteLater()
            form.removeRow(i)

    # ----------------------- File ops -----------------------
    def open_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self, "Open PDX/ODX/XML", "", "ODX Files (*.odx *.xml *.pdx)"
        )
        if not files:
            return
        try:
            self.load_files(files)
        except Exception as e:
            traceback.print_exc()
            QMessageBox.critical(self, "Parse error", str(e))

    def _is_odx(self, filename: str) -> bool:
        lower = filename.lower()
        return bool(re.search(r"\.odx(\-[a-z]+)?$", lower)) or lower.endswith(".xml")
    
    def _parse_any(self, filename: str, content: bytes):
        cont = self.parser.parse_odx_bytes(filename, content)
        return cont

    def load_files(self, files: List[str]):
        self.reset_all(clear_status=False)
        containers = []

        for p in files:
            try:
                lower = p.lower()

                # ---------- PDX / ZIP ----------
                if lower.endswith(".pdx") or lower.endswith(".zip"):
                    with zipfile.ZipFile(p, "r") as z:
                        for name in z.namelist():
                            if name.endswith("/"):
                                continue
                            if not self._is_odx(name):
                                continue
                            raw = z.read(name)
                            _, cont = self._parse_any(os.path.basename(name), raw)
                            containers.append(cont)

                # ---------- ODX / XML ----------
                else:
                    with open(p, "rb") as f:
                        raw = f.read()
                    _, cont = self._parse_any(os.path.basename(p), raw)
                    containers.append(cont)

            except Exception:
                traceback.print_exc()
                continue

        if not containers:
            self.sb.showMessage("No valid ODX parsed")
            return

        self.database = self.parser.merge_containers(containers)
        self.populate_tree(self.database)

        total_layers = (
            len(self.database.ecuVariants)
            + len(self.database.baseVariants)
            + len(self.database.protocols)
            + len(self.database.functionalGroups)
        )
        self.sb.showMessage(f"Load complete, {total_layers} layers")

    # ----------------------- Left Tree -----------------------
    def populate_tree(self, db: OdxDatabase):
        self.tree.clear()
        self.totalParamsCount = 0
        self.totalLayersCount = 0

        def make_param_item(p: OdxParam) -> QTreeWidgetItem:
            # Columns: Name | Type/Semantic | Info
            type_sem = f"{p.parentType} / {p.semantic}".strip()
            info = p.baseDataType or ""
            item = QTreeWidgetItem([p.shortName or "(param)", type_sem, info])
            item.setData(0, Qt.ItemDataRole.UserRole, p)
            # Make params checkable for export selection
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable |
                          Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)
            item.setCheckState(0, Qt.CheckState.Unchecked)
            return item

        def add_group(name: str, layers: List[OdxLayer]):
            if not layers:
                return
            root = QTreeWidgetItem([name, "", ""])
            self.tree.addTopLevelItem(root)
            for layer in layers:
                self.totalLayersCount += 1
                type_sem = f"{layer.layerType}"
                info = f"{len(layer.services)} svc"
                lay = QTreeWidgetItem([layer.shortName, type_sem, info])
                lay.setData(0, Qt.ItemDataRole.UserRole, layer)
                root.addChild(lay)

                for svc in layer.services:
                    # Service row uses colon style, like screenshot
                    svc_type_sem = f"SERVICE: {svc.semantic}".strip()
                    svc_info = f"{self._count_params_in_service(svc)} params"
                    svc_item = QTreeWidgetItem([svc.shortName, svc_type_sem, svc_info])
                    svc_item.setData(0, Qt.ItemDataRole.UserRole, svc)
                    lay.addChild(svc_item)

                    # REQUEST
                    if svc.request:
                        req_info = f"{len(svc.request.params)} params"
                        req_item = QTreeWidgetItem([svc.request.shortName or "Request", "REQUEST", req_info])
                        req_item.setData(0, Qt.ItemDataRole.UserRole, ("MSG", svc, svc.request))
                        svc_item.addChild(req_item)
                        for p in svc.request.params:
                            req_item.addChild(make_param_item(p)); self.totalParamsCount += 1

                    # POSITIVE RESPONSES (hyphen style)
                    for r in svc.posResponses:
                        pos_info = f"{len(r.params)} params"
                        pos_item = QTreeWidgetItem([r.shortName or "PosResponse", "POS-RESPONSE", pos_info])
                        pos_item.setData(0, Qt.ItemDataRole.UserRole, ("MSG", svc, r))
                        svc_item.addChild(pos_item)
                        for p in r.params:
                            pos_item.addChild(make_param_item(p)); self.totalParamsCount += 1

                    # NEGATIVE RESPONSES (hyphen style)
                    for r in svc.negResponses:
                        neg_info = f"{len(r.params)} params"
                        neg_item = QTreeWidgetItem([r.shortName or "NegResponse", "NEG-RESPONSE", neg_info])
                        neg_item.setData(0, Qt.ItemDataRole.UserRole, ("MSG", svc, r))
                        svc_item.addChild(neg_item)
                        for p in r.params:
                            neg_item.addChild(make_param_item(p)); self.totalParamsCount += 1

        add_group("ECU Variants", db.ecuVariants)
        add_group("Base Variants", db.baseVariants)
        add_group("Protocols", db.protocols)
        add_group("Functional Groups", db.functionalGroups)
        self.tree.expandAll()

        # Update toolbar labels
        self.lbl_layers.setText(f"{self.totalLayersCount} layers")
        self.update_selection_label()

    def _count_params_in_service(self, svc: OdxService) -> int:
        n = 0
        if svc.request:
            n += len(svc.request.params)
        for r in svc.posResponses:
            n += len(r.params)
        for r in svc.negResponses:
            n += len(r.params)
        return n

    def tree_selected(self):
        sel = self.tree.selectedItems()
        if not sel:
            return
        item = sel[0]
        payload = item.data(0, Qt.ItemDataRole.UserRole)
        if isinstance(payload, OdxParam):
            self.show_param_details(payload)
        else:
            self._clear_form(self.details)

    def tree_item_changed(self, item: QTreeWidgetItem, column: int):
        # Update selectedParams when a param checkbox is toggled
        payload = item.data(0, Qt.ItemDataRole.UserRole)
        if isinstance(payload, OdxParam):
            if item.checkState(0) == Qt.CheckState.Checked:
                if payload not in self.selectedParams:
                    self.selectedParams.append(payload)
            else:
                try:
                    self.selectedParams.remove(payload)
                except ValueError:
                    pass
            self.update_selection_label()

    # ----------------------- Right Details -----------------------
    def _add_detail(self, key: str, value: str):
        key_lbl = QLabel(key)
        key_lbl.setProperty('role', 'key')  # dimmer label via stylesheet
        val_lbl = QLabel(value if value else "—")
        val_lbl.setWordWrap(True)
        val_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.details.addRow(key_lbl, val_lbl)

    def show_param_details(self, p: OdxParam):
        self._clear_form(self.details)
        header = QLabel(f"PARAMETER - {p.parentType or ''}")
        header.setProperty('role', 'header')
        self.details.addRow(header, QLabel(""))

        # Identity
        self._add_detail("Short Name", p.shortName or "")
        self._add_detail("Long Name", p.longName or "")
        self._add_detail("Desc", p.description or "")
        self._add_detail("Semantic", p.semantic or "")
        self._add_detail("Service", p.serviceShortName or "")
        self._add_detail("ECU/Layer", p.layerName or "")

        # Encoding / positions
        self._add_detail("Byte Position", p.bytePosition or "")
        self._add_detail("Bit Position", p.bitPosition or "")
        self._add_detail("Bit Length", p.bitLength or "")
        self._add_detail("Min Length", p.minLength or "")
        self._add_detail("Max Length", p.maxLength or "")
        self._add_detail("Base Data Type", p.baseDataType or "")
        self._add_detail("Physical Base Type", p.physicalBaseType or "")
        self._add_detail("High/Low Byte Order", p.isHighLowByteOrder or "")

        # Constants
        self._add_detail("Coded Const Value", p.codedConstValue or "")
        self._add_detail("PhysConst Value", p.physConstValue or "")

        # References
        self._add_detail("DOP Ref ID", p.dopRefId or "")
        self._add_detail("DOP SNRef Name", p.dopSnRefName or "")
        self._add_detail("Compu Method Ref", p.compuMethodRefId or "")

        self.sb.showMessage(f"Details: {p.shortName} ({p.parentType})")

    # ----------------------- Filtering -----------------------
    def apply_filter(self):
        self._filter_text = self.search.text().strip().lower()

        # Filter the left tree safely (including params)
        for i in range(self.tree.topLevelItemCount()):
            top = self.tree.topLevelItem(i)
            if top is None:
                continue
            for j in range(top.childCount()):
                layer = top.child(j)
                if layer is None:
                    continue
                layer.setHidden(False)
                show_layer = False
                for k in range(layer.childCount()):
                    svc_item = layer.child(k)
                    if svc_item is None:
                        continue
                    svc_name = (svc_item.text(0) or "").lower()
                    svc_match = (self._filter_text in svc_name) if self._filter_text else True

                    any_match_under_svc = False

                    # Messages under service
                    for m in range(svc_item.childCount()):
                        msg_item = svc_item.child(m)
                        if msg_item is None:
                            continue
                        msg_name = (msg_item.text(0) or "").lower()
                        msg_match = (self._filter_text in msg_name) if self._filter_text else True

                        any_match_under_msg = False
                        # Params under message
                        for pidx in range(msg_item.childCount()):
                            p_item = msg_item.child(pidx)
                            if p_item is None:
                                continue
                            pname = (p_item.text(0) or "").lower()
                            p_match = (self._filter_text in pname) if self._filter_text else True
                            p_item.setHidden(not p_match)
                            any_match_under_msg = any_match_under_msg or p_match

                        msg_item.setHidden(not (msg_match or any_match_under_msg))
                        any_match_under_svc = any_match_under_svc or (msg_match or any_match_under_msg)

                    svc_item.setHidden(not (svc_match or any_match_under_svc))
                    if svc_match or any_match_under_svc:
                        show_layer = True

                layer.setHidden(not show_layer if self._filter_text else False)

        # Update selected count
        self.update_selection_label()

    # ----------------------- Selection helpers -----------------------
    def clear_selection(self):
        # Uncheck all param items
        for i in range(self.tree.topLevelItemCount()):
            top = self.tree.topLevelItem(i)
            if top is None:
                continue
            for j in range(top.childCount()):
                layer = top.child(j)
                if layer is None:
                    continue
                for k in range(layer.childCount()):
                    svc_item = layer.child(k)
                    if svc_item is None:
                        continue
                    for m in range(svc_item.childCount()):
                        msg_item = svc_item.child(m)
                        if msg_item is None:
                            continue
                        for pidx in range(msg_item.childCount()):
                            p_item = msg_item.child(pidx)
                            if p_item is None:
                                continue
                            if p_item.checkState(0) == Qt.CheckState.Checked:
                                p_item.setCheckState(0, Qt.CheckState.Unchecked)
        self.selectedParams.clear()
        self.update_selection_label()

    def select_all_visible_params(self):
        # Check all visible param items
        for i in range(self.tree.topLevelItemCount()):
            top = self.tree.topLevelItem(i)
            if top is None:
                continue
            for j in range(top.childCount()):
                layer = top.child(j)
                if layer is None or layer.isHidden():
                    continue
                for k in range(layer.childCount()):
                    svc_item = layer.child(k)
                    if svc_item is None or svc_item.isHidden():
                        continue
                    for m in range(svc_item.childCount()):
                        msg_item = svc_item.child(m)
                        if msg_item is None or msg_item.isHidden():
                            continue
                        for pidx in range(msg_item.childCount()):
                            p_item = msg_item.child(pidx)
                            if p_item is None or p_item.isHidden():
                                continue
                            if p_item.checkState(0) != Qt.CheckState.Checked:
                                p_item.setCheckState(0, Qt.CheckState.Checked)
        self.update_selection_label()

    def update_selection_label(self):
        total = self.totalParamsCount
        self.lbl_selected.setText(f"{len(self.selectedParams)} / {total} selected")

    # ----------------------- Export -----------------------
    def export_json(self):
        if not self.selectedParams:
            self.sb.showMessage("No parameters selected")
            return
        data = [p.__dict__ for p in self.selectedParams]
        path, _ = QFileDialog.getSaveFileName(self, "Save JSON", "", "JSON (*.json)")
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            self.sb.showMessage("JSON Exported")
        except Exception as ex:
            QMessageBox.critical(self, "Export JSON", str(ex))

    def export_excel(self):
        try:
            import pandas as pd
        except Exception:
            QMessageBox.warning(self, "Excel", "Install pandas to enable Excel export")
            return
        if not self.selectedParams:
            self.sb.showMessage("No parameters selected")
            return
        df = pd.DataFrame([p.__dict__ for p in self.selectedParams])
        path, _ = QFileDialog.getSaveFileName(self, "Save Excel", "", "Excel (*.xlsx)")
        if not path:
            return
        try:
            df.to_excel(path, index=False)
            self.sb.showMessage("Excel Exported")
        except Exception as ex:
            QMessageBox.critical(self, "Export Excel", str(ex))

    # ----------------------- Reset -----------------------
    def reset_all(self, clear_status: bool = True):
        self.tree.clear()
        self._clear_form(self.details)
        self.database = None
        self.selectedParams = []
        self.totalParamsCount = 0
        self.totalLayersCount = 0
        self.update_selection_label()
        if clear_status:
            self.sb.showMessage("Reset complete")


# ==============================================================
def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
