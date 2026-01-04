
# copy_ui.py
from __future__ import annotations

import os
import re
import sys
import json
import zipfile
import traceback
from typing import Optional, List, Set, Dict, Any, Tuple

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QPushButton,
    QLineEdit, QLabel, QSplitter, QTreeWidget,
    QTreeWidgetItem, QScrollArea, QFormLayout,
    QStatusBar, QFileDialog, QMessageBox, QComboBox,
)

from parser import ODXParser
from models import OdxService, OdxParam, OdxDatabase, OdxLayer


def _is_odx(name: str) -> bool:
    n = name.lower()
    # Accept .odx, .odx-C/D suffixes, and plain .xml
    return bool(re.search(r"\.odx(?:\-[a-z]+)?$", n)) or n.endswith(".xml")


class MainWindow(QMainWindow):
    """
    ODX Diagnostic Explorer (UI-only renderer)
    - DID-centric service lines (requestDidHex)
    - 'value' preferred over displayHex, then displayValue
    - Recursive rendering relies solely on parser-provided p.children
    - Expand only at initial build (filters don't re-expand)
    """

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("ODX Diagnostic Explorer")
        self.resize(1680, 950)

        # Data
        self.parser = ODXParser()
        self.database: Optional[OdxDatabase] = None
        self.selectedParams: List[OdxParam] = []

        # Filters / State
        self._filter_text: str = ""
        self._filter_variant: str = ""
        self._filter_variant_type: str = "EV"  # reserved for future
        self._filter_semantic: str = ""
        self._filter_sid_int: Optional[int] = None
        self._suppress_item_changed: bool = False

        # Debounce filter application
        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.setInterval(200)
        self._filter_timer.timeout.connect(lambda: self.apply_all_filters(False))

        # UI
        self.build_ui()
        self.apply_theme_light()

    # --------------------------
    # UI construction / theming
    # --------------------------
    def build_ui(self) -> None:
        root = QWidget()
        layout = QVBoxLayout(root)

        # Toolbar
        tb = QHBoxLayout()
        self.btn_open = QPushButton("Open PDX/ODX")
        self.btn_open.clicked.connect(self.open_files)
        tb.addWidget(self.btn_open)

        self.cmb_variant = QComboBox()
        self.cmb_variant.setMinimumWidth(240)
        self.cmb_variant.addItem("All Variants", userData="")
        self.cmb_variant.currentIndexChanged.connect(self.on_variant_changed)
        tb.addWidget(QLabel("Variant:"))
        tb.addWidget(self.cmb_variant)

        self.cmb_semantic = QComboBox()
        self.cmb_semantic.setMinimumWidth(180)
        self.cmb_semantic.addItem("All semantics", userData="")
        self.cmb_semantic.currentIndexChanged.connect(lambda _: self.apply_all_filters(False))
        tb.addWidget(QLabel("Service Semantic:"))
        tb.addWidget(self.cmb_semantic)

        self.cmb_sid = QComboBox()
        self.cmb_sid.setMinimumWidth(180)
        self.cmb_sid.addItem("All SIDs", userData=None)
        self.cmb_sid.currentIndexChanged.connect(lambda _: self.apply_all_filters(False))
        tb.addWidget(QLabel("SID:"))
        tb.addWidget(self.cmb_sid)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search Variant, Service, Message, Parameter...")
        self.search.textChanged.connect(lambda: self._filter_timer.start())
        tb.addWidget(self.search, 1)

        self.lbl_layers = QLabel("0 layers")
        tb.addWidget(self.lbl_layers)

        self.lbl_selected = QLabel("0/0 selected")
        tb.addWidget(self.lbl_selected)

        self.btn_select_all = QPushButton("Select All Visible")
        self.btn_select_all.clicked.connect(self.select_all_visible_params)
        tb.addWidget(self.btn_select_all)

        self.btn_clear_sel = QPushButton("Clear Selection")
        self.btn_clear_sel.clicked.connect(self.clear_selection)
        tb.addWidget(self.btn_clear_sel)

        self.btn_copy_did = QPushButton("Copy DID")
        self.btn_copy_did.clicked.connect(self.copy_current_did)
        tb.addWidget(self.btn_copy_did)

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

        # Splitter: Tree + Details
        split = QSplitter(Qt.Orientation.Horizontal)
        layout.addWidget(split, 1)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Variants & Services", "Type / Semantic", "Info / Value"])
        self.tree.setUniformRowHeights(True)
        self.tree.setExpandsOnDoubleClick(True)
        self.tree.itemSelectionChanged.connect(self.tree_selected)
        self.tree.itemChanged.connect(self.tree_item_changed)
        split.addWidget(self.tree)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.details_host = QWidget()
        self.details = QFormLayout(self.details_host)
        scroll.setWidget(self.details_host)
        split.addWidget(scroll)
        split.setStretchFactor(0, 3)
        split.setStretchFactor(1, 2)

        self.sb = QStatusBar()
        self.setStatusBar(self.sb)
        self.setCentralWidget(root)

    def apply_theme_light(self) -> None:
        self.setStyleSheet("")

    # --------------------------
    # File ops
    # --------------------------
    def open_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self, "Open PDX/ODX/XML", "", "ODX Files (*.odx *.xml *.pdx *.zip)"
        )
        if not files:
            return
        try:
            self.load_files(files)
        except Exception as e:
            traceback.print_exc()
            QMessageBox.critical(self, "Parse error", str(e))

    def _parse_any(self, name: str, raw: bytes):
        return self.parser.parse_odx_bytes(name, raw)

    def _merge_containers(self, containers: List[Any]) -> OdxDatabase:
        normalized = [c[1] if isinstance(c, (tuple, list)) and len(c) >= 2 else c for c in containers]
        return self.parser.merge_containers(normalized)

    def load_files(self, files: List[str]) -> None:
        self.reset_all(clear_status=False)
        containers: List[Any] = []

        for path in files:
            try:
                lower = path.lower()
                if lower.endswith(".pdx") or lower.endswith(".zip"):
                    with zipfile.ZipFile(path, "r") as zf:
                        for name in zf.namelist():
                            if name.endswith("/"):
                                continue
                            if not _is_odx(name):
                                continue
                            raw = zf.read(name)
                            containers.append(self._parse_any(os.path.basename(name), raw))
                else:
                    with open(path, "rb") as f:
                        raw = f.read()
                    containers.append(self._parse_any(os.path.basename(path), raw))
            except Exception:
                traceback.print_exc()
                continue

        if not containers:
            self.sb.showMessage("No valid ODX parsed")
            return

        # Merge parsed containers → database
        try:
            self.database = self._merge_containers(containers)
        except Exception as ex:
            traceback.print_exc()
            QMessageBox.critical(self, "Merge error", f"Failed to merge containers: {ex}")
            return

        # Populate tree after merge
        self.populate_tree(initial_build=True)
        self.sb.showMessage("Load complete")

    # --------------------------
    # Actions / Export
    # --------------------------
    def export_json(self) -> None:
        if not self.database:
            self.sb.showMessage("No database loaded")
            return
        try:
            path, _ = QFileDialog.getSaveFileName(self, "Save JSON", "export.json", "JSON (*.json)")
            if not path:
                return
            data = {
                "ecuVariants": [getattr(ev, "shortName", "") for ev in getattr(self.database, "ecuVariants", []) or []],
                "baseVariants": [getattr(bv, "shortName", "") for bv in getattr(self.database, "baseVariants", []) or []],
                "paramsCount": len(getattr(self.database, "allParams", []) or []),
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            self.sb.showMessage(f"Exported JSON to {path}")
        except Exception as ex:
            traceback.print_exc()
            QMessageBox.critical(self, "Export error", str(ex))

    def export_excel(self) -> None:
        QMessageBox.information(self, "Export Excel", "Excel export not implemented yet.")

    # --------------------------
    # Reset / Filters
    # --------------------------
    def reset_all(self, clear_status: bool = True) -> None:
        self.tree.clear()
        self.selectedParams.clear()
        self._filter_text = ""
        self._filter_variant = ""
        self._filter_semantic = ""
        self._filter_sid_int = None
        self._clear_form(self.details)
        if clear_status:
            self.sb.showMessage("Reset complete")

    def on_variant_changed(self):
        self.apply_all_filters(initial_build=False)

    def apply_all_filters(self, initial_build: bool = False) -> None:
        self.populate_tree(initial_build=initial_build)

    # --------------------------
    # Tree population / building
    # --------------------------
    def populate_tree(self, initial_build: bool = False) -> None:
        """
        UI-only renderer:
        - Read model objects from self.database
        - Render recursively using p.children (already resolved by parser)
        """
        if not self.database:
            return

        self.tree.clear()
        param_count_visible = 0
        layer_count = 0

        # 1) Collect variants
        variants: List[OdxLayer] = []
        variants.extend(getattr(self.database, "ecuVariants", []) or [])
        variants.extend(getattr(self.database, "baseVariants", []) or [])

        # 2) Initialize filter combos (once)
        if initial_build:
            self.cmb_variant.blockSignals(True)
            self.cmb_variant.clear()
            self.cmb_variant.addItem("All Variants", userData="")
            for v in variants:
                sn = getattr(v, "shortName", "")
                self.cmb_variant.addItem(sn, userData=sn)
            self.cmb_variant.blockSignals(False)

            self.cmb_semantic.blockSignals(True)
            self.cmb_semantic.clear()
            self.cmb_semantic.addItem("All semantics", userData="")
            semantics: Set[str] = set()
            for v in variants:
                for s in getattr(v, "services", []) or []:
                    sem = getattr(s, "semantic", "")
                    if sem:
                        semantics.add(sem)
            for sem in sorted(semantics):
                self.cmb_semantic.addItem(sem, userData=sem)
            self.cmb_semantic.blockSignals(False)

            self.cmb_sid.blockSignals(True)
            self.cmb_sid.clear()
            self.cmb_sid.addItem("All SIDs", userData=None)
            sids: Set[int] = set()
            for v in variants:
                for s in getattr(v, "services", []) or []:
                    sid = getattr(s, "sid", None)
                    if isinstance(sid, int):
                        sids.add(sid)
            for sid in sorted(sids):
                self.cmb_sid.addItem(f"0x{sid:02X}", userData=sid)
            self.cmb_sid.blockSignals(False)

        # 3) Read current filters
        self._filter_variant = self.cmb_variant.currentData()
        self._filter_semantic = self.cmb_semantic.currentData()
        self._filter_sid_int = self.cmb_sid.currentData()
        self._filter_text = self.search.text().strip().lower()

        # Helpers
        def m_type_label(kind: str) -> str:
            return {
                "Request": "REQUEST",
                "Positive Response": "POS_RESPONSE",
                "Negative Response": "NEG_RESPONSE",
            }.get(kind, kind.upper())

        def cell_value(p: OdxParam) -> str:
            # Prefer value → displayHex → displayValue
            return (
                getattr(p, "value", "") or
                getattr(p, "displayHex", "") or
                getattr(p, "displayValue", "") or
                ""
            )

        # Recursive renderer (UI-only; parser has already populated children)
        def add_param_recursive(parent_item: QTreeWidgetItem, p: OdxParam) -> bool:
            nonlocal param_count_visible

            pname = getattr(p, "shortName", "") or "(param)"
            semantic = getattr(p, "semantic", "") or ""
            third = cell_value(p)

            # Text filter: if this node doesn't match, check children and only show
            # if any child matches (to preserve structure visibility).
            if self._filter_text:
                match_self = any(
                    self._filter_text in (t or "").lower()
                    for t in (pname, semantic, third)
                )
                if not match_self:
                    any_child = False
                    for c in getattr(p, "children", []) or []:
                        if add_param_recursive(parent_item, c):
                            any_child = True
                    return any_child

            # Render current param
            p_item = QTreeWidgetItem([pname, semantic, third])
            p_item.setFlags(
                p_item.flags()
                | Qt.ItemFlag.ItemIsUserCheckable
                | Qt.ItemFlag.ItemIsSelectable
            )
            p_item.setCheckState(0, Qt.CheckState.Unchecked)
            p_item.setData(0, Qt.ItemDataRole.UserRole, p)
            parent_item.addChild(p_item)
            param_count_visible += 1

            # Render children from parser
            for c in getattr(p, "children", []) or []:
                add_param_recursive(p_item, c)

            return True

        # 4) Build tree
        for v in variants:
            vname = getattr(v, "shortName", "")
            if self._filter_variant and vname != self._filter_variant:
                continue

            v_item = QTreeWidgetItem([
                vname or "(variant)",
                getattr(v, "layerType", ""),
                getattr(v, "description", ""),
            ])
            v_item.setFlags(v_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            v_item.setCheckState(0, Qt.CheckState.Unchecked)
            v_item.setData(0, Qt.ItemDataRole.UserRole, v)
            self.tree.addTopLevelItem(v_item)
            layer_count += 1

            for s in getattr(v, "services", []) or []:
                if self._filter_semantic and getattr(s, "semantic", "") != self._filter_semantic:
                    continue
                sid = getattr(s, "sid", None)
                if self._filter_sid_int is not None and sid != self._filter_sid_int:
                    continue

                did_text = getattr(s, "requestDidHex", "") or ""
                service_texts = [
                    getattr(s, "shortName", ""),
                    getattr(s, "semantic", ""),
                    getattr(s, "longName", ""),
                    getattr(s, "description", ""),
                    did_text,
                ]
                service_passes_text = (
                    not self._filter_text
                    or any(self._filter_text in (t or "").lower() for t in service_texts)
                )

                s_item = QTreeWidgetItem([
                    f"{getattr(s, 'shortName', '')} ({did_text})" if did_text else getattr(s, "shortName", ""),
                    getattr(s, "semantic", ""),
                    getattr(s, "description", "") or "",
                ])
                s_item.setFlags(
                    s_item.flags()
                    | Qt.ItemFlag.ItemIsUserCheckable
                    | Qt.ItemFlag.ItemIsSelectable
                )
                s_item.setCheckState(0, Qt.CheckState.Unchecked)
                s_item.setData(0, Qt.ItemDataRole.UserRole, s)
                v_item.addChild(s_item)

                messages: List[Tuple[str, Any]] = []
                req = getattr(s, "request", None)
                if req:
                    messages.append(("Request", req))
                for r in getattr(s, "posResponses", []) or []:
                    messages.append(("Positive Response", r))
                for r in getattr(s, "negResponses", []) or []:
                    messages.append(("Negative Response", r))

                has_visible_params_any_message = False

                for kind, msg in messages:
                    m_item = QTreeWidgetItem([
                        getattr(msg, "shortName", "") or m_type_label(kind),
                        m_type_label(kind),
                        getattr(msg, "longName", "") or "",
                    ])
                    m_item.setFlags(
                        m_item.flags()
                        | Qt.ItemFlag.ItemIsUserCheckable
                        | Qt.ItemFlag.ItemIsSelectable
                    )
                    m_item.setCheckState(0, Qt.CheckState.Unchecked)
                    m_item.setData(0, Qt.ItemDataRole.UserRole, msg)
                    s_item.addChild(m_item)

                    has_visible_params_this_message = False
                    for p in getattr(msg, "params", []) or []:
                        if add_param_recursive(m_item, p):
                            has_visible_params_this_message = True

                    has_visible_params_any_message |= has_visible_params_this_message

                if not service_passes_text and not has_visible_params_any_message:
                    s_item.setHidden(True)

        # 5) Final UI updates
        if initial_build:
            self.tree.expandToDepth(1)
        self.lbl_layers.setText(f"{layer_count} layers")
        self.lbl_selected.setText(f"{len(self.selectedParams)}/{param_count_visible} selected")

    # --------------------------
    # Selection / Check state
    # --------------------------
    def select_all_visible_params(self):
        self._suppress_item_changed = True
        try:
            for i in range(self.tree.topLevelItemCount()):
                self._set_descendant_checkstate(self.tree.topLevelItem(i), Qt.CheckState.Checked)
        finally:
            self._suppress_item_changed = False
        self.tree_selected()

    def clear_selection(self):
        self._suppress_item_changed = True
        try:
            for i in range(self.tree.topLevelItemCount()):
                self._set_descendant_checkstate(self.tree.topLevelItem(i), Qt.CheckState.Unchecked)
        finally:
            self._suppress_item_changed = False
        self.tree_selected()

    def tree_selected(self):
        item = self.tree.currentItem()
        self._clear_form(self.details)
        if item:
            obj = item.data(0, Qt.ItemDataRole.UserRole)
            if isinstance(obj, OdxParam):
                self.show_param_details(obj)
            elif isinstance(obj, OdxService):
                self.show_service_details(obj)

        self.selectedParams.clear()
        for i in range(self.tree.topLevelItemCount()):
            self._collect_selected_params(self.tree.topLevelItem(i))
        self.lbl_selected.setText(f"{len(self.selectedParams)}/? selected")

    def tree_item_changed(self, item: QTreeWidgetItem, col: int):
        if self._suppress_item_changed or col != 0:
            return
        state = item.checkState(0)
        self._suppress_item_changed = True
        try:
            self._set_descendant_checkstate(item, state)
        finally:
            self._suppress_item_changed = False
        self._refresh_ancestor_state(item)

    def _set_descendant_checkstate(self, item: QTreeWidgetItem, state: Qt.CheckState) -> None:
        item.setCheckState(0, state)
        for i in range(item.childCount()):
            self._set_descendant_checkstate(item.child(i), state)

    def _refresh_ancestor_state(self, item: QTreeWidgetItem) -> None:
        parent = item.parent()
        if not parent:
            return
        checked = 0
        unchecked = 0
        for i in range(parent.childCount()):
            ch = parent.child(i)
            st = ch.checkState(0)
            if st == Qt.CheckState.Checked:
                checked += 1
            elif st == Qt.CheckState.Unchecked:
                unchecked += 1
            else:
                checked += 1
                unchecked += 1
        if checked and unchecked:
            parent.setCheckState(0, Qt.CheckState.PartiallyChecked)
        elif checked and not unchecked:
            parent.setCheckState(0, Qt.CheckState.Checked)
        else:
            parent.setCheckState(0, Qt.CheckState.Unchecked)
        self._refresh_ancestor_state(parent)

    def _collect_selected_params(self, item: QTreeWidgetItem):
        if item.checkState(0) == Qt.CheckState.Checked:
            p = item.data(0, Qt.ItemDataRole.UserRole)
            if isinstance(p, OdxParam):
                self.selectedParams.append(p)
        for i in range(item.childCount()):
            self._collect_selected_params(item.child(i))

    # --------------------------
    # Details / Helpers
    # --------------------------
    def _clear_form(self, form: QFormLayout) -> None:
        while form.rowCount() > 0:
            form.removeRow(0)

    def _add_detail(self, key: str, value: Any) -> None:
        k = QLabel(str(key))
        v = QLabel("" if value is None else str(value))
        v.setWordWrap(True)
        self.details.addRow(k, v)

    def show_param_details(self, p: OdxParam) -> None:
        val = getattr(p, "value", "") or getattr(p, "displayHex", "") or getattr(p, "displayValue", "")
        self._add_detail("Param ShortName", getattr(p, "shortName", ""))
        self._add_detail("Param LongName", getattr(p, "longName", ""))
        self._add_detail("Description", getattr(p, "description", ""))
        self._add_detail("Semantic", getattr(p, "semantic", ""))
        self._add_detail("Value", val)
        self._add_detail("BaseDataType", getattr(p, "baseDataType", ""))
        self._add_detail("PhysicalBaseType", getattr(p, "physicalBaseType", ""))
        self._add_detail("BitLength", getattr(p, "bitLength", ""))
        self._add_detail("BytePosition", getattr(p, "bytePosition", ""))
        self._add_detail("BitPosition", getattr(p, "bitPosition", ""))
        self._add_detail("MinLength", getattr(p, "minLength", ""))
        self._add_detail("MaxLength", getattr(p, "maxLength", ""))
        self._add_detail("HighLowByteOrder", getattr(p, "isHighLowByteOrder", ""))
        self._add_detail("CodedConst", getattr(p, "codedConstValue", ""))
        self._add_detail("PhysicalConst", getattr(p, "physConstValue", ""))
        self._add_detail("DOP Ref Id", getattr(p, "dopRefId", ""))
        self._add_detail("CompuMethod Ref Id", getattr(p, "compuMethodRefId", ""))
        self._add_detail("Parent Type", getattr(p, "parentType", ""))
        self._add_detail("Parent Name", getattr(p, "parentName", ""))
        self._add_detail("Layer Name", getattr(p, "layerName", ""))
        self._add_detail("Service ShortName", getattr(p, "serviceShortName", ""))
        attrs: Dict[str, str] = getattr(p, "attrs", {}) or {}
        if attrs:
            try:
                self._add_detail("Attributes", json.dumps(attrs, indent=2))
            except Exception:
                self._add_detail("Attributes", str(attrs))

    def show_service_details(self, s: OdxService) -> None:
        self._add_detail("Service ShortName", getattr(s, "shortName", ""))
        self._add_detail("Service LongName", getattr(s, "longName", ""))
        self._add_detail("Semantic", getattr(s, "semantic", ""))
        self._add_detail("Request DID", getattr(s, "requestDidHex", ""))
        sid = getattr(s, "sid", None)
        if isinstance(sid, int):
            self._add_detail("SID", f"0x{sid:02X}")
        self._add_detail("Description", getattr(s, "description", ""))
        self._add_detail("Info Text", getattr(s, "infoText", ""))
        self._add_detail("Addressing", getattr(s, "addressing", ""))

    def copy_current_did(self) -> None:
        """
        Copies the DID (requestDidHex) of the selected Service node to clipboard.
        If a Param is selected, searches its ancestor Service.
        """
        item = self.tree.currentItem()
        if not item:
            self.sb.showMessage("Select a service to copy DID")
            return

        obj = item.data(0, Qt.ItemDataRole.UserRole)
        service: Optional[OdxService] = obj if isinstance(obj, OdxService) else None

        if service is None:
            parent = item.parent()
            while parent and service is None:
                pobj = parent.data(0, Qt.ItemDataRole.UserRole)
                if isinstance(pobj, OdxService):
                    service = pobj
                    break
                parent = parent.parent()

        if not service:
            self.sb.showMessage("No service selected for DID")
            return

        did_hex = getattr(service, "requestDidHex", "") or ""
        if not did_hex:
            self.sb.showMessage("Selected service has no requestDidHex")
            return

        QGuiApplication.clipboard().setText(did_hex)
        self.sb.showMessage(f"Copied DID: {did_hex}")


# --------------------------
# Main
# --------------------------
def main() -> None:
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
