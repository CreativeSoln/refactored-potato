
from __future__ import annotations
import sys
from typing import Optional, Set

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTreeWidget, QTreeWidgetItem, QLabel, QComboBox,
    QLineEdit, QFileDialog, QMessageBox
)
from PySide6.QtCore import Qt

from parser import ODXParser
from models import OdxParam, OdxContainer, OdxService

class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("ODX Explorer")

        self.database: Optional[OdxContainer] = None
        self.selectedParams: Set[str] = set()
        self._filter_variant = ""
        self._filter_semantic = ""
        self._filter_text = ""

        self._build_ui()

    def _build_ui(self) -> None:
        root = QWidget(self)
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        bar = QHBoxLayout()
        self.btn_open = QPushButton("Open PDX / ODX")
        self.btn_open.clicked.connect(self.load_files)
        bar.addWidget(self.btn_open)

        self.cmb_variant = QComboBox()
        self.cmb_variant.currentIndexChanged.connect(self.on_variant_changed)
        bar.addWidget(self.cmb_variant)

        self.cmb_semantic = QComboBox()
        self.cmb_semantic.currentIndexChanged.connect(self.apply_all_filters)
        bar.addWidget(self.cmb_semantic)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search")
        self.search.textChanged.connect(self.apply_all_filters)
        bar.addWidget(self.search)

        layout.addLayout(bar)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Name", "Semantic", "Value / Info"])
        self.tree.itemChanged.connect(self.tree_item_changed)
        layout.addWidget(self.tree)

        foot = QHBoxLayout()
        self.lbl_layers = QLabel("0 layers")
        self.lbl_selected = QLabel("0 selected")
        foot.addWidget(self.lbl_layers)
        foot.addWidget(self.lbl_selected)

        self.btn_select_all = QPushButton("Select All")
        self.btn_select_all.clicked.connect(self.select_all_visible_params)
        foot.addWidget(self.btn_select_all)

        self.btn_clear = QPushButton("Clear")
        self.btn_clear.clicked.connect(self.clear_selection)
        foot.addWidget(self.btn_clear)

        layout.addLayout(foot)

    def load_files(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Open ODX/PDX", "", "*.odx *.pdx *.xml")
        if not path:
            return
        try:
            with open(path, "rb") as f:
                data = f.read()
            parser = ODXParser()
            _, self.database = parser.parse_odx_bytes(path, data)
            self.populate_tree(initial_build=True)
            self.tree.expandToDepth(1)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def populate_tree(self, initial_build: bool = False) -> None:
        if not self.database:
            return
        self.tree.clear()

        # Collect all layers
        layers = []
        layers.extend(getattr(self.database, "ecuVariants", []) or [])
        layers.extend(getattr(self.database, "baseVariants", []) or [])

        # Initialize filters once
        if initial_build:
            self.cmb_variant.clear()
            self.cmb_variant.addItem("All Variants", "")
            for v in layers:
                self.cmb_variant.addItem(v.shortName, v.shortName)

            self.cmb_semantic.clear()
            self.cmb_semantic.addItem("All Semantics", "")
            sems = set()
            for v in layers:
                for s in v.services:
                    if s.semantic:
                        sems.add(s.semantic)
            for s in sorted(sems):
                self.cmb_semantic.addItem(s, s)

        self._filter_variant = self.cmb_variant.currentData()
        self._filter_semantic = self.cmb_semantic.currentData()
        self._filter_text = self.search.text().lower()

        layer_count = 0
        param_count = 0

        for v in layers:
            if self._filter_variant and v.shortName != self._filter_variant:
                continue
            v_item = QTreeWidgetItem([v.shortName, v.layerType, v.description])
            self.tree.addTopLevelItem(v_item)
            layer_count += 1

            for svc in v.services:
                if self._filter_semantic and svc.semantic != self._filter_semantic:
                    continue
                # DID-centric title
                did_text = getattr(svc, 'requestDidHex', '') or ''
                title = f"{svc.shortName} ({did_text})" if did_text else svc.shortName
                s_item = QTreeWidgetItem([title, svc.semantic, svc.description])
                v_item.addChild(s_item)

                msgs = []
                if svc.request:
                    msgs.append(("REQUEST", svc.request))
                for r in svc.posResponses:
                    msgs.append(("POS_RESPONSE", r))
                for r in svc.negResponses:
                    msgs.append(("NEG_RESPONSE", r))

                for kind, msg in msgs:
                    m_item = QTreeWidgetItem([msg.shortName or kind, kind, msg.longName])
                    s_item.addChild(m_item)
                    for p in msg.params:
                        param_count += self._add_param_recursive(m_item, p)

        self.lbl_layers.setText(f"{layer_count} layers")
        self.lbl_selected.setText(f"{len(self.selectedParams)}/{param_count} selected")

    def _add_param_recursive(self, parent: QTreeWidgetItem, p: OdxParam) -> int:
        name = p.shortName or "(param)"
        sem = p.semantic or ""
        val = p.value or p.displayHex or p.displayValue or ""
        info = []
        if not val:
            if p.bytePosition:
                info.append(f"BytePos={p.bytePosition}")
            if p.bitLength:
                info.append(f"BitLen={p.bitLength}")
            if p.baseDataType:
                info.append(f"BaseType={p.baseDataType}")
        third = val if val else " | ".join(info)

        # Filter check (value-first + info)
        if self._filter_text:
            if not any(self._filter_text in (t or "").lower() for t in (name, sem, val, third)):
                visible = 0
                for c in p.children:
                    visible += self._add_param_recursive(parent, c)
                return visible

        item = QTreeWidgetItem([name, sem, third])
        item.setFlags(item.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable)
        item.setCheckState(0, Qt.Unchecked)
        item.setData(0, Qt.UserRole, p)
        parent.addChild(item)

        count = 1
        for c in p.children:
            count += self._add_param_recursive(item, c)
        return count

    def apply_all_filters(self) -> None:
        self.populate_tree()

    def on_variant_changed(self) -> None:
        self.apply_all_filters()

    def select_all_visible_params(self) -> None:
        self._walk(self.tree.invisibleRootItem(), True)

    def clear_selection(self) -> None:
        self._walk(self.tree.invisibleRootItem(), False)
        self.selectedParams.clear()
        self.lbl_selected.setText("0 selected")

    def _walk(self, item: QTreeWidgetItem, checked: bool) -> None:
        for i in range(item.childCount()):
            c = item.child(i)
            c.setCheckState(0, Qt.Checked if checked else Qt.Unchecked)
            self._walk(c, checked)

    def tree_item_changed(self, item: QTreeWidgetItem, col: int) -> None:
        p = item.data(0, Qt.UserRole)
        if isinstance(p, OdxParam):
            if item.checkState(0) == Qt.Checked:
                self.selectedParams.add(p.id)
            else:
                self.selectedParams.discard(p.id)
            self.lbl_selected.setText(f"{len(self.selectedParams)} selected")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = MainWindow()
    w.resize(1200, 800)
    w.show()
    sys.exit(app.exec())
