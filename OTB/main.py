# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import zipfile
from typing import Any, List, Optional

from PySide6 import QtCore, QtGui, QtWidgets
from PySide6.QtCore import Qt, QModelIndex

from parser import ODXParser, merge_containers, flatten_service_params


# ---------------- Utility ----------------
def _decode_best(raw: bytes) -> str:
    enc_candidates = ("utf-8", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1")
    last_err = None
    for enc in enc_candidates:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError as e:
            last_err = e
            continue
    try:
        return raw.decode("utf-8", errors="ignore")
    except Exception:
        if last_err:
            raise last_err
        raise


# ---------------- Tree Node ----------------
class Node:
    def __init__(self, kind: str, data: dict, parent: Optional["Node"] = None):
        self.kind = kind              # 'layer' | 'service' | 'message' | 'param'
        self.data = data              # dataclass dict
        self.parent = parent
        self.children: List[Node] = []
        self.checked: Qt.CheckState = Qt.Unchecked

    def add(self, child: "Node"):
        self.children.append(child)
        child.parent = self

    def row(self) -> int:
        return self.parent.children.index(self) if self.parent else 0


# ---------------- Tree Model ----------------
class OdxTreeModel(QtCore.QAbstractItemModel):

    COL_NAME = 0
    COL_META = 1
    COL_INFO = 2

    def __init__(self, parent=None):
        super().__init__(parent)
        self.root = Node("root", {})
        self.selected_ids: set[str] = set()

    def columnCount(self, parent=QtCore.QModelIndex()) -> int:
        return 3

    def rowCount(self, parent=QtCore.QModelIndex()) -> int:
        node = self._node(parent)
        return len(node.children)

    def index(self, row: int, col: int, parent=QtCore.QModelIndex()) -> QtCore.QModelIndex:
        if not self.hasIndex(row, col, parent):
            return QtCore.QModelIndex()

        pnode = self._node(parent)
        child = pnode.children[row]
        return self.createIndex(row, col, child)

    def parent(self, index: QtCore.QModelIndex) -> QtCore.QModelIndex:
        if not index.isValid():
            return QtCore.QModelIndex()

        node: Node = index.internalPointer()
        if not node.parent or node.parent == self.root:
            return QtCore.QModelIndex()

        return self.createIndex(node.parent.row(), 0, node.parent)

    def _node(self, index: QtCore.QModelIndex) -> Node:
        return index.internalPointer() if index.isValid() else self.root

    def data(self, index: QtCore.QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid():
            return None

        node: Node = index.internalPointer()

        if role == Qt.DisplayRole:

            if index.column() == self.COL_NAME:
                return node.data.get("shortName") or node.data.get("label") or "-"

            if index.column() == self.COL_META:
                if node.kind == "layer":
                    return node.data.get("layerType", "")
                if node.kind == "service":
                    return node.data.get("semantic", "")
                if node.kind == "message":
                    return node.data.get("parentType", "")
                if node.kind == "param":
                    return node.data.get("semantic", "")

            if index.column() == self.COL_INFO:
                if node.kind == "layer":
                    return f"{len(node.data.get('services', []))} svc"

                if node.kind == "service":
                    total = len(flatten_service_params(node.data))
                    return f"{total} params"

                if node.kind == "message":
                    return f"{len(node.data.get('params', []))}"

                if node.kind == "param":
                    b = node.data.get("bytePosition", "")
                    t = node.data.get("bitPosition", "")
                    parts = []
                    if b: parts.append(f"B{b}")
                    if t: parts.append(f".{t}")
                    return "".join(parts)

        if role == Qt.CheckStateRole and index.column() == self.COL_NAME:
            return node.checked

        return None

    def flags(self, index: QtCore.QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.ItemIsEnabled

        return (
            Qt.ItemIsEnabled
            | Qt.ItemIsSelectable
            | Qt.ItemIsUserCheckable
        )

    # -------- Build Model --------
    def clear(self):
        self.beginResetModel()
        self.root = Node("root", {})
        self.endResetModel()

    def build_from_layers(self, layers: List[dict]):
        self.beginResetModel()
        self.root = Node("root", {})

        for layer in layers:
            lnode = Node("layer", layer, self.root)
            self.root.add(lnode)

            for svc in layer.get("services", []):
                snode = Node("service", svc, lnode)
                lnode.add(snode)

                req = svc.get("request")
                if req and req.get("params"):
                    mnode = Node(
                        "message",
                        {"label": "Request", "parentType": "REQUEST", "params": req.get("params", [])},
                        snode
                    )
                    snode.add(mnode)

                    for p in req.get("params", []):
                        mnode.add(Node("param", p, mnode))

                for i, res in enumerate(svc.get("posResponses", []), start=1):
                    label = "Positive Response" + (f" {i}" if len(svc.get("posResponses", [])) > 1 else "")
                    mnode = Node("message", {"label": label, "parentType": "POS_RESPONSE", "params": res.get("params", [])}, snode)
                    snode.add(mnode)

                    for p in res.get("params", []):
                        mnode.add(Node("param", p, mnode))

                for i, res in enumerate(svc.get("negResponses", []), start=1):
                    label = "Negative Response" + (f" {i}" if len(svc.get("negResponses", [])) > 1 else "")
                    mnode = Node("message", {"label": label, "parentType": "NEG_RESPONSE", "params": res.get("params", [])}, snode)
                    snode.add(mnode)

                    for p in res.get("params", []):
                        mnode.add(Node("param", p, mnode))

        self.endResetModel()


# ---------------- Recursive Filter ----------------
class RecursiveFilterProxy(QtCore.QSortFilterProxyModel):
    def filterAcceptsRow(self, row, parent):
        if super().filterAcceptsRow(row, parent):
            return True

        idx = self.sourceModel().index(row, 0, parent)
        for i in range(self.sourceModel().rowCount(idx)):
            if self.filterAcceptsRow(i, idx):
                return True
        return False


# ---------------- Dark Theme ----------------
def apply_dark_theme(app: QtWidgets.QApplication):
    app.setStyle("Fusion")
    p = QtGui.QPalette()

    p.setColor(QtGui.QPalette.Window, QtGui.QColor("#09090b"))
    p.setColor(QtGui.QPalette.WindowText, QtGui.QColor("#e4e4e7"))
    p.setColor(QtGui.QPalette.Base, QtGui.QColor("#18181b"))
    p.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor("#1c1c20"))
    p.setColor(QtGui.QPalette.ToolTipBase, QtGui.QColor("#27272a"))
    p.setColor(QtGui.QPalette.ToolTipText, QtGui.QColor("#e4e4e7"))
    p.setColor(QtGui.QPalette.Text, QtGui.QColor("#e4e4e7"))
    p.setColor(QtGui.QPalette.Button, QtGui.QColor("#18181b"))
    p.setColor(QtGui.QPalette.ButtonText, QtGui.QColor("#e4e4e7"))
    p.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#3b82f6"))
    p.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#ffffff"))

    app.setPalette(p)


# ---------------- Main Window ----------------
class MainWindow(QtWidgets.QMainWindow):

    def __init__(self):
        super().__init__()

        self.setWindowTitle("ODX Explorer - Python")
        self.resize(1300, 800)

        self.parser = ODXParser()
        self.database = None
        self._compu_by_id = {}

        self._build_ui()
        self.statusBar().showMessage("Drop PDX/ODX/XML or use Open…")

    # -------- UI --------
    def _build_ui(self):
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        v = QtWidgets.QVBoxLayout(central)

        # Toolbar
        tb = QtWidgets.QHBoxLayout()

        self.btn_open = QtWidgets.QPushButton("Open PDX/ODX")
        self.btn_open.clicked.connect(self._open_files)
        tb.addWidget(self.btn_open)

        self.search = QtWidgets.QLineEdit()
        self.search.setPlaceholderText("Search ECU, Service, Parameter…")
        self.search.textChanged.connect(self._on_search)
        tb.addWidget(self.search, 1)

        self.lbl_sel_label = QtWidgets.QLabel("0 selected")
        tb.addWidget(self.lbl_sel_label)

        v.addLayout(tb)

        # Splitter
        splitter = QtWidgets.QSplitter(Qt.Horizontal)
        v.addWidget(splitter, 1)

        # Left Panel
        left = QtWidgets.QWidget()
        left_v = QtWidgets.QVBoxLayout(left)
        left_v.setContentsMargins(0, 0, 0, 0)

        h = QtWidgets.QHBoxLayout()
        self.lbl_layers = QtWidgets.QLabel("ECU Variants & Services")
        self.lbl_count = QtWidgets.QLabel("")
        self.lbl_count.setStyleSheet("color:#a1a1aa;")

        h.addWidget(self.lbl_layers)
        h.addStretch(1)
        h.addWidget(self.lbl_count)
        left_v.addLayout(h)

        self.model = OdxTreeModel(self)
        self.proxy = RecursiveFilterProxy(self)
        self.proxy.setSourceModel(self.model)

        self.view = QtWidgets.QTreeView()
        self.view.setModel(self.proxy)
        self.view.setUniformRowHeights(True)
        self.view.setAlternatingRowColors(True)
        self.view.setAllColumnsShowFocus(True)
        self.view.setExpandsOnDoubleClick(True)
        self.view.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.view.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.view.setHeaderHidden(False)

        left_v.addWidget(self.view, 1)
        splitter.addWidget(left)

    # -------- Load Files --------
    def _open_files(self):
        paths, _ = QtWidgets.QFileDialog.getOpenFileNames(
            self, "Open ODX / PDX", "", "ODX/PDX/XML (*.odx *.pdx *.xml *.zip)"
        )
        if not paths:
            return

        self.load_files(paths)

    def load_files(self, paths: List[str]):
        containers = []

        for path in paths:
            c = self.parser.parse_file(path)
            if c:
                containers.append(c)

        self.database = merge_containers(containers) if containers else None

        if self.database:
            all_layers = (
                self.database.ecuVariants +
                self.database.baseVariants +
                self.database.protocols +
                self.database.functionalGroups +
                self.database.ecuSharedData
            )

            from dataclasses import asdict
            layer_dicts = [asdict(lay) for lay in all_layers]
            self.model.build_from_layers(layer_dicts)

            self.view.expandToDepth(0)
            self.lbl_count.setText(f"{len(all_layers)} layers")

        else:
            self.model.clear()
            self.lbl_count.setText("0")

    # -------- Search --------
    def _on_search(self, text: str):
        self.proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.proxy.setFilterFixedString(text)


# ---------------- Run ----------------
if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    apply_dark_theme(app)

    win = MainWindow()
    win.show()

    sys.exit(app.exec())
