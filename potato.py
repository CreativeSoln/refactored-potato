from __future__ import annotations

import os
import sys
import zipfile
import traceback
from typing import Optional, List, Dict, Any

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QPushButton,
    QLineEdit, QLabel, QSplitter, QTreeWidget,
    QTreeWidgetItem, QScrollArea, QFormLayout,
    QStatusBar, QFileDialog, QMessageBox, QComboBox,
    QAbstractItemView, QMenu
)

from parser import ODXParser
from models import OdxDatabase, OdxLayer, OdxService, OdxParam


# ------------------------------------------------------------
# helpers
# ------------------------------------------------------------

def is_odx(name: str) -> bool:
    n = name.lower()
    return n.endswith((".odx", ".xml", ".odx.xml"))


# ------------------------------------------------------------
# Main Window
# ------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        self.setWindowTitle("ODX Diagnostic Explorer")
        self.resize(1600, 900)

        self.parser = ODXParser()
        self.database: Optional[OdxDatabase] = None

        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.setInterval(200)
        self._filter_timer.timeout.connect(self.apply_all_filters)

        self.build_ui()

    # --------------------------------------------------------
    # UI
    # --------------------------------------------------------

    def build_ui(self) -> None:
        root = QWidget()
        layout = QVBoxLayout(root)

        tb = QHBoxLayout()

        self.btn_open = QPushButton("Open PDX/ODX")
        self.btn_open.clicked.connect(self.open_files)
        tb.addWidget(self.btn_open)

        self.cmb_variant = QComboBox()
        self.cmb_variant.addItem("All Variants", userData=None)
        self.cmb_variant.currentIndexChanged.connect(self.apply_all_filters)
        tb.addWidget(QLabel("Variant:"))
        tb.addWidget(self.cmb_variant)

        self.txt_search = QLineEdit()
        self.txt_search.setPlaceholderText("Search service / param / DID / path…")
        self.txt_search.setClearButtonEnabled(True)
        self.txt_search.textChanged.connect(self.on_search_changed)
        tb.addWidget(self.txt_search, 1)

        layout.addLayout(tb)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        layout.addWidget(splitter, 1)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Name", "Semantic", "Info"])
        self.tree.setUniformRowHeights(True)
        splitter.addWidget(self.tree)

        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_tree_context_menu)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.details_host = QWidget()
        self.details = QFormLayout(self.details_host)
        scroll.setWidget(self.details_host)
        splitter.addWidget(scroll)

        self.sb = QStatusBar()
        self.setStatusBar(self.sb)

        self.setCentralWidget(root)

    # --------------------------------------------------------
    # File loading
    # --------------------------------------------------------

    def open_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self, "Open ODX / PDX", "", "ODX / XML / ZIP (*.odx *.xml *.zip)"
        )
        if not files:
            return
        self.load_files(files)

    def load_files(self, files: List[str]) -> None:
        containers = []

        try:
            for path in files:
                if path.lower().endswith(".zip"):
                    with zipfile.ZipFile(path, "r") as zf:
                        for name in zf.namelist():
                            if is_odx(name):
                                _, cont = self.parser.parse_odx_bytes(
                                    name, zf.read(name)
                                )
                                containers.append(cont)
                else:
                    with open(path, "rb") as f:
                        _, cont = self.parser.parse_odx_bytes(
                            os.path.basename(path), f.read()
                        )
                        containers.append(cont)

            self.database = self.parser.merge_containers(containers)
            self.populate_tree()
            self.build_search_index()
            self.sb.showMessage("Load complete")

        except Exception:
            traceback.print_exc()
            QMessageBox.critical(self, "Error", "Failed to load files")

    # --------------------------------------------------------
    # Tree population
    # --------------------------------------------------------

    def populate_tree(self) -> None:
        self.tree.clear()
        self.cmb_variant.clear()
        self.cmb_variant.addItem("All Variants", userData=None)

        if not self.database:
            return

        for layer in self.database.layers:
            self.cmb_variant.addItem(layer.shortName, userData=layer)

            layer_item = QTreeWidgetItem(self.tree)
            layer_item.setText(0, layer.shortName)
            layer_item.setData(0, Qt.ItemDataRole.UserRole, layer)

            for svc in layer.services:
                svc_item = QTreeWidgetItem(layer_item)
                svc_item.setText(0, svc.shortName)
                svc_item.setText(1, "SERVICE")
                svc_item.setData(0, Qt.ItemDataRole.UserRole, svc)

                if svc.request:
                    self._add_params(svc_item, svc.request.params, svc)

                for resp in svc.posResponses:
                    self._add_params(svc_item, resp.params, svc)

        self.tree.expandToDepth(1)

    def _add_params(
        self,
        parent: QTreeWidgetItem,
        params: List[OdxParam],
        svc: OdxService,
    ) -> None:
        for p in params:
            item = QTreeWidgetItem(parent)
            item.setText(0, p.shortName)
            item.setText(1, p.semantic)

            info = ""
            if p.semantic == "SERVICE-ID" and p.codedValue is not None:
                info = f"0x{int(p.codedValue):02X}"
            elif p.semantic == "DATA-ID" and p.codedValue is not None:
                info = f"0x{int(p.codedValue):04X}"
            elif "TABLE-ROW-KEY" in p.attrs:
                try:
                    info = f"0x{int(p.attrs['TABLE-ROW-KEY']):02X}"
                except Exception:
                    info = str(p.attrs["TABLE-ROW-KEY"])

            item.setText(2, info)

            item.setData(
                0,
                Qt.ItemDataRole.UserRole,
                {
                    "path": p.parentName + "." + p.shortName if p.parentName else p.shortName,
                    "service": svc.shortName,
                },
            )

            if p.children:
                self._add_params(item, p.children, svc)

    # --------------------------------------------------------
    # Search / jump
    # --------------------------------------------------------

    def build_search_index(self) -> None:
        self._search_index: List[tuple[str, QTreeWidgetItem]] = []

        def walk(item: QTreeWidgetItem) -> None:
            text = item.text(0)
            meta = item.data(0, Qt.ItemDataRole.UserRole)

            if isinstance(meta, dict):
                blob = " ".join([
                    text,
                    meta.get("path", ""),
                    meta.get("service", ""),
                ]).lower()
            else:
                blob = text.lower()

            self._search_index.append((blob, item))

            for i in range(item.childCount()):
                child = item.child(i)
                if child is not None:
                    walk(child)

        for i in range(self.tree.topLevelItemCount()):
            top = self.tree.topLevelItem(i)
            if top is not None:
                walk(top)

    def on_search_changed(self, text: str) -> None:
        if not hasattr(self, "_search_index"):
            return

        q = text.strip().lower()
        if not q:
            return

        for blob, item in self._search_index:
            if q in blob:
                self.jump_to_item(item)
                break

    def jump_to_item(self, item: QTreeWidgetItem) -> None:
        parent = item.parent()
        while parent is not None:
            parent.setExpanded(True)
            parent = parent.parent()

        self.tree.setCurrentItem(item)
        self.tree.scrollToItem(item, QAbstractItemView.PositionAtCenter)

    # --------------------------------------------------------
    # Context menu
    # --------------------------------------------------------

    def on_tree_context_menu(self, pos) -> None:
        item = self.tree.itemAt(pos)
        if item is None:
            return

        meta = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(meta, dict):
            return

        path = meta.get("path")
        if not path:
            return

        menu = QMenu(self)
        act = menu.addAction("Copy full path")

        if menu.exec(self.tree.viewport().mapToGlobal(pos)) == act:
            QGuiApplication.clipboard().setText(path)

    # --------------------------------------------------------
    # Filters (no-op but implemented)
    # --------------------------------------------------------

    def apply_all_filters(self) -> None:
        # current implementation keeps tree intact
        pass


# ------------------------------------------------------------
# entry
# ------------------------------------------------------------

def main() -> None:
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
