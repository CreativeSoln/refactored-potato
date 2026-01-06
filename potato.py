
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
    QAbstractItemView, QMenu
)

from parser import ODXParser
from models import OdxService, OdxParam, OdxDatabase, OdxLayer


def _is_odx(name: str) -> bool:
    n = name.lower()
    return (
        n.endswith(".odx")
        or re.search(r"\.odx-[a-z]+$", n) is not None
        or n.endswith(".odx.xml")
        or n.endswith(".xml")
    )


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("ODX Diagnostic Explorer")
        self.resize(1680, 950)

        self.parser = ODXParser()
        self.database: Optional[OdxDatabase] = None

        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.setInterval(200)
        self._filter_timer.timeout.connect(lambda: self.apply_all_filters(False))

        self.build_ui()

    # ---------------- UI ----------------

    def build_ui(self) -> None:
        root = QWidget()
        layout = QVBoxLayout(root)

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

        # >>> SEARCH ADDITION
        self.txt_search = QLineEdit()
        self.txt_search.setPlaceholderText("Search service / param / DID / path...")
        self.txt_search.setClearButtonEnabled(True)
        self.txt_search.textChanged.connect(self.on_search_changed)
        tb.addWidget(self.txt_search, 1)
        # <<< SEARCH ADDITION

        layout.addLayout(tb)

        split = QSplitter(Qt.Orientation.Horizontal)
        layout.addWidget(split, 1)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Variants & Services", "Type / Semantic", "Info / Value"])
        self.tree.setUniformRowHeights(True)
        self.tree.setExpandsOnDoubleClick(True)
        split.addWidget(self.tree)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.details_host = QWidget()
        self.details = QFormLayout(self.details_host)
        scroll.setWidget(self.details_host)
        split.addWidget(scroll)

        self.sb = QStatusBar()
        self.setStatusBar(self.sb)
        self.setCentralWidget(root)

        # >>> SEARCH ADDITION
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_tree_context_menu)
        # <<< SEARCH ADDITION

    # ---------------- SEARCH SUPPORT ----------------

    def build_search_index(self):
        self._search_index = []

        def walk(item: QTreeWidgetItem):
            text = item.text(0)
            meta = item.data(0, Qt.ItemDataRole.UserRole) or {}
            blob = " ".join([
                text,
                meta.get("path", ""),
                meta.get("did", ""),
                meta.get("service", ""),
            ]).lower()

            self._search_index.append((blob, item))
            for i in range(item.childCount()):
                walk(item.child(i))

        for i in range(self.tree.topLevelItemCount()):
            walk(self.tree.topLevelItem(i))

    def on_search_changed(self, text: str):
        if not hasattr(self, "_search_index"):
            return
        t = text.strip().lower()
        if not t:
            return
        for blob, item in self._search_index:
            if t in blob:
                self.jump_to_item(item)
                break

    def jump_to_item(self, item: QTreeWidgetItem):
        parent = item.parent()
        while parent:
            parent.setExpanded(True)
            parent = parent.parent()
        self.tree.setCurrentItem(item)
        self.tree.scrollToItem(item, QAbstractItemView.PositionAtCenter)

    def on_tree_context_menu(self, pos):
        item = self.tree.itemAt(pos)
        if not item:
            return
        meta = item.data(0, Qt.ItemDataRole.UserRole) or {}
        path = meta.get("path")
        if not path:
            return
        menu = QMenu(self)
        act = menu.addAction("Copy full path")
        if menu.exec_(self.tree.viewport().mapToGlobal(pos)) == act:
            QGuiApplication.clipboard().setText(path)

    # ---------------- FILE OPS ----------------

    def open_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self, "Open PDX/ODX/XML", "", "ODX Files (*.odx *.xml *.pdx *.zip)"
        )
        if not files:
            return
        try:
            self.load_files(files)
        except Exception:
            traceback.print_exc()

    def load_files(self, files: List[str]) -> None:
        containers = []
        for path in files:
            try:
                if path.lower().endswith((".pdx", ".zip")):
                    with zipfile.ZipFile(path, "r") as zf:
                        for name in zf.namelist():
                            if _is_odx(name):
                                containers.append(self.parser.parse_odx_bytes(name, zf.read(name)))
                else:
                    with open(path, "rb") as f:
                        containers.append(self.parser.parse_odx_bytes(os.path.basename(path), f.read()))
            except Exception:
                traceback.print_exc()

        self.database = self.parser.merge_containers([c[1] for c in containers])
        self.populate_tree(True)
        self.build_search_index()
        self.sb.showMessage("Load complete")

    # ---------------- PLACEHOLDERS ----------------
    # (all your existing logic remains unchanged)

    def populate_tree(self, initial_build: bool):
        pass

    def apply_all_filters(self, rebuild: bool):
        pass

    def on_variant_changed(self, *_):
        pass


def main() -> None:
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
