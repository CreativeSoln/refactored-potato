from __future__ import annotations

import sys
from typing import Optional, List, Set

from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QLineEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QSplitter,
    QScrollArea,
    QFormLayout,
    QFileDialog,
    QProgressDialog,
    QMenu,
)
from PySide6.QtCore import (
    Qt,
    QTimer,
    QThread,
    QObject,
    Signal,
    QSettings,
)
from PySide6.QtGui import QBrush, QColor, QFont, QAction

# ------------------------------------------------------------
# Parser / model imports
# ------------------------------------------------------------
from parser import ODXParser
from models import OdxContainer, OdxLayer, OdxService, OdxParam


# ============================================================
# Background worker (thread-safe)
# ============================================================

class OdxLoadWorker(QObject):
    progress = Signal(int, str)        # percent, message
    finished = Signal(object)          # OdxContainer | None
    error = Signal(str)

    def __init__(self, parser: ODXParser, paths: List[str]) -> None:
        super().__init__()
        self._parser = parser
        self._paths = paths
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        containers: List[OdxContainer] = []
        total = len(self._paths)

        for idx, path in enumerate(self._paths):
            if self._cancelled:
                self.finished.emit(None)
                return

            percent = int((idx / max(total, 1)) * 100)
            self.progress.emit(percent, f"Parsing: {path}")

            try:
                _, container = self._parser.parse_odx(path)
                containers.append(container)
            except Exception as e:
                self.error.emit(f"Failed to parse {path}: {e}")

        if not containers:
            self.finished.emit(None)
            return

        if len(containers) == 1:
            self.finished.emit(containers[0])
        else:
            merged = self._parser.merge_containers(containers)
            self.finished.emit(merged)


# ============================================================
# Main Window
# ============================================================

class MainWindow(QMainWindow):

    def __init__(self) -> None:
        super().__init__()

        self.setWindowTitle("ODX Diagnostic Explorer")
        self.resize(1680, 950)

        # Core
        self.parser = ODXParser()
        self.database: Optional[OdxContainer] = None

        # Selection state
        self.selectedParams: List[OdxParam] = []
        self.totalParamsCount = 0
        self.totalLayersCount = 0

        # Filters
        self._filter_text: str = ""

        # Recent files
        self.settings = QSettings("DiagnosticExplorer", "ODXTool")
        self._recent_files: List[str] = []

        # Timers
        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.setInterval(200)
        self._filter_timer.timeout.connect(self.apply_all_filters)

        self._build_ui()
        self._load_recent_files()

    # ========================================================
    # UI
    # ========================================================

    def _build_ui(self) -> None:
        root = QWidget()
        layout = QVBoxLayout(root)

        tb = QHBoxLayout()

        self.btn_open = QPushButton("Open PDX/ODX")
        self.btn_open.clicked.connect(self.open_files)
        tb.addWidget(self.btn_open)

        # Recent files menu
        self.menu_recent = QMenu("Recent Files", self)
        self._recent_actions: List[QAction] = []

        for _ in range(8):
            act = QAction(self)
            act.setVisible(False)
            act.triggered.connect(self._open_recent_file)
            self.menu_recent.addAction(act)
            self._recent_actions.append(act)

        self.btn_open.setMenu(self.menu_recent)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search parameter…")
        self.search.textChanged.connect(lambda _: self._filter_timer.start())
        tb.addWidget(self.search, 1)

        self.lbl_layers = QLabel("0 layers")
        tb.addWidget(self.lbl_layers)

        self.lbl_selected = QLabel("0 / 0 selected")
        tb.addWidget(self.lbl_selected)

        layout.addLayout(tb)

        split = QSplitter(Qt.Orientation.Horizontal)
        layout.addWidget(split, 1)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(
            ["Variants & Services", "Type / Semantic", "Info"]
        )
        self.tree.itemSelectionChanged.connect(self.tree_selected)
        split.addWidget(self.tree)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        host = QWidget()
        self.details = QFormLayout(host)
        scroll.setWidget(host)
        split.addWidget(scroll)

        self.setCentralWidget(root)

    # ========================================================
    # File loading (threaded)
    # ========================================================

    def open_files(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Open PDX / ODX",
            "",
            "PDX / ODX Files (*.pdx *.odx *.odx-d *.odx-c *.odx-e *.xml);;All Files (*)",
        )
        if paths:
            self.load_files(paths)

    def load_files(self, paths: List[str]) -> None:
        self._progress = QProgressDialog(
            "Loading diagnostic data...",
            "Cancel",
            0,
            100,
            self,
        )
        self._progress.setWindowTitle("Loading")
        self._progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        self._progress.show()

        self._thread = QThread(self)
        self._worker = OdxLoadWorker(self.parser, paths)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.progress.connect(self._on_load_progress)
        self._worker.finished.connect(self._on_load_finished)
        self._worker.error.connect(self._on_load_error)
        self._progress.canceled.connect(self._worker.cancel)

        self._worker.finished.connect(self._thread.quit)
        self._worker.finished.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)

        for p in paths:
            self._add_recent_file(p)

        self._thread.start()

    def _on_load_progress(self, percent: int, message: str) -> None:
        self._progress.setValue(percent)
        self._progress.setLabelText(message)

    def _on_load_error(self, msg: str) -> None:
        print("[LOAD ERROR]", msg)

    def _on_load_finished(self, db: Optional[OdxContainer]) -> None:
        self._progress.setValue(100)
        self._progress.close()

        if db is None:
            return

        self.database = db
        self.populate_tree(db)

    # ========================================================
    # Tree population
    # ========================================================

    def populate_tree(self, db: OdxContainer) -> None:
        self.tree.clear()
        self.totalParamsCount = 0
        self.totalLayersCount = 0

        def make_param_item(p: OdxParam) -> QTreeWidgetItem:
            item = QTreeWidgetItem([
                p.shortName or "(param)",
                p.parentType or "",
                p.baseDataType or "",
            ])
            item.setData(0, Qt.ItemDataRole.UserRole, p)
            item.setFlags(
                Qt.ItemFlag.ItemIsEnabled
                | Qt.ItemFlag.ItemIsSelectable
                | Qt.ItemFlag.ItemIsUserCheckable
            )
            item.setCheckState(0, Qt.CheckState.Unchecked)
            return item

        def add_group(title: str, layers: List[OdxLayer]) -> None:
            if not layers:
                return

            root = QTreeWidgetItem([title, "", ""])
            self.tree.addTopLevelItem(root)

            for layer in layers:
                self.totalLayersCount += 1
                lay = QTreeWidgetItem([
                    layer.shortName or "(layer)",
                    layer.layerType,
                    f"{len(layer.services)} svc",
                ])
                root.addChild(lay)

                for svc in layer.services:
                    svc_item = QTreeWidgetItem([
                        svc.shortName or "(service)",
                        "SERVICE",
                        "",
                    ])
                    lay.addChild(svc_item)

                    if svc.request:
                        req = QTreeWidgetItem(["Request", "REQUEST", ""])
                        svc_item.addChild(req)
                        for p in svc.request.params:
                            req.addChild(make_param_item(p))
                            self.totalParamsCount += 1

        add_group("ECU Variants", db.ecuVariants)
        add_group("Base Variants", db.baseVariants)

        self.tree.expandAll()
        self.lbl_layers.setText(f"{self.totalLayersCount} layers")
        self.update_selection_label()

    # ========================================================
    # Filtering
    # ========================================================

    def apply_all_filters(self) -> None:
        self._filter_text = self.search.text().strip().lower()

        for i in range(self.tree.topLevelItemCount()):
            top = self.tree.topLevelItem(i)
            if top is None:
                continue

            top_visible = False

            for j in range(top.childCount()):
                layer = top.child(j)
                if layer is None:
                    continue

                layer_visible = False

                for k in range(layer.childCount()):
                    svc = layer.child(k)
                    if svc is None:
                        continue

                    svc_visible = False

                    for m in range(svc.childCount()):
                        msg = svc.child(m)
                        if msg is None:
                            continue

                        msg_visible = False

                        for pidx in range(msg.childCount()):
                            p_item = msg.child(pidx)
                            if p_item is None:
                                continue

                            name = (p_item.text(0) or "").lower()
                            visible = not self._filter_text or self._filter_text in name
                            p_item.setHidden(not visible)
                            msg_visible |= visible

                        msg.setHidden(not msg_visible)
                        svc_visible |= msg_visible

                    svc.setHidden(not svc_visible)
                    layer_visible |= svc_visible

                layer.setHidden(not layer_visible)
                top_visible |= layer_visible

            top.setHidden(not top_visible)

    # ========================================================
    # Selection / details
    # ========================================================

    def tree_selected(self) -> None:
        items = self.tree.selectedItems()
        if not items:
            return

        payload = items[0].data(0, Qt.ItemDataRole.UserRole)
        if isinstance(payload, OdxParam):
            self.show_param_details(payload)

    def show_param_details(self, p: OdxParam) -> None:
        while self.details.rowCount():
            self.details.removeRow(0)

        def add(k: str, v: Optional[str]) -> None:
            self.details.addRow(QLabel(k), QLabel(str(v or "")))

        add("Name", p.shortName)
        add("Semantic", p.semantic)
        add("Parent Type", p.parentType)
        add("Base Type", p.baseDataType)
        add("Byte Position", str(p.bytePosition))
        add("Bit Length", str(p.bitLength))

    def update_selection_label(self) -> None:
        self.lbl_selected.setText(
            f"{len(self.selectedParams)} / {self.totalParamsCount} selected"
        )

    # ========================================================
    # Recent files
    # ========================================================

    def _load_recent_files(self) -> None:
        files = self.settings.value("recentFiles", [])
        self._recent_files = files if isinstance(files, list) else []
        self._update_recent_menu()

    def _save_recent_files(self) -> None:
        self.settings.setValue("recentFiles", self._recent_files)

    def _add_recent_file(self, path: str) -> None:
        if path in self._recent_files:
            self._recent_files.remove(path)
        self._recent_files.insert(0, path)
        self._recent_files = self._recent_files[:8]
        self._save_recent_files()
        self._update_recent_menu()

    def _update_recent_menu(self) -> None:
        for i, act in enumerate(self._recent_actions):
            if i < len(self._recent_files):
                act.setText(self._recent_files[i])
                act.setData(self._recent_files[i])
                act.setVisible(True)
            else:
                act.setVisible(False)

    def _open_recent_file(self) -> None:
        sender = self.sender()
        if not isinstance(sender, QAction):
            return

        path = sender.data()
        if isinstance(path, str):
            self.load_files([path])


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
