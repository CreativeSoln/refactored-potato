import sys
import os
import socket
import subprocess
import requests
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QComboBox, QLineEdit, QLabel,
    QSplitter, QTextEdit, QTableWidget, QTableWidgetItem,
    QTreeWidget, QTreeWidgetItem, QMessageBox
)
from PySide6.QtCore import Qt

BACKEND_URL = "http://127.0.0.1:5015/load_dids"


# ---------------- Backend Helpers ----------------
def is_port_in_use(port=5015):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", port))
        s.close()
        return False
    except OSError:
        return True


def start_backend():
    backend_path = os.path.join(os.path.dirname(__file__), "backend.py")
    print("Starting backend:", backend_path)
    return subprocess.Popen([sys.executable, backend_path])


# ---------------- UI Class ----------------
class HybridUI(QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("OTXT Generator Pro 3.0 - Hybrid UI")
        self.setGeometry(100, 50, 1500, 900)

        self.jsonData = None
        self.selected_items = []

        self.build_ui()

    # --------------------------------------------------------
    # BUILD UI
    # --------------------------------------------------------
    def build_ui(self):
        main = QVBoxLayout()

        # ---------------- Toolbar ----------------
        toolbar = QHBoxLayout()

        btnLoad = QPushButton("Load PDX")
        btnLoad.clicked.connect(self.load_backend_data)

        self.cboVariant = QComboBox()
        self.cboSemantic = QComboBox()
        self.cboService = QComboBox()

        self.txtSearch = QLineEdit()
        self.txtSearch.setPlaceholderText("Search DID / Path ...")
        self.txtSearch.textChanged.connect(self.apply_all_filters)

        toolbar.addWidget(btnLoad)

        toolbar.addWidget(QLabel("Variant:"))
        toolbar.addWidget(self.cboVariant)

        toolbar.addWidget(QLabel("Semantic:"))
        toolbar.addWidget(self.cboSemantic)

        toolbar.addWidget(QLabel("Service ID:"))
        toolbar.addWidget(self.cboService)

        toolbar.addWidget(QLabel("Search:"))
        toolbar.addWidget(self.txtSearch)

        toolbar.addStretch()
        toolbar.addWidget(QPushButton("Export Log"))
        toolbar.addWidget(QPushButton("Generate OTXT"))

        main.addLayout(toolbar)

        # ---------------- Split Main Body ----------------
        bodySplitter = QSplitter(Qt.Orientation.Vertical)

        # ---------- Upper Split (Tree + Details) ----------
        upperSplitter = QSplitter(Qt.Orientation.Horizontal)

        # ----- LEFT TREE -----
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["DID / Semantic", "Service"])
        self.tree.setColumnWidth(0, 400)

        self.tree.itemClicked.connect(self.tree_item_selected)
        self.tree.itemDoubleClicked.connect(self.tree_item_double_clicked)

        upperSplitter.addWidget(self.tree)

        # ----- RIGHT DETAILS -----
        self.detailsTable = QTableWidget()
        self.detailsTable.setColumnCount(4)
        self.detailsTable.setHorizontalHeaderLabels(
            ["Property", "Value", "Unit", "Notes"]
        )

        upperSplitter.addWidget(self.detailsTable)
        bodySplitter.addWidget(upperSplitter)

        # ---------- Middle Selected Table ----------
        self.selectedTable = QTableWidget()
        self.selectedTable.setColumnCount(5)
        self.selectedTable.setHorizontalHeaderLabels(
            ["Full Path", "DID", "Unit", "Bytes", "Phys Type"]
        )
        bodySplitter.addWidget(self.selectedTable)

        # Remove Button
        self.btnRemove = QPushButton("Remove Selected")
        self.btnRemove.clicked.connect(self.remove_selected)

        # ---------- Bottom Output ----------
        bottom = QVBoxLayout()
        bottom.addWidget(self.btnRemove)
        bottom.addWidget(QLabel("Generated OTXT Code:"))
        self.output = QTextEdit()
        bottom.addWidget(self.output)

        bottomWidget = QWidget()
        bottomWidget.setLayout(bottom)
        bodySplitter.addWidget(bottomWidget)

        main.addWidget(bodySplitter)
        self.setLayout(main)

    # --------------------------------------------------------
    # LOAD BACKEND DATA
    # --------------------------------------------------------
    def load_backend_data(self):
        try:
            resp = requests.get(BACKEND_URL)
            if resp.status_code != 200:
                self.output.setText("Backend Error:\n" + resp.text)
                return

            self.jsonData = resp.json()
            self.populate_tree()
            self.populate_filters()

        except Exception as e:
            self.output.setText("Failed to connect backend:\n" + str(e))

    # --------------------------------------------------------
    # GROUP TREE BY SERVICE
    # --------------------------------------------------------
    def populate_tree(self):
        self.tree.clear()

        did_list = (self.jsonData or {}).get("read_did_groups", [])

        service_groups = {}

        for did_item in did_list:
            service_name = did_item.get("service", "Unknown")
            service_id = did_item.get("sid", "")

            key = f"{service_name} [{service_id}]"
            service_groups.setdefault(key, []).append(did_item)

        for service_label, items in service_groups.items():
            parent = QTreeWidgetItem([service_label, service_label])
            parent.setData(0, Qt.ItemDataRole.UserRole, None)
            self.tree.addTopLevelItem(parent)

            for did_item in items:
                did = did_item.get("did", "UNKNOWN")
                semantic = did_item.get("semantic", "UNKNOWN")

                child = QTreeWidgetItem([
                    f"{did} ({semantic})",
                    service_label
                ])
                child.setData(0, Qt.ItemDataRole.UserRole, did_item)
                parent.addChild(child)

        self.tree.expandAll()

    # --------------------------------------------------------
    # POPULATE FILTER DROPDOWNS
    # --------------------------------------------------------
    def populate_filters(self):
        data = self.jsonData or {}
        ecu_info = data.get("ecuInfo", {})
        did_list = data.get("read_did_groups", [])

        # -------- Variant --------
        variants = {ecu_info.get("variant", "Unknown")}
        self.cboVariant.clear()
        self.cboVariant.addItems(sorted(list(variants)))

        # -------- Semantic --------
        semantics = set()
        for x in did_list:
            s = x.get("semantic") or ""
            if s:
                semantics.add(s)

        self.cboSemantic.blockSignals(True)
        self.cboSemantic.clear()
        self.cboSemantic.addItem("")              # All
        self.cboSemantic.addItems(sorted(list(semantics)))

        # OPTION A default → select first semantic if available
        if self.cboSemantic.count() > 1:
            self.cboSemantic.setCurrentIndex(1)
        self.cboSemantic.blockSignals(False)

        self.cboSemantic.currentTextChanged.connect(self.apply_all_filters)

        # -------- Service (SID) --------
        services = {x.get("sid", "") for x in did_list if x.get("sid")}

        self.cboService.blockSignals(True)
        self.cboService.clear()
        self.cboService.addItem("")              # All
        self.cboService.addItems(sorted(list(services)))
        self.cboService.blockSignals(False)

        self.cboService.currentTextChanged.connect(self.apply_all_filters)

        # -------- NOW apply filters AFTER tree ready --------
        self.apply_all_filters()

    # --------------------------------------------------------
    # FILTER ENGINE (AND logic)
    # --------------------------------------------------------
    def apply_all_filters(self):
        search = self.txtSearch.text().lower()
        semantic_filter = self.cboSemantic.currentText().lower()
        service_filter = self.cboService.currentText().lower()

        any_match = False

        for i in range(self.tree.topLevelItemCount()):
            parent = self.tree.topLevelItem(i)
            if parent is None:
                continue

            parent_visible = False

            for j in range(parent.childCount()):
                node = parent.child(j)
                if node is None:
                    continue

                did_item = node.data(0, Qt.ItemDataRole.UserRole)

                did_text = node.text(0).lower()
                semantic = (did_item.get("semantic", "").lower()
                            if did_item else "")
                service_id = (did_item.get("sid", "").lower()
                            if did_item else "")

                visible = True

                if search and search not in did_text:
                    visible = False

                if semantic_filter and semantic_filter not in semantic:
                    visible = False

                if service_filter and service_filter not in service_id:
                    visible = False

                node.setHidden(not visible)

                if visible:
                    parent_visible = True
                    any_match = True

            parent.setHidden(not parent_visible)

            if parent_visible:
                self.tree.expandItem(parent)
            else:
                self.tree.collapseItem(parent)

        if not search and not semantic_filter and not service_filter:
            self.tree.expandAll()

    # --------------------------------------------------------
    # TREE ITEM CLICK → DETAILS
    # --------------------------------------------------------
    def tree_item_selected(self, item):
        did_item = item.data(0, Qt.ItemDataRole.UserRole)
        if did_item:
            self.fill_details(did_item)

    def fill_details(self, did_item):
        self.detailsTable.setRowCount(0)

        def add_row(label, value):
            r = self.detailsTable.rowCount()
            self.detailsTable.insertRow(r)
            self.detailsTable.setItem(r, 0, QTableWidgetItem(label))
            self.detailsTable.setItem(r, 1, QTableWidgetItem(str(value)))
            self.detailsTable.setItem(r, 2, QTableWidgetItem(""))
            self.detailsTable.setItem(r, 3, QTableWidgetItem(""))

        add_row("DID", did_item.get("did"))
        add_row("Semantic", did_item.get("semantic"))
        add_row("Service", did_item.get("serviceName"))
        add_row("Service", did_item.get("service"))
        add_row("Service ID", did_item.get("sid"))


        selection = did_item.get("selection") or {}

        if selection.get("type") == "tableRow":
            add_row("Type", "Table Row")
            add_row("Path", selection["table"]["rowFullXPath"])

        elif selection.get("type") == "structureLeaf":
            add_row("Type", "Structure Leaf")
            add_row("Path", selection["structure"][0]["path"])

    # --------------------------------------------------------
    # DOUBLE CLICK → ADD DID
    # --------------------------------------------------------
    def tree_item_double_clicked(self, item):
        did_item = item.data(0, Qt.ItemDataRole.UserRole)
        if not did_item:
            return

        did = did_item.get("did", "")
        selection = did_item.get("selection") or {}

        for r in range(self.selectedTable.rowCount()):
            cell = self.selectedTable.item(r, 1)
            if cell is not None and cell.text() == did:
                QMessageBox.information(self, "Info", "DID already added.")
                return


        path = ""   
        if selection.get("type") == "tableRow":
            path = selection["table"]["rowFullXPath"]
        elif selection.get("type") == "structureLeaf":
            path = selection["structure"][0]["path"]

        row = self.selectedTable.rowCount()
        self.selectedTable.insertRow(row)

        self.selectedTable.setItem(row, 0, QTableWidgetItem(path))
        self.selectedTable.setItem(row, 1, QTableWidgetItem(did))
        self.selectedTable.setItem(row, 2, QTableWidgetItem(""))
        self.selectedTable.setItem(row, 3, QTableWidgetItem("4"))
        self.selectedTable.setItem(row, 4, QTableWidgetItem(""))

        self.selected_items.append(did_item)

    # --------------------------------------------------------
    # REMOVE ROW
    # --------------------------------------------------------
    def remove_selected(self):
        row = self.selectedTable.currentRow()
        if row < 0:
            QMessageBox.warning(self, "Warning", "No row selected.")
            return

        self.selectedTable.removeRow(row)

        if row < len(self.selected_items):
            self.selected_items.pop(row)


# --------------------------------------------------------
# RUN
# --------------------------------------------------------
if __name__ == "__main__":

    backend_process = None

    if not is_port_in_use(5015):
        backend_process = start_backend()
    else:
        print("Backend already running. Using existing instance.")

    app = QApplication(sys.argv)
    ui = HybridUI()
    ui.show()

    exit_code = app.exec()

    if backend_process:
        backend_process.terminate()

    sys.exit(exit_code)
