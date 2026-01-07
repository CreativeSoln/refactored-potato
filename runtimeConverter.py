#populate_from_param(param: OdxParam)
#Enum table for TEXTTABLE params

from typing import Optional
from PyQt6.QtWidgets import QLabel
from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem
from PyQt6.QtCore import Qt



class ParamDetailPanel(QWidget):
    ...

    def populate_from_param(
        self,
        param: "OdxParam",
        *,
        full_path: str,
        db
    ) -> None:
        """
        Populate the detail panel from an OdxParam.
        This method is READ-ONLY and performs no parsing.
        """

        self._clear()

        def add(label: str, value: Optional[object]) -> None:
            if value is None or value == "":
                return
            self.form.addRow(label, QLabel(str(value)))

        # -------------------------------------------------
        # 1) Identity & Context
        # -------------------------------------------------
        add("Name", param.shortName)
        add("Long Name", getattr(param, "longName", None))
        add("Description", getattr(param, "description", None))
        add("Semantic", param.semantic)
        add("Full Path", full_path)
        add("Service", getattr(param, "serviceShortName", None))
        add("Layer", getattr(param, "layerName", None))

        # -------------------------------------------------
        # 2) Binary Layout
        # -------------------------------------------------
        add("Byte Position", param.bytePosition)
        add("Bit Position", param.bitPosition)
        add("Bit Length", param.bitLength)

        if param.bitLength is not None:
            byte_len = (param.bitLength + 7) // 8
            add("Byte Length", byte_len)

        add("Base Data Type", getattr(param, "baseDataType", None))
        add("Physical Base Type", getattr(param, "physicalBaseType", None))

        # -------------------------------------------------
        # 3) Constants / Raw
        # -------------------------------------------------
        add("Coded Constant", getattr(param, "codedConstValue", None))
        add("Physical Constant", getattr(param, "physConstValue", None))
        add("Display Value", getattr(param, "displayValue", None))

        # -------------------------------------------------
        # 4) DOP
        # -------------------------------------------------
        dop = None
        if param.dopRefId:
            dop = db.dopsById.get(param.dopRefId)
            add("DOP ID", param.dopRefId)
            add("DOP Name", getattr(dop, "shortName", None))
            add("Unit", getattr(dop, "unit", None))

        # -------------------------------------------------
        # 5) COMPU-METHOD
        # -------------------------------------------------
        cm = None
        if dop and dop.compuMethodRef:
            cm = db.compuMethodsById.get(dop.compuMethodRef)

        if cm:
            add("COMPU-METHOD", cm.shortName)
            add("COMPU Category", cm.category)

            raw_min, raw_max, phys_min, phys_max = self._compute_min_max(cm)

            if raw_min is not None or raw_max is not None:
                add("Raw Min / Max", f"{raw_min} .. {raw_max}")

            if phys_min is not None or phys_max is not None:
                unit = getattr(dop, "unit", "")
                add("Physical Min / Max", f"{phys_min} .. {phys_max} {unit}")

        # -------------------------------------------------
        # 6) TABLE / ENUM
        # -------------------------------------------------
        if param.semantic in ("TABLE-KEY", "TABLE-ROW"):
            add("Table Key", getattr(param, "tableKey", None))
            add("Table Name", getattr(param, "tableShortName", None))


def _compute_min_max(self, cm):
    raw_lo, raw_hi, phys_lo, phys_hi = [], [], [], []

    for s in getattr(cm, "scales", []):
        lo = getattr(s, "lowerLimit", None)
        hi = getattr(s, "upperLimit", None)

        if lo is not None:
            raw_lo.append(lo)
        if hi is not None:
            raw_hi.append(hi)

        factor = getattr(s, "factor", None)
        offset = getattr(s, "offset", 0)

        if factor is not None:
            if lo is not None:
                phys_lo.append(lo * factor + offset)
            if hi is not None:
                phys_hi.append(hi * factor + offset)

    return (
        min(raw_lo) if raw_lo else None,
        max(raw_hi) if raw_hi else None,
        min(phys_lo) if phys_lo else None,
        max(phys_hi) if phys_hi else None,
    )


def _clear(self) -> None:
    while self.form.rowCount() > 0:
        self.form.removeRow(0)

def on_param_selected(self, meta):
    param = meta["param"]
    full_path = meta["path"]

    self.details.populate_from_param(
        param,
        full_path=full_path,
        db=self.database
    )

def _create_enum_table(self, cm) -> QTableWidget:
    """
    Create a read-only enum table for TEXTTABLE COMPU-METHOD.
    """
    table = QTableWidget()
    table.setColumnCount(2)
    table.setHorizontalHeaderLabels(["Raw", "Text"])
    table.verticalHeader().setVisible(False)
    table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
    table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
    table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)

    rows = []

    for scale in getattr(cm, "scales", []):
        raw = getattr(scale, "lowerLimit", None)
        text = getattr(scale, "compuConstVT", None)

        if raw is None or text is None:
            continue

        rows.append((raw, text))

    table.setRowCount(len(rows))

    for r, (raw, text) in enumerate(rows):
        raw_item = QTableWidgetItem(f"0x{int(raw):X}")
        raw_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        text_item = QTableWidgetItem(str(text))

        table.setItem(r, 0, raw_item)
        table.setItem(r, 1, text_item)

    table.resizeColumnsToContents()
    table.setMaximumHeight(min(220, table.verticalHeader().length() + 40))

    return table

# -------------------------------------------------
# ENUM / TEXTTABLE
# -------------------------------------------------
if cm.category == "TEXTTABLE":
    enum_table = self._create_enum_table(cm)
    self.form.addRow("Enum Mapping", enum_table)

current_raw = getattr(param, "codedValue", None)


if raw == current_raw:
    raw_item.setBackground(Qt.GlobalColor.lightGray)
    text_item.setBackground(Qt.GlobalColor.lightGray)


