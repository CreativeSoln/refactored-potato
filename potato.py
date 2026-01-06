Field	Source
Name	param.shortName
Full Path	meta["path"]
Semantic	param.semantic
Byte Position	param.bytePosition
Bit Position	param.bitPosition
Bit Length	param.bitLength
DOP	param.dopRef
COMPU-METHOD	param.compuMethodRef
Unit	param.unit
Raw Hex	param.rawHex
Physical Value	param.value

Name	param.shortName
Full Path	meta["path"]
Semantic	param.semantic
Byte Position	param.bytePosition
Bit Position	param.bitPosition
Bit Length	param.bitLength
DOP	param.dopRef
COMPU-METHOD	param.compuMethodRef
Unit	param.unit
Raw Hex	param.rawHex
Physical Value	param.value
1️⃣ Wire tree selection → details panel (ONE line)
🔽 Add this in build_ui() (no structure change)
self.tree.currentItemChanged.connect(self.on_tree_selection_changed)

2️⃣ Add a helper to clear the panel
🔽 Add inside MainWindow
def _clear_details(self):
    while self.details.rowCount():
        self.details.removeRow(0)

3️⃣ Add the detail panel renderer (CORE FEATURE)
🔽 Add inside MainWindow
def on_tree_selection_changed(self, item, _prev):
    self._clear_details()

    if item is None:
        return

    meta = item.data(0, Qt.ItemDataRole.UserRole)

    # --------------------------------------------------
    # Case 1: PARAM node (dict metadata)
    # --------------------------------------------------
    if isinstance(meta, dict):
        self.details.addRow("Name:", QLabel(item.text(0)))
        self.details.addRow("Full Path:", QLabel(meta.get("path", "")))
        self.details.addRow("Service:", QLabel(meta.get("service", "")))

        param = meta.get("param")  # optional, if you store it

        if param:
            self._add_param_details(param)
        return

    # --------------------------------------------------
    # Case 2: SERVICE / LAYER node (object metadata)
    # --------------------------------------------------
    self.details.addRow("Name:", QLabel(item.text(0)))
    self.details.addRow("Type:", QLabel(type(meta).__name__))

4️⃣ Render PARAM-specific fields (clean & isolated)
🔽 Add inside MainWindow
def _add_param_details(self, p):
    def add(label, value):
        if value is not None and value != "":
            self.details.addRow(label, QLabel(str(value)))

    add("Semantic", p.semantic)
    add("Byte Position", p.bytePosition)
    add("Bit Position", p.bitPosition)
    add("Bit Length", p.bitLength)
    add("DOP Ref", p.dopRef)
    add("COMPU-METHOD", p.compuMethodRef)
    add("Unit", getattr(p, "unit", None))
    add("Raw Hex", getattr(p, "rawHex", None))
    add("Physical Value", getattr(p, "value", None))

5️⃣ ONE SMALL but IMPORTANT change in tree population

When creating param tree items, add the param object to metadata.

🔴 CURRENT (you already do this)
item.setData(0, Qt.ItemDataRole.UserRole, {
    "path": path,
    "service": svc.shortName,
})

🟢 MODIFY (add ONE key)
item.setData(0, Qt.ItemDataRole.UserRole, {
    "path": path,
    "service": svc.shortName,
    "param": p,   # ← THIS enables full detail panel
})


That’s it. No other structural change.

✅ What this gives you immediately

✔ Clicking a PARAM shows full technical details
✔ Clicking SERVICE / LAYER still works
✔ Works before and after decoding
✔ No duplication of logic
✔ No parser touch
✔ Easy to extend

🧪 Verification checklist

After adding this:

Click SERVICE-ID → see semantic + value

Click DATA param → see byte/bit info

Decode response → click param → value shown

No crashes on layer/service clicks

🔒 Why this is the correct design

UI owns presentation

Parser owns structure

Decoder owns values

Detail panel is read-only, safe, and future-proof

🔜 Easy follow-ups (optional)

Once this is in, the next upgrades are trivial:

Copy any field with one click

Show min/max from COMPU-METHOD

Show decoded + raw side by side

Add “Go to DOP” jump

If you want, tell me which one next and I’ll give you another drop-in patch.
