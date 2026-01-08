coded = (
    self._text(el, "CODED-CONST")
    or self._text(el, "CODED-VALUE")
)
param.codedConstValue = coded


def normalize_did(self, raw: str) -> str:
    """
    Normalize DID to 4-digit uppercase hex.
    Handles decimal and hex input.
    """
    if not raw:
        return ""

    try:
        raw = raw.strip().lower()
        if raw.startswith("0x"):
            val = int(raw, 16)
        else:
            val = int(raw, 10)
        return f"{val:04X}"
    except Exception:
        return ""



did_raw = ""

if svc.request and svc.request.params:
    for p in svc.request.params:
        if (p.semantic or "").upper() == "DATA-ID":
            did_raw = p.codedConstValue
            break

svc.attrs["didNormalized"] = self._fmt.normalize_did(did_raw)


did = service.attrs.get("didNormalized", "")
if did:
    label.setText(f"DID: {did}")
