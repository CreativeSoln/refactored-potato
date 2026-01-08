coded_value = ""

# Case 1: xsi:type="CODED-CONST"  (MOST IMPORTANT)
if xsi_type and xsi_type.upper() == "CODED-CONST":
    coded_value = extract_coded_value(param_el)

# Case 2: legacy explicit <CODED-CONST> child (rare)
else:
    codedConst = find_child(param_el, "CODED-CONST")
    if codedConst is not None:
        coded_value = extract_coded_value(codedConst)

# Final safety fallback (important)
if not coded_value:
    coded_value = extract_coded_value(param_el)


def extract_coded_value(scope: Optional[ET.Element]) -> str:
    """
    Extract coded value from ODX PARAM.
    Supports CODED-VALUE and CODED-CONST (ODX compliant).
    """
    if scope is None:
        return ""

    # 1️⃣ Preferred: CODED-VALUE (your PDX uses this)
    cv = first_text(scope, ["CODED-VALUE"])
    if cv:
        return cv.strip()

    # 2️⃣ Alternate: CODED-CONST (allowed by ODX)
    cc = first_text(scope, ["CODED-CONST"])
    if cc:
        return cc.strip()

    # 3️⃣ Fallback: generic <V> (rare but valid)
    v = first_text(scope, ["V"])
    if v:
        return v.strip()

    return ""


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
