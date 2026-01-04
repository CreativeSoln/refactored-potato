# =======================
# Hex normalization utils
# =======================

def _hex_from_coded_string(self, s: str) -> Optional[str]:
    """
    Convert coded ODX values like:
      - 'F1 90'
      - '0xF190'
      - 'F190'
      - 'f190'
    into canonical hex string WITHOUT 0x: 'F190'
    """
    if not s:
        return None

    t = s.strip().replace(" ", "").lower()
    if t.startswith("0x"):
        t = t[2:]
    if not re.fullmatch(r"[0-9a-f]+", t):
        return None
    if len(t) % 2 == 1:
        t = "0" + t
    return t.upper()


def _normalize_did_from_hex(self, raw_hex: str) -> Optional[str]:
    """
    Normalize hex string to DID format 0xXXXX (big-endian).
    """
    if not raw_hex or len(raw_hex) < 4:
        return None
    did = int(raw_hex[:4], 16)
    return f"0x{did:04X}"


# -----------------------------
# Canonical coded / hex handling
# -----------------------------
raw_hex = self._hex_from_coded_string(coded_value)

p = OdxParam(
    id=pid,
    shortName=shortName,
    longName=get_text_local(param_el, "LONG-NAME"),
    description=get_text_local(param_el, "DESC"),
    semantic=semantic,
    bytePosition=get_text_local(param_el, "BYTE-POSITION"),
    bitPosition=get_text_local(param_el, "BIT-POSITION"),
    bitLength=get_text_local(diagCodedType, "BIT-LENGTH") if diagCodedType else "",
    minLength=get_text_local(diagCodedType, "MIN-LENGTH") if diagCodedType else "",
    maxLength=get_text_local(diagCodedType, "MAX-LENGTH") if diagCodedType else "",
    baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE") if diagCodedType else "",
    physicalBaseType=get_attr(physType, "BASE-DATA-TYPE") if physType else "",
    isHighLowByteOrder=(
        get_attr(diagCodedType, "IS-HIGH-LOW-BYTE-ORDER")
        or get_attr(diagCodedType, "IS-HIGHLOW-BYTE-ORDER")
    ) if diagCodedType else "",

    # 🔒 authoritative fields
    value=get_text_local(physConst, "V") if physConst else "",
    rawHex=raw_hex or "",
    displayHex=f"0x{raw_hex}" if raw_hex else "",

    dopRefId=get_attr(dopRef, "ID-REF") if dopRef else "",
    dopSnRefName=get_text_local(dopSnRef, "SHORT-NAME") if dopSnRef else "",
    compuMethodRefId=get_attr(compuRef, "ID-REF") if compuRef else "",
    parentType=parentType,
    parentName=parentPath,
    layerName=layerName,
    serviceShortName=serviceShortName,
    attrs=attrs,
)

# -----------------------------
# Detect Request DID (authoritative)
# -----------------------------
requestDidHex = None
if request:
    for p in request.params:
        sem = (p.semantic or "").upper()
        sn = (p.shortName or "").upper()
        if "DATA-ID" in sem or "IDENTIFIER" in sem or sn == "DID":
            if p.rawHex:
                requestDidHex = self._normalize_did_from_hex(p.rawHex)
                break
