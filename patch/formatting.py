
from __future__ import annotations
from models import OdxParam, OdxService

def param_value(p: OdxParam) -> str:
    return (p.value or p.displayHex or p.displayValue or "")

def param_info_line(p: OdxParam) -> str:
    parts = []
    if p.bytePosition:
        parts.append(f"BytePos={p.bytePosition}")
    if p.bitLength:
        parts.append(f"BitLen={p.bitLength}")
    base = p.baseDataType or p.physicalBaseType
    if base:
        parts.append(f"BaseType={base}")
    const = p.codedConstValue or p.physConstValue
    if const:
        parts.append(f"Const={const}")
    return " | ".join(parts)

def service_title(s: OdxService) -> str:
    did = s.requestDidHex or ""
    return f"{s.shortName} ({did})" if did else s.shortName
