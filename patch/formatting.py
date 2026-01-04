from __future__ import annotations
from typing import Optional, Sequence

class FormatterService:
    """
    Domain-only formatter/resolver.
    Populates presentation fields on models (svc.sid:int, svc.didNormalized:str,
    param.displayValue, param.isSelectableFor22:bool). UI must not call this.
    """

    def parse_sid(self, raw_sid: Optional[str | int]) -> Optional[int]:
        if raw_sid is None:
            return None
        if isinstance(raw_sid, int):
            return raw_sid & 0xFFFF
        s = str(raw_sid).strip().lower().replace(" ", "")
        if not s:
            return None
        try:
            if s.startswith("0x"):
                return int(s, 16) & 0xFFFF
            if s.endswith("h"):
                return int(s[:-1], 16) & 0xFFFF
            if s.isdigit():
                return int(s, 10) & 0xFFFF
            return int(s, 16) & 0xFFFF
        except Exception:
            return None

    def normalize_did(self, raw_did: Optional[str | int]) -> Optional[str]:
        if raw_did is None:
            return None
        if isinstance(raw_did, int):
            return f"0x{raw_did & 0xFFFF:04X}"
        s = str(raw_did).strip().lower().replace(" ", "")
        if not s:
            return None
        try:
            if s.startswith("0x"):
                s = s[2:]
            val = int(s, 16)
            return f"0x{val & 0xFFFF:04X}"
        except Exception:
            return None

    def format_param_display(self, raw_const: Optional[str | int]) -> Optional[str]:
        if raw_const is None:
            return None
        s = str(raw_const).strip()
        return s or None

    def is_param_selectable_for_22(self, sid: Optional[int], parent_type: Optional[str], semantic: Optional[str]) -> bool:
        if sid != 0x22:
            return True
        pt = (parent_type or "").upper()
        sem = (semantic or "").upper()
        return any([
            "POS-RESPONSE" in pt,
            "STRUCTURE" in pt,
            "DATA" in pt,
            "TABLE-ROW" in pt,
            "RECORD-DATA" in sem,
        ])

    def bytes_to_int(self, data: Sequence[int], byteorder: str = "big") -> Optional[int]:
        try:
            if not data:
                return None
            return int.from_bytes(bytes(data), byteorder=byteorder, signed=False)
        except Exception:
            return None
