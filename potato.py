def _fmt_did_from_bytes(self, hb: list[int]) -> Optional[str]:
    if len(hb) < 2:
        return None
    return f"0x{((hb[0] << 8) | hb[1]):04X}"
