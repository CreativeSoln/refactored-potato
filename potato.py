def _format_did_value(self, raw) -> Optional[str]:
    if raw is None:
        return None

    s = str(raw).strip()

    # 1️⃣ Decimal DID from ODX
    if s.isdigit():
        did = int(s, 10)
        return f"0x{did:04X}"

    # 2️⃣ Hex / byte encoded DID
    hb = self._hex_bytes_from_string(s)
    if hb and len(hb) >= 2:
        did = (hb[0] << 8) | hb[1]
        return f"0x{did:04X}"

    return None
