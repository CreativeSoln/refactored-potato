def _display_dual(self, val: Optional[str]) -> str:
    if val is None:
        return ""

    kind = self._classify_numeric_string(str(val
                                            
    if kind not in ('hex', 'bytes'):
        return str(val)

    s_hex = self._display_hex(val)
    dec = self._parse_int_value(val)
    return f"{s_hex} ({dec})" if dec is not None else s_hex

def _display_hex(self, val: Optional[str]) -> Optional[str]:
    if val is None:
        return None

    s = str(val).strip()
    if not s:
        return None

    kind = self._classify_numeric_string(s)

    # ✅ Only display hex if the source itself is hex / bytes
    if kind in ('bytes', 'hex'):
        hb = self._hex_bytes_from_string(s)
        if hb:
            return '0x' + ''.join(f"{b:02X}" for b in hb)
        return s

    # ❌ DO NOT convert decimal to hex
    return s
