def _normalize_did_hex(self, value) -> Optional[str]:
    """
    Normalize any DID representation to canonical 16-bit hex format: 0xXXXX.

    Accepts:
      - int (decimal or hex literal)
      - '4DF', '0x4DF'
      - 'F190', '0xF1 90'
      - 'f1 90'
    Returns:
      - '0x04DF', '0xF190', etc.
      - None if value cannot be interpreted
    """
    if value is None:
        return None

    try:
        # Integer input (decimal or hex literal)
        if isinstance(value, int):
            return f"0x{value & 0xFFFF:04X}"

        s = str(value).strip().lower()
        if not s:
            return None

        # Remove prefix and spaces
        if s.startswith("0x"):
            s = s[2:]
        s = s.replace(" ", "")

        # Parse hex
        val = int(s, 16)
        return f"0x{val & 0xFFFF:04X}"

    except Exception:
        return None
