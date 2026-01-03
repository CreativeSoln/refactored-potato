if did_req_hex:
    try:
        # normalize ANY hex form to int
        s = str(did_req_hex).strip().lower()

        if s.startswith("0x"):
            val = int(s, 16)
        else:
            # handles "4df", "f190", "f1 90"
            s = s.replace(" ", "")
            val = int(s, 16)

        svc_info += f" | DID=0x{val:04X}"

    except Exception:
        # fallback: show raw only if parsing fails
        svc_info += f" | DID={did_req_hex}"


if did_req_hex:
    did_fmt = self._format_did_value(did_req_hex)
    if did_fmt:
        svc_info += f" | DID={did_fmt}"


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

fmt = self._format_did_value(raw)
if fmt:
    return fmt

