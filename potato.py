def _format_did(self, hb: list[int]) -> Optional[str]:
    if not hb or len(hb) < 2:
        return None
    did = (hb[0] << 8) | hb[1]
    return f"0x{did:04X}"


def _detect_did_for_service_request_only(
    self, svc: OdxService
) -> Optional[str]:

    if not svc.request:
        return None

    params = svc.request.params or []

    # 1. Try explicit DID parameter
    didp = self._detect_did_param(params)
    if didp:
        raw = (
            didp.codedConstValue
            or didp.physConstValue
            or getattr(didp, "defaultValue", None)
        )

        hb = self._hex_bytes_from_string(str(raw) if raw else "")
        fmt = self._format_did(hb)
        if fmt:
            return fmt

    # 2. Try identifier byte collection
    hb2 = self._collect_identifier_bytes(params)
    fmt = self._format_did(hb2)
    if fmt:
        return fmt

    # 3. Fallback scan
    for p in params:
        raw = p.codedConstValue or p.physConstValue or ""
        hb = self._hex_bytes_from_string(str(raw))
        fmt = self._format_did(hb)
        if fmt:
            return fmt

    return None
