def _svc_root(svc_short: str, msg_short: str) -> str:
    if svc_short and msg_short:
        return f"{svc_short}.{msg_short}"
    return svc_short or msg_short or ""


root_path = _svc_root(svc_short, rshort)
params = _collect_params(el, "POS_RESPONSE", root_path, svc_short)

root_path = _svc_root(svc_short, rshort)
params = _collect_params(el, "NEG_RESPONSE", root_path, svc_short)


print("[DBG]", parent_type, rp.path)
