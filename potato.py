
base_path = svc_short
rparams = _collect_params(req, "REQUEST", base_path)


base_path = svc_short
rparams = _collect_params(req, "REQUEST", base_path)

base_path = f"{svc_short}.{rshort}"
rparams = _collect_params(el, "NEG_RESPONSE", base_path)


def _collect_params(msg_el, parentType, base_path):
    params: List[OdxParam] = []
    for p_el in findall_descendants(msg_el, "PARAM"):
        rp = self._try_parse_param(
            p_el,
            parentType,
            base_path,          # ← SINGLE SOURCE OF TRUTH
            layer_short,
            svc_short,
            dop_by_id,
            dop_by_sn,
            dop_meta_by_id,
            struct_by_id,
            struct_by_sn,
            table_by_id,
        )
        if rp is not None:
            params.append(rp)
    return params
