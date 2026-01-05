# ---------------- SERVICES (inline + references) ----------------
services: List[OdxService] = []
attached_pos_ids: Set[str] = set()
attached_neg_ids: Set[str] = set()

for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
    svc_attrs = get_all_attrs(svc_el)
    svc_short = get_text_local(svc_el, "SHORT-NAME")

    request_ref = find_child(svc_el, "REQUEST-REF")
    request_ref_id = get_attr(request_ref, "ID-REF") if request_ref is not None else ""

    pos_ref_ids = [get_attr(r, "ID-REF") for r in find_children(svc_el, "POS-RESPONSE-REF")]
    neg_ref_ids = [get_attr(r, "ID-REF") for r in find_children(svc_el, "NEG-RESPONSE-REF")]

    inline_req = find_child(svc_el, "REQUEST")
    inline_pos = find_children(svc_el, "POS-RESPONSE")
    inline_neg = find_children(svc_el, "NEG-RESPONSE")

    # ---------- REQUEST ----------
    request = None
    if request_ref_id and request_ref_id in request_map:
        request = request_map[request_ref_id]
        prefix = f"{svc_short}.{request.shortName or 'Request'}" if svc_short else (request.shortName or "")
        self._prefix_path(request.params, prefix)
        self._annotate_service_name(request.params, svc_short)

    elif inline_req is not None:
        rshort = get_text_local(inline_req, "SHORT-NAME") or f"{svc_short}_req"
        root_path = f"{svc_short}.{rshort}" if svc_short else rshort
        rparams: List[OdxParam] = []

        for p_el in findall_descendants(inline_req, "PARAM"):
            rp = self._try_parse_param(
                p_el, "REQUEST", root_path, layer_short, svc_short,
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            if rp:
                rparams.append(rp)

        self._annotate_service_name(rparams, svc_short)
        request = OdxMessage(
            id=get_attr(inline_req, "ID"),
            shortName=rshort,
            longName=get_text_local(inline_req, "LONG-NAME"),
            params=rparams,
        )

    # ---------- POS RESPONSES ----------
    pos_responses: List[OdxMessage] = []

    for rid in pos_ref_ids:
        rr = pos_resp_map.get(rid)
        if rr:
            rr = self._clone_message(rr)
            prefix = f"{svc_short}.{rr.shortName or 'PosResponse'}" if svc_short else (rr.shortName or "")
            self._prefix_path(rr.params, prefix)
            self._annotate_service_name(rr.params, svc_short)
            pos_responses.append(rr)
            attached_pos_ids.add(rid)

    for el in inline_pos:
        rshort = get_text_local(el, "SHORT-NAME") or f"{svc_short}_pos"
        root_path = f"{svc_short}.{rshort}" if svc_short else rshort
        rparams: List[OdxParam] = []

        for p_el in findall_descendants(el, "PARAM"):
            rp = self._try_parse_param(
                p_el, "POS_RESPONSE", root_path, layer_short, svc_short,
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            if rp:
                rparams.append(rp)

        self._annotate_service_name(rparams, svc_short)
        pos_responses.append(
            OdxMessage(
                id=get_attr(el, "ID"),
                shortName=rshort,
                longName=get_text_local(el, "LONG-NAME"),
                params=rparams,
            )
        )
        if get_attr(el, "ID"):
            attached_pos_ids.add(get_attr(el, "ID"))

    # ---------- NEG RESPONSES ----------
    neg_responses: List[OdxMessage] = []

    for rid in neg_ref_ids:
        rr = neg_resp_map.get(rid)
        if rr:
            rr = self._clone_message(rr)
            prefix = f"{svc_short}.{rr.shortName or 'NegResponse'}" if svc_short else (rr.shortName or "")
            self._prefix_path(rr.params, prefix)
            self._annotate_service_name(rr.params, svc_short)
            neg_responses.append(rr)
            attached_neg_ids.add(rid)

    for el in inline_neg:
        rshort = get_text_local(el, "SHORT-NAME") or f"{svc_short}_neg"
        root_path = f"{svc_short}.{rshort}" if svc_short else rshort
        rparams: List[OdxParam] = []

        for p_el in findall_descendants(el, "PARAM"):
            rp = self._try_parse_param(
                p_el, "NEG_RESPONSE", root_path, layer_short, svc_short,
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            if rp:
                rparams.append(rp)

        self._annotate_service_name(rparams, svc_short)
        neg_responses.append(
            OdxMessage(
                id=get_attr(el, "ID"),
                shortName=rshort,
                longName=get_text_local(el, "LONG-NAME"),
                params=rparams,
            )
        )
        if get_attr(el, "ID"):
            attached_neg_ids.add(get_attr(el, "ID"))

    services.append(
        OdxService(
            id=svc_attrs.get("ID", ""),
            shortName=svc_short,
            longName=get_text_local(svc_el, "LONG-NAME"),
            description=get_text_local(svc_el, "DESC"),
            semantic=svc_attrs.get("SEMANTIC", ""),
            addressing=svc_attrs.get("ADDRESSING", ""),
            request=request,
            posResponses=pos_responses,
            negResponses=neg_responses,
            attrs=svc_attrs,
        )
    )
