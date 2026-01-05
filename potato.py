def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
    layer_short = get_text_local(layer_el, "SHORT-NAME")
    struct_by_id, struct_by_sn = harvest_structures(layer_el)

    # ---------------- DOPs ----------------
    dop_by_id: Dict[str, OdxDataObjectProp] = {}
    dop_by_sn: Dict[str, OdxDataObjectProp] = {}
    dop_meta_by_id: Dict[str, Dict[str, str]] = {}

    for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
        dd, meta = self._parse_dop_with_struct_map(d, struct_by_id, struct_by_sn)
        dop_by_id[dd.id] = dd
        dop_meta_by_id[dd.id] = meta
        if dd.shortName:
            dop_by_sn[dd.shortName] = dd

    # ---------------- TABLES ----------------
    table_by_id: Dict[str, Dict] = {}
    for t in findall_descendants(layer_el, "TABLE"):
        tid = get_attr(t, "ID")
        rows = []
        for tr in findall_descendants(t, "TABLE-ROW"):
            struct_params: List[ET.Element] = []
            sref = find_child(tr, "STRUCTURE-REF")
            if sref is not None:
                rid = get_attr(sref, "ID-REF")
                rsn = get_text_local(sref, "SHORT-NAME")
                struct_params = (
                    struct_by_id.get(rid)
                    or struct_by_sn.get(rsn)
                    or []
                )
            rows.append({
                "id": get_attr(tr, "ID"),
                "shortName": get_text_local(tr, "SHORT-NAME"),
                "longName": get_text_local(tr, "LONG-NAME"),
                "desc": get_text_local(tr, "DESC"),
                "key": get_text_local(tr, "KEY"),
                "structParams": struct_params,
            })
        if tid:
            table_by_id[tid] = {"rows": rows}

    # ---------------- MESSAGE MAPS ----------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    # -------- Standalone REQUEST ----------
    for req in findall_descendants(layer_el, "REQUEST"):
        rid = get_attr(req, "ID")
        rparams: List[OdxParam] = []
        for p_el in findall_descendants(req, "PARAM"):
            rp = self._try_parse_param(
                p_el, "REQUEST", get_text_local(req, "SHORT-NAME"),
                layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            if rp:
                rparams.append(rp)
        request_map[rid] = OdxMessage(
            id=rid,
            shortName=get_text_local(req, "SHORT-NAME"),
            longName=get_text_local(req, "LONG-NAME"),
            params=rparams,
        )

    # -------- Standalone POS RESPONSE ----------
    for res in findall_descendants(layer_el, "POS-RESPONSE"):
        rid = get_attr(res, "ID")
        rparams: List[OdxParam] = []
        for p_el in findall_descendants(res, "PARAM"):
            rp = self._try_parse_param(
                p_el, "POS_RESPONSE", get_text_local(res, "SHORT-NAME"),
                layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            if rp:
                rparams.append(rp)
        pos_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=get_text_local(res, "SHORT-NAME"),
            longName=get_text_local(res, "LONG-NAME"),
            params=rparams,
        )

    # -------- Standalone NEG RESPONSE ----------
    for res in findall_descendants(layer_el, "NEG-RESPONSE"):
        rid = get_attr(res, "ID")
        rparams: List[OdxParam] = []
        for p_el in findall_descendants(res, "PARAM"):
            rp = self._try_parse_param(
                p_el, "NEG_RESPONSE", get_text_local(res, "SHORT-NAME"),
                layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            if rp:
                rparams.append(rp)
        neg_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=get_text_local(res, "SHORT-NAME"),
            longName=get_text_local(res, "LONG-NAME"),
            params=rparams,
        )

    # ---------------- SERVICES ----------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_attrs = get_all_attrs(svc_el)
        svc_short = get_text_local(svc_el, "SHORT-NAME")

        # ---- REQUEST ----
        request = None
        req_ref = find_child(svc_el, "REQUEST-REF")
        if req_ref:
            rid = get_attr(req_ref, "ID-REF")
            request = request_map.get(rid)

        # ---- RESPONSE BINDINGS (FIX) ----
        pos_ref_ids = [
            get_attr(r, "ID-REF")
            for r in find_children(svc_el, "POS-RESPONSE-REF")
            if get_attr(r, "ID-REF")
        ]
        neg_ref_ids = [
            get_attr(r, "ID-REF")
            for r in find_children(svc_el, "NEG-RESPONSE-REF")
            if get_attr(r, "ID-REF")
        ]
        inline_pos = find_children(svc_el, "POS-RESPONSE")
        inline_neg = find_children(svc_el, "NEG-RESPONSE")

        # ---- POS RESPONSES ----
        pos_responses: List[OdxMessage] = []
        for rid in pos_ref_ids:
            rr = pos_resp_map.get(rid)
            if rr:
                rr = self._clone_message(rr)
                self._prefix_path(rr.params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(rr.params, svc_short)
                pos_responses.append(rr)

        for el in inline_pos:
            rparams: List[OdxParam] = []
            rshort = get_text_local(el, "SHORT-NAME")
            for p_el in findall_descendants(el, "PARAM"):
                rp = self._try_parse_param(
                    p_el, "POS_RESPONSE",
                    f"{svc_short}.{rshort}",
                    layer_short, svc_short,
                    dop_by_id, dop_by_sn, dop_meta_by_id,
                    struct_by_id, struct_by_sn, table_by_id
                )
                if rp:
                    rparams.append(rp)
            pos_responses.append(
                OdxMessage(
                    id=get_attr(el, "ID"),
                    shortName=rshort,
                    longName=get_text_local(el, "LONG-NAME"),
                    params=rparams,
                )
            )

        # ---- NEG RESPONSES ----
        neg_responses: List[OdxMessage] = []
        for rid in neg_ref_ids:
            rr = neg_resp_map.get(rid)
            if rr:
                rr = self._clone_message(rr)
                self._prefix_path(rr.params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(rr.params, svc_short)
                neg_responses.append(rr)

        for el in inline_neg:
            rparams: List[OdxParam] = []
            rshort = get_text_local(el, "SHORT-NAME")
            for p_el in findall_descendants(el, "PARAM"):
                rp = self._try_parse_param(
                    p_el, "NEG_RESPONSE",
                    f"{svc_short}.{rshort}",
                    layer_short, svc_short,
                    dop_by_id, dop_by_sn, dop_meta_by_id,
                    struct_by_id, struct_by_sn, table_by_id
                )
                if rp:
                    rparams.append(rp)
            neg_responses.append(
                OdxMessage(
                    id=get_attr(el, "ID"),
                    shortName=rshort,
                    longName=get_text_local(el, "LONG-NAME"),
                    params=rparams,
                )
            )

        logger.warning(
            "[SERVICE CHECK] %s POS=%d NEG=%d",
            svc_short, len(pos_responses), len(neg_responses)
        )

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

    return OdxLayer(
        layerType=layerType,
        id=get_attr(layer_el, "ID"),
        shortName=layer_short,
        longName=get_text_local(layer_el, "LONG-NAME"),
        description=get_text_local(layer_el, "DESC"),
        services=services,
        units=[],
        compuMethods=[],
        dataObjectProps=list(dop_by_id.values()),
        dtcs=[],
        attrs=get_all_attrs(layer_el),
        linkedLayerIds=self._collect_links(layer_el),
    )
