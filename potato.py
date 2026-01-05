def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
    layer_short = get_text_local(layer_el, "SHORT-NAME")

    # -------------------------------------------------
    # STRUCTURES / DOPs / TABLES
    # -------------------------------------------------
    struct_by_id, struct_by_sn = harvest_structures(layer_el)

    dop_by_id: Dict[str, OdxDataObjectProp] = {}
    dop_by_sn: Dict[str, OdxDataObjectProp] = {}
    dop_meta_by_id: Dict[str, Dict[str, str]] = {}

    for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
        dd, meta = self._parse_dop_with_struct_map(d, struct_by_id, struct_by_sn)
        dop_by_id[dd.id] = dd
        dop_meta_by_id[dd.id] = meta
        if dd.shortName:
            dop_by_sn[dd.shortName] = dd

    table_by_id: Dict[str, Dict] = {}
    for t in findall_descendants(layer_el, "TABLE"):
        tid = get_attr(t, "ID")
        rows = []
        for tr in findall_descendants(t, "TABLE-ROW"):
            rows.append({
                "id": get_attr(tr, "ID"),
                "shortName": get_text_local(tr, "SHORT-NAME"),
                "longName": get_text_local(tr, "LONG-NAME"),
                "desc": get_text_local(tr, "DESC"),
                "key": get_text_local(tr, "KEY"),
                "structParams": []
            })
        if tid:
            table_by_id[tid] = {"rows": rows}

    units = [self._parse_unit(u) for u in findall_descendants(layer_el, "UNIT")]
    compu_methods = [self._parse_compu_method(c) for c in findall_descendants(layer_el, "COMPU-METHOD")]
    dtcs = [self._parse_dtc(d) for d in findall_descendants(layer_el, "DTC")]

    # -------------------------------------------------
    # STANDALONE REQUEST / RESP MAPS
    # -------------------------------------------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    def _collect_params(owner_el, parent_type, root_path, svc_short):
        params: List[OdxParam] = []
        for p_el in findall_descendants(owner_el, "PARAM"):
            rp = self._try_parse_param(
                p_el, parent_type, root_path,
                layer_short, svc_short,
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            if rp:
                params.append(rp)
        return params

    for req in findall_descendants(layer_el, "REQUEST"):
        rid = get_attr(req, "ID")
        rshort = get_text_local(req, "SHORT-NAME")
        params = _collect_params(req, "REQUEST", rshort, "")
        request_map[rid] = OdxMessage(rid, rshort, get_text_local(req, "LONG-NAME"), params)

    for res in findall_descendants(layer_el, "POS-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        params = _collect_params(res, "POS_RESPONSE", rshort, "")
        pos_resp_map[rid] = OdxMessage(rid, rshort, get_text_local(res, "LONG-NAME"), params)

    for res in findall_descendants(layer_el, "NEG-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        params = _collect_params(res, "NEG_RESPONSE", rshort, "")
        neg_resp_map[rid] = OdxMessage(rid, rshort, get_text_local(res, "LONG-NAME"), params)

    # -------------------------------------------------
    # SERVICES
    # -------------------------------------------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_attrs = get_all_attrs(svc_el)
        svc_short = get_text_local(svc_el, "SHORT-NAME")

        pos_ref_ids = [get_attr(r, "ID-REF") for r in find_children(svc_el, "POS-RESPONSE-REF")]
        neg_ref_ids = [get_attr(r, "ID-REF") for r in find_children(svc_el, "NEG-RESPONSE-REF")]

        inline_req = find_child(svc_el, "REQUEST")
        inline_pos = find_children(svc_el, "POS-RESPONSE")
        inline_neg = find_children(svc_el, "NEG-RESPONSE")

        # ---------- REQUEST ----------
        request = None
        req_ref = find_child(svc_el, "REQUEST-REF")
        if req_ref:
            rid = get_attr(req_ref, "ID-REF")
            request = request_map.get(rid)

        if not request and inline_req is not None:
            rshort = get_text_local(inline_req, "SHORT-NAME") or f"{svc_short}_req"
            params = _collect_params(inline_req, "REQUEST", f"{svc_short}.{rshort}", svc_short)
            request = OdxMessage(get_attr(inline_req, "ID"), rshort, "", params)

        if request:
            self._prefix_path(request.params, svc_short)
            self._annotate_service_name(request.params, svc_short)

        # ---------- POS RESPONSES ----------
        pos_responses: List[OdxMessage] = []
        attached_pos: Set[str] = set()

        for rid in pos_ref_ids:
            rr = pos_resp_map.get(rid)
            if rr:
                msg = self._clone_message(rr)
                self._prefix_path(msg.params, f"{svc_short}.{msg.shortName}")
                self._annotate_service_name(msg.params, svc_short)
                pos_responses.append(msg)
                attached_pos.add(rid)

        for el in inline_pos:
            rshort = get_text_local(el, "SHORT-NAME") or f"{svc_short}_pos"
            params = _collect_params(el, "POS_RESPONSE", f"{svc_short}.{rshort}", svc_short)
            pos_responses.append(OdxMessage(get_attr(el, "ID"), rshort, "", params))

        # fallback by name
        for rid, msg in pos_resp_map.items():
            if rid in attached_pos:
                continue
            if svc_short.lower() in (msg.shortName or "").lower():
                m = self._clone_message(msg)
                self._prefix_path(m.params, f"{svc_short}.{m.shortName}")
                self._annotate_service_name(m.params, svc_short)
                pos_responses.append(m)

        # ---------- NEG RESPONSES ----------
        neg_responses: List[OdxMessage] = []
        attached_neg: Set[str] = set()

        for rid in neg_ref_ids:
            rr = neg_resp_map.get(rid)
            if rr:
                msg = self._clone_message(rr)
                self._prefix_path(msg.params, f"{svc_short}.{msg.shortName}")
                self._annotate_service_name(msg.params, svc_short)
                neg_responses.append(msg)
                attached_neg.add(rid)

        for el in inline_neg:
            rshort = get_text_local(el, "SHORT-NAME") or f"{svc_short}_neg"
            params = _collect_params(el, "NEG_RESPONSE", f"{svc_short}.{rshort}", svc_short)
            neg_responses.append(OdxMessage(get_attr(el, "ID"), rshort, "", params))

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

    # -------------------------------------------------
    # FINAL LAYER
    # -------------------------------------------------
    return OdxLayer(
        layerType=layerType,
        id=get_attr(layer_el, "ID"),
        shortName=layer_short,
        longName=get_text_local(layer_el, "LONG-NAME"),
        description=get_text_local(layer_el, "DESC"),
        parentId=get_attr(find_child(layer_el, "PARENT-REF"), "ID-REF"),
        rxId=get_text_local(layer_el, "RECEIVE-ID"),
        txId=get_text_local(layer_el, "TRANSMIT-ID"),
        services=services,
        units=units,
        compuMethods=compu_methods,
        dataObjectProps=list(dop_by_id.values()),
        dtcs=dtcs,
        attrs=get_all_attrs(layer_el),
        linkedLayerIds=self._collect_links(layer_el),
    )
