def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
    layer_short = get_text_local(layer_el, "SHORT-NAME")

    # ---------------------------------------------------------
    # STRUCTURES
    # ---------------------------------------------------------
    struct_by_id, struct_by_sn = harvest_structures(layer_el)

    # ---------------------------------------------------------
    # DOPs + meta
    # ---------------------------------------------------------
    dop_by_id: Dict[str, OdxDataObjectProp] = {}
    dop_by_sn: Dict[str, OdxDataObjectProp] = {}
    dop_meta_by_id: Dict[str, Dict[str, str]] = {}

    for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
        dd, meta = self._parse_dop_with_struct_map(d, struct_by_id, struct_by_sn)
        if dd.id:
            dop_by_id[dd.id] = dd
            dop_meta_by_id[dd.id] = meta
        if dd.shortName:
            dop_by_sn[dd.shortName] = dd

    # ---------------------------------------------------------
    # TABLES (for TABLE-KEY)
    # ---------------------------------------------------------
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
                "structParams": (
                    struct_by_id.get(get_attr(find_child(tr, "STRUCTURE-REF"), "ID-REF"))
                    or struct_by_sn.get(get_text_local(find_child(tr, "STRUCTURE-REF"), "SHORT-NAME"))
                    or []
                ),
            })
        if tid:
            table_by_id[tid] = {
                "shortName": get_text_local(t, "SHORT-NAME"),
                "rows": rows,
            }

    # ---------------------------------------------------------
    # UNITS / COMPU / DTC
    # ---------------------------------------------------------
    units = [self._parse_unit(u) for u in findall_descendants(layer_el, "UNIT")]
    compu_methods = [self._parse_compu_method(c) for c in findall_descendants(layer_el, "COMPU-METHOD")]
    dtcs = [self._parse_dtc(d) for d in findall_descendants(layer_el, "DTC")]

    # ---------------------------------------------------------
    # STANDALONE MESSAGE MAPS
    # ---------------------------------------------------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    def _collect_params(el, ptype, root_path, svc_short):
        out: List[OdxParam] = []
        for p_el in findall_descendants(el, "PARAM"):
            rp = self._try_parse_param(
                p_el, ptype, root_path, layer_short, svc_short,
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            if rp:
                out.append(rp)
        return out

    # REQUEST
    for req in findall_descendants(layer_el, "REQUEST"):
        rid = get_attr(req, "ID")
        rshort = get_text_local(req, "SHORT-NAME")
        params = _collect_params(req, "REQUEST", rshort, "")
        request_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(req, "LONG-NAME"),
            params=params,
        )

    # POS-RESPONSE
    for res in findall_descendants(layer_el, "POS-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        params = _collect_params(res, "POS_RESPONSE", rshort, "")
        pos_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(res, "LONG-NAME"),
            params=params,
        )

    # NEG-RESPONSE
    for res in findall_descendants(layer_el, "NEG-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        params = _collect_params(res, "NEG_RESPONSE", rshort, "")
        neg_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(res, "LONG-NAME"),
            params=params,
        )

    # ---------------------------------------------------------
    # SERVICES (inline + references + fallback)
    # ---------------------------------------------------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_attrs = get_all_attrs(svc_el)
        svc_short = get_text_local(svc_el, "SHORT-NAME")
        svc_key = svc_short.lower()

        # ---------------- REQUEST ----------------
        request = None
        req_ref = find_child(svc_el, "REQUEST-REF")
        if req_ref:
            request = request_map.get(get_attr(req_ref, "ID-REF"))

        if request is None:
            inline_req = find_child(svc_el, "REQUEST")
            if inline_req is not None:
                rshort = get_text_local(inline_req, "SHORT-NAME") or f"{svc_short}_req"
                params = _collect_params(inline_req, "REQUEST", f"{svc_short}.{rshort}", svc_short)
                request = OdxMessage(
                    get_attr(inline_req, "ID"),
                    rshort,
                    get_text_local(inline_req, "LONG-NAME"),
                    params,
                )

        if request is None:
            for msg in request_map.values():
                if svc_key and svc_key in (msg.shortName or "").lower():
                    request = self._clone_message(msg)
                    break

        if request:
            self._prefix_path(request.params, svc_short)
            self._annotate_service_name(request.params, svc_short)

        # ---------------- POS RESPONSES ----------------
        pos_responses: List[OdxMessage] = []
        attached_pos: Set[str] = set()

        for ref in find_children(svc_el, "POS-RESPONSE-REF"):
            rid = get_attr(ref, "ID-REF")
            if rid in pos_resp_map:
                m = self._clone_message(pos_resp_map[rid])
                self._prefix_path(m.params, f"{svc_short}.{m.shortName}")
                self._annotate_service_name(m.params, svc_short)
                pos_responses.append(m)
                attached_pos.add(rid)

        for rid, msg in pos_resp_map.items():
            if rid in attached_pos:
                continue
            if svc_key and svc_key in (msg.shortName or "").lower():
                m = self._clone_message(msg)
                self._prefix_path(m.params, f"{svc_short}.{m.shortName}")
                self._annotate_service_name(m.params, svc_short)
                pos_responses.append(m)

        # ---------------- NEG RESPONSES ----------------
        neg_responses: List[OdxMessage] = []
        attached_neg: Set[str] = set()

        for ref in find_children(svc_el, "NEG-RESPONSE-REF"):
            rid = get_attr(ref, "ID-REF")
            if rid in neg_resp_map:
                m = self._clone_message(neg_resp_map[rid])
                self._prefix_path(m.params, f"{svc_short}.{m.shortName}")
                self._annotate_service_name(m.params, svc_short)
                neg_responses.append(m)
                attached_neg.add(rid)

        for rid, msg in neg_resp_map.items():
            if rid in attached_neg:
                continue
            if svc_key and svc_key in (msg.shortName or "").lower():
                m = self._clone_message(msg)
                self._prefix_path(m.params, f"{svc_short}.{m.shortName}")
                self._annotate_service_name(m.params, svc_short)
                neg_responses.append(m)

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

    # ---------------------------------------------------------
    # FINAL LAYER
    # ---------------------------------------------------------
    parent_ref = find_child(layer_el, "PARENT-REF")

    return OdxLayer(
        layerType=layerType,
        id=get_attr(layer_el, "ID", ""),
        shortName=layer_short,
        longName=get_text_local(layer_el, "LONG-NAME"),
        description=get_text_local(layer_el, "DESC"),
        parentId=get_attr(parent_ref, "ID-REF") if parent_ref is not None else "",
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
