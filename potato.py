def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
    layer_short = get_text_local(layer_el, "SHORT-NAME")

    # ---------------------------------------------------------
    # Harvest structures first (CRITICAL for structure params)
    # ---------------------------------------------------------
    struct_by_id, struct_by_sn = harvest_structures(layer_el)

    # ---------------------------------------------------------
    # Parse DOPs + meta
    # ---------------------------------------------------------
    dop_by_id: Dict[str, OdxDataObjectProp] = {}
    dop_by_sn: Dict[str, OdxDataObjectProp] = {}
    dop_meta_by_id: Dict[str, Dict[str, str]] = {}

    for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
        dd, meta = self._parse_dop_with_struct_map(d, struct_by_id, struct_by_sn)
        dop_by_id[dd.id] = dd
        dop_meta_by_id[dd.id] = meta
        if dd.shortName:
            dop_by_sn[dd.shortName] = dd

    # ---------------------------------------------------------
    # Tables (TABLE-KEY expansion)
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
                "structParams": [],
            })
        if tid:
            table_by_id[tid] = {"rows": rows}

    # ---------------------------------------------------------
    # Units / Compu / DTC
    # ---------------------------------------------------------
    units = [self._parse_unit(u) for u in findall_descendants(layer_el, "UNIT")]
    compu_methods = [self._parse_compu_method(c) for c in findall_descendants(layer_el, "COMPU-METHOD")]
    dtcs = [self._parse_dtc(d) for d in findall_descendants(layer_el, "DTC")]

    # ---------------------------------------------------------
    # REQUEST / POS / NEG message maps (NO CLONING)
    # ---------------------------------------------------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    def parse_message(msg_el: ET.Element, kind: str, svc_sn: str) -> OdxMessage:
        mid = get_attr(msg_el, "ID")
        mshort = get_text_local(msg_el, "SHORT-NAME")
        root_path = f"{svc_sn}.{mshort}" if svc_sn else mshort
        params: List[OdxParam] = []

        for p_el in findall_descendants(msg_el, "PARAM"):
            p = self._try_parse_param(
                p_el,
                kind,
                root_path,
                layer_short,
                svc_sn,
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
            if p:
                params.append(p)

        self._annotate_service_name(params, svc_sn)
        return OdxMessage(
            id=mid,
            shortName=mshort,
            longName=get_text_local(msg_el, "LONG-NAME"),
            params=params,
        )

    for req in findall_descendants(layer_el, "REQUEST"):
        request_map[get_attr(req, "ID")] = parse_message(req, "REQUEST", "")

    for res in findall_descendants(layer_el, "POS-RESPONSE"):
        pos_resp_map[get_attr(res, "ID")] = parse_message(res, "POS_RESPONSE", "")

    for res in findall_descendants(layer_el, "NEG-RESPONSE"):
        neg_resp_map[get_attr(res, "ID")] = parse_message(res, "NEG_RESPONSE", "")

    # ---------------------------------------------------------
    # SERVICES (attach messages directly, NO cloning)
    # ---------------------------------------------------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_attrs = get_all_attrs(svc_el)
        svc_short = get_text_local(svc_el, "SHORT-NAME")

        # REQUEST
        request = None
        req_ref = find_child(svc_el, "REQUEST-REF")
        if req_ref is not None:
            request = request_map.get(get_attr(req_ref, "ID-REF"))
        else:
            inline_req = find_child(svc_el, "REQUEST")
            if inline_req is not None:
                request = parse_message(inline_req, "REQUEST", svc_short)

        # POS RESPONSES
        pos_responses: List[OdxMessage] = []
        for ref in find_children(svc_el, "POS-RESPONSE-REF"):
            rr = pos_resp_map.get(get_attr(ref, "ID-REF"))
            if rr:
                self._prefix_path(rr.params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(rr.params, svc_short)
                pos_responses.append(rr)

        for el in find_children(svc_el, "POS-RESPONSE"):
            pos_responses.append(parse_message(el, "POS_RESPONSE", svc_short))

        # NEG RESPONSES
        neg_responses: List[OdxMessage] = []
        for ref in find_children(svc_el, "NEG-RESPONSE-REF"):
            rr = neg_resp_map.get(get_attr(ref, "ID-REF"))
            if rr:
                self._prefix_path(rr.params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(rr.params, svc_short)
                neg_responses.append(rr)

        for el in find_children(svc_el, "NEG-RESPONSE"):
            neg_responses.append(parse_message(el, "NEG_RESPONSE", svc_short))

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
    # Final layer
    # ---------------------------------------------------------
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
