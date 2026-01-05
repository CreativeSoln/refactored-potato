def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:

    layer_short = get_text_local(layer_el, "SHORT-NAME")

    # ---------------------------------------------------------
    # STRUCTURES + DOPs (required by parse_param)
    # ---------------------------------------------------------
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
                "structParams": [],
            })
        if tid:
            table_by_id[tid] = {"rows": rows}

    # ---------------------------------------------------------
    # UNITS / COMPU / DTC
    # ---------------------------------------------------------
    units = [self._parse_unit(u) for u in findall_descendants(layer_el, "UNIT")]
    compu_methods = [self._parse_compu_method(c) for c in findall_descendants(layer_el, "COMPU-METHOD")]
    dtcs = [self._parse_dtc(d) for d in findall_descendants(layer_el, "DTC")]

    # ---------------------------------------------------------
    # BUILD MESSAGE MAPS
    # ---------------------------------------------------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    def parse_params(scope_el, parentType, parentPath, svc_short):
        out: List[OdxParam] = []
        for p in findall_descendants(scope_el, "PARAM"):
            rp = self._try_parse_param(
                p,
                parentType,
                parentPath,
                layer_short,
                svc_short,
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
            if rp:
                out.append(rp)
        return out

    # REQUESTS
    for req in findall_descendants(layer_el, "REQUEST"):
        rid = get_attr(req, "ID")
        rshort = get_text_local(req, "SHORT-NAME")
        request_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(req, "LONG-NAME"),
            params=parse_params(req, "REQUEST", rshort, ""),
        )

    # POS RESPONSES
    for res in findall_descendants(layer_el, "POS-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        pos_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(res, "LONG-NAME"),
            params=parse_params(res, "POS_RESPONSE", rshort, ""),
        )

    # NEG RESPONSES
    for res in findall_descendants(layer_el, "NEG-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        neg_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(res, "LONG-NAME"),
            params=parse_params(res, "NEG_RESPONSE", rshort, ""),
        )

    # ---------------------------------------------------------
    # SERVICES
    # ---------------------------------------------------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_attrs = get_all_attrs(svc_el)
        svc_short = get_text_local(svc_el, "SHORT-NAME")

        request = None
        req_ref = find_child(svc_el, "REQUEST-REF")
        if req_ref:
            request = request_map.get(get_attr(req_ref, "ID-REF"))

        if request is None:
            inline_req = find_child(svc_el, "REQUEST")
            if inline_req is not None:
                rshort = get_text_local(inline_req, "SHORT-NAME") or f"{svc_short}_req"
                request = OdxMessage(
                    id=get_attr(inline_req, "ID"),
                    shortName=rshort,
                    longName=get_text_local(inline_req, "LONG-NAME"),
                    params=parse_params(inline_req, "REQUEST", rshort, svc_short),
                )

        pos_responses = [
            pos_resp_map[r]
            for r in [get_attr(x, "ID-REF") for x in find_children(svc_el, "POS-RESPONSE-REF")]
            if r in pos_resp_map
        ]

        neg_responses = [
            neg_resp_map[r]
            for r in [get_attr(x, "ID-REF") for x in find_children(svc_el, "NEG-RESPONSE-REF")]
            if r in neg_resp_map
        ]

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
    return OdxLayer(
        layerType=layerType,
        id=get_attr(layer_el, "ID", ""),
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
