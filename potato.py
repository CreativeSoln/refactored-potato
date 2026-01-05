def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
    layer_short = get_text_local(layer_el, "SHORT-NAME")

    # ---------------------------------------------------------
    # Harvest STRUCTURES + DOPs (same as working code)
    # ---------------------------------------------------------
    struct_by_id, struct_by_sn = harvest_structures(layer_el)

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
    # TABLES
    # ---------------------------------------------------------
    table_by_id: Dict[str, Dict] = {}
    for t in findall_descendants(layer_el, "TABLE"):
        tid = get_attr(t, "ID")
        rows = []
        for tr in findall_descendants(t, "TABLE-ROW"):
            struct_params = []
            sref = find_child(tr, "STRUCTURE-REF")
            if sref is not None:
                ref_id = get_attr(sref, "ID-REF")
                ref_sn = get_text_local(sref, "SHORT-NAME")
                struct_params = (
                    struct_by_id.get(ref_id)
                    or struct_by_sn.get(ref_sn)
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

    # ---------------------------------------------------------
    # Units / Compu / DTC
    # ---------------------------------------------------------
    units = [self._parse_unit(u) for u in findall_descendants(layer_el, "UNIT")]
    compu_methods = [self._parse_compu_method(c) for c in findall_descendants(layer_el, "COMPU-METHOD")]
    dtcs = [self._parse_dtc(d) for d in findall_descendants(layer_el, "DTC")]

    # ---------------------------------------------------------
    # Message maps (REQUEST / POS / NEG)
    # ---------------------------------------------------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    def _collect_params(msg_el, parent_type, base_path, svc_short):
        params: List[OdxParam] = []
        for p_el in findall_descendants(msg_el, "PARAM"):
            rp = self._try_parse_param(
                p_el,
                parent_type,
                base_path,
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

    # Standalone REQUEST
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

    # Standalone POS-RESPONSE
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

    # Standalone NEG-RESPONSE
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
    # SERVICES
    # ---------------------------------------------------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_attrs = get_all_attrs(svc_el)
        svc_short = get_text_local(svc_el, "SHORT-NAME")

        request = None
        req_ref = find_child(svc_el, "REQUEST-REF")
        if req_ref is not None:
            request = request_map.get(get_attr(req_ref, "ID-REF"))

        inline_req = find_child(svc_el, "REQUEST")
        if request is None and inline_req is not None:
            params = _collect_params(inline_req, "REQUEST", svc_short, svc_short)
            request = OdxMessage(
                id=get_attr(inline_req, "ID"),
                shortName=get_text_local(inline_req, "SHORT-NAME"),
                longName=get_text_local(inline_req, "LONG-NAME"),
                params=params,
            )

        pos_responses: List[OdxMessage] = []
        for ref in find_children(svc_el, "POS-RESPONSE-REF"):
            rr = pos_resp_map.get(get_attr(ref, "ID-REF"))
            if rr:
                rr2 = self._clone_message(rr)
                self._prefix_path(rr2.params, f"{svc_short}.{rr2.shortName}")
                self._annotate_service_name(rr2.params, svc_short)
                pos_responses.append(rr2)

        for el in find_children(svc_el, "POS-RESPONSE"):
            rshort = get_text_local(el, "SHORT-NAME")
            params = _collect_params(el, "POS_RESPONSE", f"{svc_short}.{rshort}", svc_short)
            pos_responses.append(
                OdxMessage(
                    id=get_attr(el, "ID"),
                    shortName=rshort,
                    longName=get_text_local(el, "LONG-NAME"),
                    params=params,
                )
            )

        neg_responses: List[OdxMessage] = []
        for ref in find_children(svc_el, "NEG-RESPONSE-REF"):
            rr = neg_resp_map.get(get_attr(ref, "ID-REF"))
            if rr:
                rr2 = self._clone_message(rr)
                self._prefix_path(rr2.params, f"{svc_short}.{rr2.shortName}")
                self._annotate_service_name(rr2.params, svc_short)
                neg_responses.append(rr2)

        for el in find_children(svc_el, "NEG-RESPONSE"):
            rshort = get_text_local(el, "SHORT-NAME")
            params = _collect_params(el, "NEG_RESPONSE", f"{svc_short}.{rshort}", svc_short)
            neg_responses.append(
                OdxMessage(
                    id=get_attr(el, "ID"),
                    shortName=rshort,
                    longName=get_text_local(el, "LONG-NAME"),
                    params=params,
                )
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

    # ---------------------------------------------------------
    # FINAL LAYER
    # ---------------------------------------------------------
    parent_ref = find_child(layer_el, "PARENT-REF")

    return OdxLayer(
        layerType=layerType,
        id=get_attr(layer_el, "ID"),
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
