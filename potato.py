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

    # ---------------- MESSAGE MAPS ----------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    # -------- REQUESTS --------
    for req in findall_descendants(layer_el, "REQUEST"):
        rid = get_attr(req, "ID")
        rshort = get_text_local(req, "SHORT-NAME")
        params: List[OdxParam] = []

        for p in findall_descendants(req, "PARAM"):
            rp = self._try_parse_param(
                p, "REQUEST", rshort,
                layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, {}
            )
            if rp:
                params.append(rp)

        request_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(req, "LONG-NAME"),
            params=params,
        )

    # -------- POS RESPONSES --------
    for res in findall_descendants(layer_el, "POS-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        params: List[OdxParam] = []

        for p in findall_descendants(res, "PARAM"):
            rp = self._try_parse_param(
                p, "POS_RESPONSE", rshort,
                layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, {}
            )
            if rp:
                params.append(rp)

        pos_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(res, "LONG-NAME"),
            params=params,
        )

    # -------- NEG RESPONSES --------
    for res in findall_descendants(layer_el, "NEG-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        params: List[OdxParam] = []

        for p in findall_descendants(res, "PARAM"):
            rp = self._try_parse_param(
                p, "NEG_RESPONSE", rshort,
                layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, {}
            )
            if rp:
                params.append(rp)

        neg_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=rshort,
            longName=get_text_local(res, "LONG-NAME"),
            params=params,
        )

    # ---------------- SERVICES ----------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_short = get_text_local(svc_el, "SHORT-NAME")
        svc_attrs = get_all_attrs(svc_el)

        # REQUEST
        request = None
        req_ref = find_child(svc_el, "REQUEST-REF")
        if req_ref:
            rid = get_attr(req_ref, "ID-REF")
            request = request_map.get(rid)
            if request:
                self._prefix_path(request.params, f"{svc_short}.{request.shortName}")
                self._annotate_service_name(request.params, svc_short)

        # POS RESPONSES
        pos_responses: List[OdxMessage] = []
        for ref in find_children(svc_el, "POS-RESPONSE-REF"):
            rid = get_attr(ref, "ID-REF")
            rr = pos_resp_map.get(rid)
            if rr:
                rr = self._clone_message(rr)
                self._prefix_path(rr.params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(rr.params, svc_short)
                pos_responses.append(rr)

        # NEG RESPONSES
        neg_responses: List[OdxMessage] = []
        for ref in find_children(svc_el, "NEG-RESPONSE-REF"):
            rid = get_attr(ref, "ID-REF")
            rr = neg_resp_map.get(rid)
            if rr:
                rr = self._clone_message(rr)
                self._prefix_path(rr.params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(rr.params, svc_short)
                neg_responses.append(rr)

        logger.warning(
            "[SERVICE CHECK] %s REQ=%s POS=%d NEG=%d",
            svc_short,
            "Y" if request else "N",
            len(pos_responses),
            len(neg_responses),
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
