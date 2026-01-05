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
        tsn = get_text_local(t, "SHORT-NAME")
        key_dop_ref = get_attr(find_child(t, "KEY-DOP-REF"), "ID-REF")
        rows = []

        for tr in findall_descendants(t, "TABLE-ROW"):
            struct_params = []
            struct_ref = find_child(tr, "STRUCTURE-REF")
            if struct_ref is not None:
                rid = get_attr(struct_ref, "ID-REF")
                rsn = get_text_local(struct_ref, "SHORT-NAME")
                if rid and rid in struct_by_id:
                    struct_params = struct_by_id[rid]
                elif rsn and rsn in struct_by_sn:
                    struct_params = struct_by_sn[rsn]

            rows.append({
                "id": get_attr(tr, "ID"),
                "shortName": get_text_local(tr, "SHORT-NAME"),
                "longName": get_text_local(tr, "LONG-NAME"),
                "desc": get_text_local(tr, "DESC"),
                "key": get_text_local(tr, "KEY"),
                "structParams": struct_params,
            })

        if tid:
            table_by_id[tid] = {
                "shortName": tsn,
                "keyDopRefId": key_dop_ref,
                "rows": rows,
            }

    # ---------------- STANDALONE MESSAGES ----------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    def _collect_params(msg_el, parent_type, parent_path):
        params: List[OdxParam] = []
        param_blocks = (
            find_children(msg_el, "PARAM")
            or findall_descendants(msg_el, "PARAM")
        )
        for p_el in param_blocks:
            rp = self._try_parse_param(
                p_el,
                parent_type,
                parent_path,
                layer_short,
                "",
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
            if rp:
                params.append(rp)
        return params

    for req in findall_descendants(layer_el, "REQUEST"):
        rid = get_attr(req, "ID")
        sn = get_text_local(req, "SHORT-NAME")
        request_map[rid] = OdxMessage(
            id=rid,
            shortName=sn,
            longName=get_text_local(req, "LONG-NAME"),
            params=_collect_params(req, "REQUEST", sn),
        )

    for res in findall_descendants(layer_el, "POS-RESPONSE"):
        rid = get_attr(res, "ID")
        sn = get_text_local(res, "SHORT-NAME")
        pos_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=sn,
            longName=get_text_local(res, "LONG-NAME"),
            params=_collect_params(res, "POS_RESPONSE", sn),
        )

    for res in findall_descendants(layer_el, "NEG-RESPONSE"):
        rid = get_attr(res, "ID")
        sn = get_text_local(res, "SHORT-NAME")
        neg_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=sn,
            longName=get_text_local(res, "LONG-NAME"),
            params=_collect_params(res, "NEG_RESPONSE", sn),
        )

    # ---------------- SERVICES ----------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_attrs = get_all_attrs(svc_el)
        svc_short = get_text_local(svc_el, "SHORT-NAME")

        # ---------- REQUEST ----------
        request: Optional[OdxMessage] = None

        request_ref = find_child(svc_el, "REQUEST-REF")
        request_ref_id = get_attr(request_ref, "ID-REF") if request_ref is not None else ""
        inline_req = find_child(svc_el, "REQUEST")

        if request_ref_id and request_ref_id in request_map:
            request = self._clone_message(request_map[request_ref_id])
            prefix = f"{svc_short}.{request.shortName or 'Request'}"
            self._prefix_path(request.params, prefix)
            self._annotate_service_name(request.params, svc_short)

        elif inline_req is not None:
            rshort = get_text_local(inline_req, "SHORT-NAME") or f"{svc_short}_req"
            root_path = f"{svc_short}.{rshort}"
            rparams = _collect_params(inline_req, "REQUEST", root_path)
            self._annotate_service_name(rparams, svc_short)
            request = OdxMessage(
                id=get_attr(inline_req, "ID"),
                shortName=rshort,
                longName=get_text_local(inline_req, "LONG-NAME"),
                params=rparams,
            )

        # ---------- POS RESPONSES ----------
        pos_responses: List[OdxMessage] = []

        for ref in find_children(svc_el, "POS-RESPONSE-REF"):
            rid = get_attr(ref, "ID-REF")
            if rid and rid in pos_resp_map:
                rr = self._clone_message(pos_resp_map[rid])
                prefix = f"{svc_short}.{rr.shortName or 'PosResponse'}"
                self._prefix_path(rr.params, prefix)
                self._annotate_service_name(rr.params, svc_short)
                pos_responses.append(rr)

        for el in find_children(svc_el, "POS-RESPONSE"):
            rshort = get_text_local(el, "SHORT-NAME") or f"{svc_short}_pos"
            root_path = f"{svc_short}.{rshort}"
            rparams = _collect_params(el, "POS_RESPONSE", root_path)
            self._annotate_service_name(rparams, svc_short)
            pos_responses.append(
                OdxMessage(
                    id=get_attr(el, "ID"),
                    shortName=rshort,
                    longName=get_text_local(el, "LONG-NAME"),
                    params=rparams,
                )
            )

        # ---------- NEG RESPONSES ----------
        neg_responses: List[OdxMessage] = []

        for ref in find_children(svc_el, "NEG-RESPONSE-REF"):
            rid = get_attr(ref, "ID-REF")
            if rid and rid in neg_resp_map:
                rr = self._clone_message(neg_resp_map[rid])
                prefix = f"{svc_short}.{rr.shortName or 'NegResponse'}"
                self._prefix_path(rr.params, prefix)
                self._annotate_service_name(rr.params, svc_short)
                neg_responses.append(rr)

        for el in find_children(svc_el, "NEG-RESPONSE"):
            rshort = get_text_local(el, "SHORT-NAME") or f"{svc_short}_neg"
            root_path = f"{svc_short}.{rshort}"
            rparams = _collect_params(el, "NEG_RESPONSE", root_path)
            self._annotate_service_name(rparams, svc_short)
            neg_responses.append(
                OdxMessage(
                    id=get_attr(el, "ID"),
                    shortName=rshort,
                    longName=get_text_local(el, "LONG-NAME"),
                    params=rparams,
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

    return OdxLayer(
        layerType=layerType,
        id=get_attr(layer_el, "ID"),
        shortName=layer_short,
        longName=get_text_local(layer_el, "LONG-NAME"),
        description=get_text_local(layer_el, "DESC"),
        services=services,
        attrs=get_all_attrs(layer_el),
    )
