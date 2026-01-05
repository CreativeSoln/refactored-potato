def _deep_copy_params(self, params: List[OdxParam]) -> List[OdxParam]:
    def clone(p: OdxParam) -> OdxParam:
        cp = OdxParam(**{
            k: getattr(p, k)
            for k in p.__dataclass_fields__
            if k != "children"
        })
        cp.children = [clone(c) for c in (p.children or [])]
        return cp

    return [clone(p) for p in (params or [])]

def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:

    layer_short = get_text_local(layer_el, "SHORT-NAME")

    # ---------------------------------------------------------
    # Pre-collect STRUCTURES / DOPs / TABLES (already correct)
    # ---------------------------------------------------------
    struct_by_id, struct_by_sn = harvest_structures(layer_el)

    dop_by_id = {}
    dop_by_sn = {}
    dop_meta_by_id = {}

    for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
        dd, meta = self._parse_dop_with_struct_map(d, struct_by_id, struct_by_sn)
        dop_by_id[dd.id] = dd
        dop_meta_by_id[dd.id] = meta
        if dd.shortName:
            dop_by_sn[dd.shortName] = dd

    table_by_id = {}
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
                    struct_by_id.get(get_attr(find_child(tr, "STRUCTURE-REF"), "ID-REF"), [])
                ),
            })
        table_by_id[tid] = {"rows": rows}

    # ---------------------------------------------------------
    # REQUEST / RESPONSE MAPS (BASE DEFINITIONS)
    # ---------------------------------------------------------
    request_map: Dict[str, OdxMessage] = {}
    pos_resp_map: Dict[str, OdxMessage] = {}
    neg_resp_map: Dict[str, OdxMessage] = {}

    for req in findall_descendants(layer_el, "REQUEST"):
        rid = get_attr(req, "ID")
        rshort = get_text_local(req, "SHORT-NAME")
        params = [
            self._try_parse_param(
                p, "REQUEST", rshort, layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            for p in findall_descendants(req, "PARAM")
        ]
        request_map[rid] = OdxMessage(rid, rshort, get_text_local(req, "LONG-NAME"), params)

    for res in findall_descendants(layer_el, "POS-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        params = [
            self._try_parse_param(
                p, "POS_RESPONSE", rshort, layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            for p in findall_descendants(res, "PARAM")
        ]
        pos_resp_map[rid] = OdxMessage(rid, rshort, get_text_local(res, "LONG-NAME"), params)

    for res in findall_descendants(layer_el, "NEG-RESPONSE"):
        rid = get_attr(res, "ID")
        rshort = get_text_local(res, "SHORT-NAME")
        params = [
            self._try_parse_param(
                p, "NEG_RESPONSE", rshort, layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, table_by_id
            )
            for p in findall_descendants(res, "PARAM")
        ]
        neg_resp_map[rid] = OdxMessage(rid, rshort, get_text_local(res, "LONG-NAME"), params)

    # ---------------------------------------------------------
    # SERVICES (SAFE, ISOLATED PARAMS)
    # ---------------------------------------------------------
    services: List[OdxService] = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_attrs = get_all_attrs(svc_el)
        svc_short = get_text_local(svc_el, "SHORT-NAME")

        # ---------- REQUEST ----------
        request = None
        req_ref = find_child(svc_el, "REQUEST-REF")
        if req_ref:
            rr = request_map.get(get_attr(req_ref, "ID-REF"))
            if rr:
                params = self._deep_copy_params(rr.params)
                self._prefix_path(params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(params, svc_short)
                request = OdxMessage(rr.id, rr.shortName, rr.longName, params)

        # ---------- POS RESPONSES ----------
        pos_responses: List[OdxMessage] = []
        for ref in find_children(svc_el, "POS-RESPONSE-REF"):
            rr = pos_resp_map.get(get_attr(ref, "ID-REF"))
            if rr:
                params = self._deep_copy_params(rr.params)
                self._prefix_path(params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(params, svc_short)
                pos_responses.append(
                    OdxMessage(rr.id, rr.shortName, rr.longName, params)
                )

        # ---------- NEG RESPONSES ----------
        neg_responses: List[OdxMessage] = []
        for ref in find_children(svc_el, "NEG-RESPONSE-REF"):
            rr = neg_resp_map.get(get_attr(ref, "ID-REF"))
            if rr:
                params = self._deep_copy_params(rr.params)
                self._prefix_path(params, f"{svc_short}.{rr.shortName}")
                self._annotate_service_name(params, svc_short)
                neg_responses.append(
                    OdxMessage(rr.id, rr.shortName, rr.longName, params)
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
        units=[self._parse_unit(u) for u in findall_descendants(layer_el, "UNIT")],
        compuMethods=[self._parse_compu_method(c) for c in findall_descendants(layer_el, "COMPU-METHOD")],
        dataObjectProps=list(dop_by_id.values()),
        dtcs=[self._parse_dtc(d) for d in findall_descendants(layer_el, "DTC")],
        attrs=get_all_attrs(layer_el),
        linkedLayerIds=self._collect_links(layer_el),
    )
