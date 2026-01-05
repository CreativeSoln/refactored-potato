def _deep_copy_param(self, p: OdxParam) -> OdxParam:
    cp = OdxParam(
        id=p.id,
        shortName=p.shortName,
        longName=p.longName,
        description=p.description,
        semantic=p.semantic,

        bytePosition=p.bytePosition,
        bitPosition=p.bitPosition,
        bitLength=p.bitLength,
        minLength=p.minLength,
        maxLength=p.maxLength,

        baseDataType=p.baseDataType,
        physicalBaseType=p.physicalBaseType,

        value=p.value,
        rawHex=p.rawHex,
        displayHex=p.displayHex,
        requestDidHex=p.requestDidHex,

        codedConstValue=p.codedConstValue,
        physConstValue=p.physConstValue,

        parentType=p.parentType,
        parentName=p.parentName,
        layerName=p.layerName,
        serviceShortName=p.serviceShortName,

        attrs=dict(p.attrs),
        children=[],
    )

    for c in p.children:
        cp.children.append(self._deep_copy_param(c))

    return cp


def _clone_message(self, src: OdxMessage) -> OdxMessage:
    return OdxMessage(
        id=src.id,
        shortName=src.shortName,
        longName=src.longName,
        params=[self._deep_copy_param(p) for p in (src.params or [])],
    )

def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
    layer_short = get_text_local(layer_el, "SHORT-NAME")

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

    # ------------------ build standalone message maps ------------------
    request_map = {}
    pos_resp_map = {}
    neg_resp_map = {}

    def collect_params(el, ptype, root):
        out = []
        for p in find_children(el, "PARAM"):   # 🔴 DIRECT ONLY
            cp = self._try_parse_param(
                p, ptype, root, layer_short, "",
                dop_by_id, dop_by_sn, dop_meta_by_id,
                struct_by_id, struct_by_sn, {}
            )
            if cp:
                out.append(cp)
        return out

    for el in findall_descendants(layer_el, "REQUEST"):
        rid = get_attr(el, "ID")
        sn = get_text_local(el, "SHORT-NAME")
        request_map[rid] = OdxMessage(
            id=rid,
            shortName=sn,
            longName=get_text_local(el, "LONG-NAME"),
            params=collect_params(el, "REQUEST", sn),
        )

    for el in findall_descendants(layer_el, "POS-RESPONSE"):
        rid = get_attr(el, "ID")
        sn = get_text_local(el, "SHORT-NAME")
        pos_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=sn,
            longName=get_text_local(el, "LONG-NAME"),
            params=collect_params(el, "POS_RESPONSE", sn),
        )

    for el in findall_descendants(layer_el, "NEG-RESPONSE"):
        rid = get_attr(el, "ID")
        sn = get_text_local(el, "SHORT-NAME")
        neg_resp_map[rid] = OdxMessage(
            id=rid,
            shortName=sn,
            longName=get_text_local(el, "LONG-NAME"),
            params=collect_params(el, "NEG_RESPONSE", sn),
        )

    # ------------------ SERVICES ------------------
    services = []

    for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
        svc_short = get_text_local(svc_el, "SHORT-NAME")
        attrs = get_all_attrs(svc_el)

        # REQUEST
        req = None
        ref = find_child(svc_el, "REQUEST-REF")
        if ref:
            req = self._clone_message(request_map.get(get_attr(ref, "ID-REF")))
        else:
            inline = find_child(svc_el, "REQUEST")
            if inline:
                sn = get_text_local(inline, "SHORT-NAME") or svc_short + "_req"
                req = OdxMessage(
                    id=get_attr(inline, "ID"),
                    shortName=sn,
                    longName=get_text_local(inline, "LONG-NAME"),
                    params=collect_params(inline, "REQUEST", sn),
                )

        # POS
        pos = []
        for r in find_children(svc_el, "POS-RESPONSE-REF"):
            rr = pos_resp_map.get(get_attr(r, "ID-REF"))
            if rr:
                pos.append(self._clone_message(rr))
        for el in find_children(svc_el, "POS-RESPONSE"):
            sn = get_text_local(el, "SHORT-NAME") or svc_short + "_pos"
            pos.append(OdxMessage(
                id=get_attr(el, "ID"),
                shortName=sn,
                longName=get_text_local(el, "LONG-NAME"),
                params=collect_params(el, "POS_RESPONSE", sn),
            ))

        # NEG
        neg = []
        for r in find_children(svc_el, "NEG-RESPONSE-REF"):
            rr = neg_resp_map.get(get_attr(r, "ID-REF"))
            if rr:
                neg.append(self._clone_message(rr))
        for el in find_children(svc_el, "NEG-RESPONSE"):
            sn = get_text_local(el, "SHORT-NAME") or svc_short + "_neg"
            neg.append(OdxMessage(
                id=get_attr(el, "ID"),
                shortName=sn,
                longName=get_text_local(el, "LONG-NAME"),
                params=collect_params(el, "NEG_RESPONSE", sn),
            ))

        services.append(OdxService(
            id=attrs.get("ID", ""),
            shortName=svc_short,
            longName=get_text_local(svc_el, "LONG-NAME"),
            description=get_text_local(svc_el, "DESC"),
            semantic=attrs.get("SEMANTIC", ""),
            addressing=attrs.get("ADDRESSING", ""),
            request=req,
            posResponses=pos,
            negResponses=neg,
            attrs=attrs,
        ))

    return OdxLayer(
        layerType=layerType,
        id=get_attr(layer_el, "ID"),
        shortName=layer_short,
        longName=get_text_local(layer_el, "LONG-NAME"),
        description=get_text_local(layer_el, "DESC"),
        parentId="",
        rxId=get_text_local(layer_el, "RECEIVE-ID"),
        txId=get_text_local(layer_el, "TRANSMIT-ID"),
        services=services,
        units=[],
        compuMethods=[],
        dataObjectProps=list(dop_by_id.values()),
        dtcs=[],
        attrs=get_all_attrs(layer_el),
        linkedLayerIds=[],
    )
