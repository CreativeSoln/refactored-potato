def parse_param(
    self,
    param_el: ET.Element,
    parentType: str,
    parentPath: str,
    layerName: str,
    serviceShortName: str,
    dop_by_id: Dict[str, OdxDataObjectProp],
    dop_by_sn: Dict[str, OdxDataObjectProp],
    dop_meta_by_id: Dict[str, Dict[str, str]],
    struct_by_id: Dict[str, List[ET.Element]],
    struct_by_sn: Dict[str, List[ET.Element]],
    table_by_id: Dict[str, Dict],
) -> OdxParam:

    attrs = get_all_attrs(param_el)

    shortName = get_text_local(param_el, "SHORT-NAME") or ""
    semantic = (
        attrs.get("SEMANTIC")
        or get_text_local(param_el, "SEMANTIC")
        or ""
    )

    codedConst = find_child(param_el, "CODED-CONST")
    physConst = find_child(param_el, "PHYS-CONST")
    diagCodedType = find_child(param_el, "DIAG-CODED-TYPE")
    physType = find_child(param_el, "PHYSICAL-TYPE")

    coded_value = extract_coded_value(codedConst) if codedConst is not None else ""
    if not coded_value:
        coded_value = extract_coded_value(param_el)

    pid = f"{layerName}::{serviceShortName}::{parentType}::{shortName}::{uuid.uuid4().hex[:8]}"

    p = OdxParam(
        id=pid,
        shortName=shortName,
        longName=get_text_local(param_el, "LONG-NAME"),
        description=get_text_local(param_el, "DESC"),
        semantic=semantic,

        bytePosition=get_text_local(param_el, "BYTE-POSITION"),
        bitPosition=get_text_local(param_el, "BIT-POSITION"),
        bitLength=get_text_local(diagCodedType, "BIT-LENGTH") if diagCodedType else "",
        minLength=get_text_local(diagCodedType, "MIN-LENGTH") if diagCodedType else "",
        maxLength=get_text_local(diagCodedType, "MAX-LENGTH") if diagCodedType else "",
        baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE") if diagCodedType else "",
        physicalBaseType=get_attr(physType, "BASE-DATA-TYPE") if physType else "",

        codedConstValue=coded_value,
        physConstValue=get_text_local(physConst, "V") if physConst else "",

        dopRefId=get_attr(find_child(param_el, "DOP-REF"), "ID-REF") or "",
        dopSnRefName=get_text_local(find_child(param_el, "DOP-SNREF"), "SHORT-NAME") or "",

        parentType=parentType,
        parentName=parentPath,
        layerName=layerName,
        serviceShortName=serviceShortName,
        attrs=attrs,
    )

    # -------------------------------------------------
    # Resolve DOP (NO SIDE EFFECTS)
    # -------------------------------------------------
    dop = None
    if p.dopRefId:
        dop = dop_by_id.get(p.dopRefId)
    if dop is None and p.dopSnRefName:
        dop = dop_by_sn.get(p.dopSnRefName)

    self._fill_from_dop_if_missing(p, dop, dop_meta_by_id)

    next_path = f"{parentPath}.{shortName}" if parentPath else shortName

    # =================================================
    # (1) INLINE <STRUCTURE> UNDER <PARAM>
    # =================================================
    inline_struct = find_child(param_el, "STRUCTURE")
    if inline_struct is not None:
        params_block = find_child(inline_struct, "PARAMS")
        struct_params = (
            find_children(params_block, "PARAM")
            if params_block is not None
            else find_children(inline_struct, "PARAM")
        )
        for child_el in struct_params:
            child = self._try_parse_param(
                child_el,
                "STRUCTURE",
                next_path,
                layerName,
                serviceShortName,
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
            if child:
                p.children.append(child)

    # =================================================
    # (2) TABLE-KEY EXPANSION
    # =================================================
    table_ref = find_child(param_el, "TABLE-REF")
    if table_ref is not None:
        tbl_id = get_attr(table_ref, "ID-REF")
        tbl = table_by_id.get(tbl_id)
        if tbl:
            for row in tbl.get("rows", []):
                row_label = row.get("shortName") or f"Row_{row.get('key', '')}"
                row_param = OdxParam(
                    id=f"{pid}::{row_label}",
                    shortName=row_label,
                    semantic="TABLE-ROW",
                    parentType="TABLE-KEY",
                    parentName=next_path,
                    layerName=layerName,
                    serviceShortName=serviceShortName,
                    attrs={"TABLE-ROW-KEY": row.get("key", "")},
                )
                row_next = f"{next_path}.{row_label}"
                for child_el in row.get("structParams", []) or []:
                    child = self._try_parse_param(
                        child_el,
                        "STRUCTURE",
                        row_next,
                        layerName,
                        serviceShortName,
                        dop_by_id,
                        dop_by_sn,
                        dop_meta_by_id,
                        struct_by_id,
                        struct_by_sn,
                        table_by_id,
                    )
                    if child:
                        row_param.children.append(child)
                p.children.append(row_param)

    # =================================================
    # (3) DOP-OWNED STRUCTURE
    # =================================================
    if dop and getattr(dop, "structureParams", None):
        for child_el in dop.structureParams:
            child = self._try_parse_param(
                child_el,
                "STRUCTURE",
                next_path,
                layerName,
                serviceShortName,
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
            if child:
                p.children.append(child)

    # =================================================
    # (4) DOP-REF → STRUCTURE
    # =================================================
    struct_params = []
    if p.dopRefId and p.dopRefId in struct_by_id:
        struct_params = struct_by_id[p.dopRefId]
    elif p.dopSnRefName and p.dopSnRefName in struct_by_sn:
        struct_params = struct_by_sn[p.dopSnRefName]

    for child_el in struct_params:
        child = self._try_parse_param(
            child_el,
            "STRUCTURE",
            next_path,
            layerName,
            serviceShortName,
            dop_by_id,
            dop_by_sn,
            dop_meta_by_id,
            struct_by_id,
            struct_by_sn,
            table_by_id,
        )
        if child:
            p.children.append(child)

    # =================================================
    # (5) DIRECT STRUCTURE-REF
    # =================================================
    struct_ref = find_child(param_el, "STRUCTURE-REF")
    if struct_ref is not None:
        ref_id = get_attr(struct_ref, "ID-REF")
        ref_sn = get_text_local(struct_ref, "SHORT-NAME")
        struct_params = (
            struct_by_id.get(ref_id)
            or struct_by_sn.get(ref_sn)
            or []
        )
        for child_el in struct_params:
            child = self._try_parse_param(
                child_el,
                "STRUCTURE",
                next_path,
                layerName,
                serviceShortName,
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
            if child:
                p.children.append(child)

    return p
