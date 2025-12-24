def flatten_parameter(
    param,
    db: Database,
    parent: str,
    service_name: str,
    struct_leaf_total=None,
    struct_depth=1,
    index_map=None,
    struct_hierarchy=None,
    struct_hierarchy_detail=None,
    structure_registry=None
):
    results = []

    if structure_registry is None:
        structure_registry = {}

    pname = getattr(param, "short_name", "UNKNOWN")
    norm = normalize_name(pname)

    if norm in SKIP_PARAMS:
        return results

    full_path = f"{parent}.{pname}" if parent else f"{service_name}.{pname}"

    # ------------------ Resolve DOP ------------------
    dop = None
    if getattr(param, "dop_ref", None):
        dop = safe_resolve(param.dop_ref, db)

    children = get_child_parameters_from_dop(dop)
    has_children = len(children) > 0

    para_type = "DIRECT_PA"
    if has_children:
        para_type = "STRUCT_PA"
    elif getattr(param, "table_ref", None) or getattr(param, "table_row_ref", None):
        para_type = "TABLEROW_PA"

    # =========================================================
    #          DIRECT LEAF PARAMETER SUPPORT
    # =========================================================
    if not has_children:
        scale, offset, unit = get_scale_offset_unit(dop)

        # bit length extraction fallback
        bitlen = 0
        try:
            if hasattr(dop, "diag_coded_type") and hasattr(dop.diag_coded_type, "bit_length"):
                bitlen = dop.diag_coded_type.bit_length
            elif hasattr(dop, "bit_length"):
                bitlen = dop.bit_length
        except:
            pass

        results.append({
            "FullPath": full_path,

            "serviceMeta": {
                "paraType": para_type,
                "structureKey": "",
                "parameterIndexInsideStructure": (
                    index_map.get(full_path, 0) if index_map else 0
                ),
                "arrayName": getattr(param, "short_name", ""),
                "topStruct": parent
            },

            "responseMapping": {
                "specificParaName": getattr(param, "short_name", ""),
                "ParaType": get_physical_type(dop),
                "Scale": scale,
                "Offset": offset,
                "Unit": unit
            },

            "bitLength": bitlen,
            "Description": getattr(param, "long_name", "") or getattr(param, "description", "")
        })

        return results

    # =========================================================
    #            STRUCTURE PARAMETER HANDLING
    # =========================================================
    if struct_hierarchy is None:
        struct_hierarchy = [service_name]

    if struct_hierarchy_detail is None:
        struct_hierarchy_detail = [{
            "shortName": service_name,
            "longName": ""
        }]

    current_struct_short = getattr(param, "short_name", "UNKNOWN")
    current_struct_long = getattr(param, "long_name", "") or getattr(param, "description", "")

    new_hierarchy = struct_hierarchy + [current_struct_short]
    new_hierarchy_detail = struct_hierarchy_detail + [{
        "shortName": current_struct_short,
        "longName": current_struct_long
    }]

    # -------- First pass collect all leafs --------
    temp = []
    for sub in children:
        temp.extend(
            flatten_parameter(
                sub,
                db,
                full_path,
                service_name,
                struct_leaf_total=None,
                struct_depth=struct_depth + 1,
                struct_hierarchy=new_hierarchy,
                struct_hierarchy_detail=new_hierarchy_detail,
                structure_registry=structure_registry
            )
        )

    struct_leaf_total = count_leaf_parameters(dop, db)

    structure_key = ".".join(new_hierarchy)

    structure_registry[structure_key] = {
        "parameterCountInsideStructure": struct_leaf_total,
        "structureLevelDepth": struct_depth + 1,
        "structureHierarchy": new_hierarchy,
        "structureHierarchyPath": structure_key,
        "structureHierarchyDetailed": new_hierarchy_detail
    }

    # -------- Index assignment --------
    index_map = {}
    for i, leaf in enumerate(temp, start=0):
        index_map[leaf.get("FullPath", "")] = i

    # -------- Second actual traversal --------
    for sub in children:
        results.extend(
            flatten_parameter(
                sub,
                db,
                full_path,
                service_name,
                struct_leaf_total=struct_leaf_total,
                struct_depth=struct_depth + 1,
                index_map=index_map,
                struct_hierarchy=new_hierarchy,
                struct_hierarchy_detail=new_hierarchy_detail,
                structure_registry=structure_registry
            )
        )

    return results


def convert_existing_groups_to_final_json(read_groups):
    ecu_map = {}

    for g in read_groups:

        ecu = g.get("ECUVariant") or g.get("ECU", "")

        base_variant_value = (
            g.get("BaseVariant")
            or g.get("autoBaseVariant")
            or g.get("baseVariant")
            or ""
        )

        if ecu not in ecu_map:
            ecu_map[ecu] = {
                "ecuVariant": ecu,
                "baseVariant": base_variant_value,
                "services": []
            }

        service_entry = {
            "service": g.get("ServiceName", ""),
            "did": g.get("DID", ""),
            "semantic": g.get("Semantic", ""),
            "description": g.get("Description", "")
        }

        # -------- TABLE ROW --------
        if g.get("tableName"):
            service_entry["selection"] = {
                "type": "tableRow",
                "table": {
                    "name": g["tableName"],
                    "rowFullXPath": g.get("tableRowFullXPath", "")
                }
            }

        # -------- STRUCTURE LEAF --------
        else:
            structure_list = []
            for p in g.get("Parameters", []):
                sm = p.get("serviceMeta", {})

                structure_list.append({
                    "path": p.get("FullPath", ""),
                    "arrayIndex": sm.get("parameterIndexInsideStructure", 0),
                    "arrayName": sm.get("arrayName", ""),
                    "topStruct": sm.get("topStruct", "")
                })

            service_entry["selection"] = {
                "type": "structureLeaf",
                "structure": structure_list
            }

        # -------- FINAL PARAMETERS --------
        final_params = []
        for p in g.get("Parameters", []):
            rm = p.get("responseMapping", {})
            sm = p.get("serviceMeta", {})

            factor = rm.get("Scale", 1)
            offset = rm.get("Offset", 0)

            final_params.append({
                "name": rm.get("specificParaName", ""),
                "path": p.get("FullPath", ""),
                "arrayIndex": sm.get("parameterIndexInsideStructure", 0),
                "dataType": rm.get("ParaType", ""),
                "bitlength": p.get("bitLength", 0),
                "endianness": "INTEL",
                "scaling": {
                    "category": "LINEAR" if rm.get("Scale") else "IDENTITY",
                    "factor": factor,
                    "offset": offset,
                    "unit": rm.get("Unit", "")
                },
                "description": p.get("Description", "")
            })

        service_entry["finalParameters"] = final_params
        ecu_map[ecu]["services"].append(service_entry)

    return list(ecu_map.values())


from error_handler import (
    logger,
    log_error,
    PARSER_HEALTH,
    ERROR_CONTROL,
    finalize_health_report
)


def run_parser(db):
    did_groups = []

    try:
        logger.info("ODX Parsing Started")

        for ecu in getattr(db, "ecus", []) or []:

            try:
                PARSER_HEALTH["totalECUsProcessed"] += 1

                for svc in getattr(ecu, "services", []) or []:
                    svc_name = getattr(svc, "short_name", "UNKNOWN")

                    try:
                        extract_normal_dids(ecu, svc, db, did_groups)
                        extract_tablekey_dids(ecu, db, did_groups)

                    except Exception as e:
                        log_error(
                            "recoverable",
                            f"ECU={ecu.short_name} Service={svc_name}",
                            "Service processing failed",
                            e
                        )

                        if ERROR_CONTROL["STOP_ON_FATAL_SERVICE_FAILURE"]:
                            raise

            except Exception as e:
                log_error(
                    "fatal",
                    f"ECU={getattr(ecu,'short_name','UNKNOWN')}",
                    "ECU processing failed",
                    e
                )

                if ERROR_CONTROL["STOP_ON_FATAL_SERVICE_FAILURE"]:
                    raise

    except Exception as e:
        log_error("fatal", "GLOBAL", "Unhandled top-level failure", e)

    finally:
        finalize_health_report()

    return did_groups
