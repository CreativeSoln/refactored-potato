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
