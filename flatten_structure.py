from typing import List, Dict, Any
from odxtools.database import Database

from odx_utils import (
    normalize_name,
    safe_resolve,
    get_child_parameters_from_dop,
    get_physical_type,
    get_scale_offset_unit
)

# =========================
# GLOBAL GROUP INDEX
# =========================
# Used for ECUs where OEM did NOT define structure,
# but logically parameters form indexed arrays (Whl0..Whl3)
GROUP_INDEX: Dict[str, int] = {}

# Parameters to skip
SKIP_PARAMS = {
    "SID", "SID_RQ", "SID_PR",
    "SERVICEID", "REQUESTSERVICEID", "RESPONSESERVICEID",

    "DID", "DATAIDENTIFIER", "DATA_IDENTIFIER",
    "RECORDDATAIDENTIFIER", "RECORD_DATA_IDENTIFIER",

    "RID", "ROUTINEIDENTIFIER", "ROUTINE_IDENTIFIER",
    "SUBFUNCTION", "SF"
}


def flatten_parameter(
    param,
    db: Database,
    parent: str,
    service_name: str,
    struct_depth=1,
    struct_hierarchy=None,
    struct_hierarchy_detail=None,
    structure_registry=None
):
    """
    Universal parameter flattener.
    Handles:
      - Simple (leaf) parameters
      - Structured parameters
      - Nested structures
      - Table parameters
    """

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

    # =====================================================================
    #                LEAF PARAMETER
    # =====================================================================
    if not has_children:
        scale, offset, unit = get_scale_offset_unit(dop)

        # -----------------------------
        # ARRAY INDEX MANAGEMENT
        # -----------------------------
        parent_key = parent or service_name

        global GROUP_INDEX
        if parent_key not in GROUP_INDEX:
            GROUP_INDEX[parent_key] = 0
        else:
            GROUP_INDEX[parent_key] += 1

        array_index = GROUP_INDEX[parent_key]

        # -----------------------------
        # Bit length extraction
        # -----------------------------
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
                "parameterIndexInsideStructure": array_index,
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

    # =====================================================================
    #                STRUCTURE PARAMETER
    # =====================================================================
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
        "LongName": current_struct_long
    }]

    # -------- Collect all leaves --------
    temp = []
    for sub in children:
        temp.extend(
            flatten_parameter(
                sub,
                db,
                full_path,
                service_name,
                struct_depth=struct_depth + 1,
                struct_hierarchy=new_hierarchy,
                struct_hierarchy_detail=new_hierarchy_detail,
                structure_registry=structure_registry
            )
        )

    # -------- Assign Index deterministically --------
    for i, leaf in enumerate(temp):
        sm = leaf.setdefault("serviceMeta", {})
        sm["parameterIndexInsideStructure"] = i

    # -------- Register structure metadata --------
    structure_key = ".".join(new_hierarchy)

    structure_registry[structure_key] = {
        "parameterCountInsideStructure": len(temp),
        "structureLevelDepth": struct_depth + 1,
        "structureHierarchy": new_hierarchy,
        "structureHierarchyPath": structure_key,
        "structureHierarchyDetailed": new_hierarchy_detail
    }

    return temp
