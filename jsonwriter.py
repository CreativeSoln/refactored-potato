from typing import Any, Dict, List
import logging
import re

import odxtools
from odxtools.database import Database
from odxtools.diaglayers.protocolraw import ProtocolRaw
from odxtools.odxlink import OdxLinkDatabase

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)

# ============================================================================
# PATCH to bypass COMPARAM failures safely
# ============================================================================

_real_resolve = OdxLinkDatabase.resolve
def _patched_resolve(self, ref, expected_type=None, *, use_weakrefs=None):
    try:
        return _real_resolve(self, ref, expected_type, use_weakrefs=use_weakrefs)
    except Exception as e:
        msg = str(e)
        if "COMPARAM" in msg or "ODXLINK" in msg or "ISO_15765" in msg:
            return None
        raise
OdxLinkDatabase.resolve = _patched_resolve


_real_snrefs = ProtocolRaw._resolve_snrefs
def _patched_snrefs(self, context):
    try:
        if getattr(self, "_comparan_spec", None) is None:
            return
        return _real_snrefs(self, context)
    except Exception as e:
        if "prot_stacks" in str(e):
            return
        raise
ProtocolRaw._resolve_snrefs = _patched_snrefs


# ============================================================================
# Utilities
# ============================================================================

def normalize_name(text: str) -> str:
    if not text:
        return ""
    text = text.replace(".", "_")
    return re.sub(r"[^A-Za-z0-9_]", "_", text).upper()


def auto_base_variant(v: str) -> str:
    v = normalize_name(v)
    return v.split("_")[0] if "_" in v else v


def safe_resolve(ref, db: Database):
    try:
        if not ref:
            return None
        return db.odxlinks.resolve_lenient(ref)
    except Exception:
        return None


def get_semantic(service) -> str:
    s = getattr(service, "semantic", None)
    if s:
        return s.strip()

    req = getattr(service, "request", None)
    if req and getattr(req, "semantic", None):
        return req.semantic.strip()

    pos = getattr(service, "positive_responses", None)
    if pos:
        for pr in pos:
            if getattr(pr, "semantic", None):
                return pr.semantic.strip()

    name = (getattr(service, "short_name", "") or "").upper()
    if "READ" in name or "RDBI" in name:
        return "CURRENTDATA"
    if "IDENT" in name:
        return "IDENTIFICATION"
    return ""


# ============================================================================
# Service ID Detector
# ============================================================================

def detect_service_sid(service) -> str:
    req = getattr(service, "request", None)
    if not req:
        return ""

    for p in getattr(req, "parameters", []) or []:
        nm = normalize_name(getattr(p, "short_name", ""))
        if nm in ["SID", "SID_RQ", "SERVICEID", "REQUESTSERVICEID"]:
            try:
                v = getattr(p, "coded_value", None)
                if v is None:
                    continue
                return f"0x{int(v):02X}"
            except:
                continue

    name = (getattr(service, "short_name", "") or "").upper()
    if "RDBI" in name or "READ" in name:
        return "0x22"
    if "WDBI" in name or "WRITE" in name:
        return "0x2E"
    return ""


# ============================================================================
# Physical datatype resolution
# ============================================================================

def get_physical_type(dop) -> str:
    try:
        if not dop:
            return ""
        paths = (
            ("diag_coded_type", "physical_type", "base_data_type", "name"),
            ("physical_type", "base_data_type", "name"),
            ("compu_method", "physical_type", "base_data_type", "name"),
        )
        for path in paths:
            obj = dop
            for attr in path:
                obj = getattr(obj, attr, None)
                if obj is None:
                    break
            if obj:
                return str(obj)
        return ""
    except Exception:
        return ""


# ============================================================================
# UNIVERSAL CHILD PARAMETER RESOLVER
# ============================================================================

def get_child_parameters_from_dop(dop):
    if dop is None:
        return []

    if getattr(dop, "structure", None):
        params = getattr(dop.structure, "parameters", None)
        if params:
            return params

    if hasattr(dop, "sub_elements"):
        return dop.sub_elements

    if hasattr(dop, "parameters"):
        return dop.parameters

    return []


# ============================================================================
# SCALE + OFFSET + UNIT Extractor
# ============================================================================

def get_scale_offset_unit(dop):
    try:
        if not dop or not hasattr(dop, "compu_method") or dop.compu_method is None:
            return None, None, None

        cm = dop.compu_method

        unit = None
        if hasattr(cm, "unit_ref") and cm.unit_ref:
            unit_obj = safe_resolve(cm.unit_ref, dop._database)
            if unit_obj:
                unit = getattr(unit_obj, "display_name", None) or getattr(unit_obj, "name", None)

        if hasattr(cm, "compu_scales") and cm.compu_scales:
            scale = getattr(cm.compu_scales[0], "factor", None)
            offset = getattr(cm.compu_scales[0], "offset", None)
            return scale, offset, unit

        return None, None, unit

    except Exception:
        return None, None, None

def count_leaf_parameters(dop, db: Database) -> int:
    if not dop:
        return 0

    children = get_child_parameters_from_dop(dop)
    if not children:
        return 1

    total = 0
    for p in children:
        sub_dop = None
        if getattr(p, "dop_ref", None):
            sub_dop = safe_resolve(p.dop_ref, db)

        total += count_leaf_parameters(sub_dop, db)

    return total

# ============================================================================
# PARAMETER FLATTENING
# ============================================================================

SKIP_PARAMS = {
    "SID_RQ",
    "SID_PR",
    "DATAIDENTIFIER",
    "RECORDDATAIDENTIFIER"
}

structure_registry = {}


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

       
    if has_children:

    # --- Ensure hierarchy base ---
        if struct_hierarchy is None:
            struct_hierarchy = [service_name]

        if struct_hierarchy_detail is None:
            struct_hierarchy_detail = [{
            "shortName": service_name,
            "longName": ""
        }]

    # --- Ensure hierarchy base ---
    if struct_hierarchy is None:
        struct_hierarchy = []

    if struct_hierarchy_detail is None:
        struct_hierarchy_detail = []

    # --- Current structure identity ---
    current_struct_short = getattr(param, "short_name", "UNKNOWN")
    current_struct_long = getattr(param, "long_name", "") or getattr(param, "description", "")

    # --- Build new hierarchy ---
    new_hierarchy = struct_hierarchy + [current_struct_short]

    new_hierarchy_detail = struct_hierarchy_detail + [{
        "shortName": current_struct_short,
        "longName": current_struct_long
    }]


    # --- First pass: collect leafs ---
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

    # --- Structure Key ---
    structure_key = ".".join(new_hierarchy)

    # --- Register metadata only once ---
    structure_registry[structure_key] = {
        "parameterCountInsideStructure": struct_leaf_total,
        "structureLevelDepth": struct_depth + 1,
        "structureHierarchy": new_hierarchy,
        "structureHierarchyPath": structure_key,
        "structureHierarchyDetailed": new_hierarchy_detail
    }

    # --- Leaf indexing ---
    index_map = {}
    for i, leaf in enumerate(temp, start=1):
        index_map[leaf.get("FullPath", "")] = i

    # --- Second pass recurse leaves ---
    results = []
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

def build_final_parameters_for_export(flatten_nodes):
    final = []

    for leaf in flatten_nodes:
        rm = leaf.get("responseMapping", {})
        sm = leaf.get("serviceMeta", {})

        final.append({
            "name": rm.get("specificParaName", ""),
            "path": leaf.get("FullPath", ""),
            "arrayIndex": sm.get("parameterIndexInsideStructure", 0),
            "dataType": rm.get("ParaType", ""),
            "bitlength": leaf.get("bitLength", 0),
            "endianness": "INTEL",
            "scaling": {
                "category": "LINEAR" if rm.get("Scale") not in (None, "") else "IDENTITY",
                "factor": rm.get("Scale") if rm.get("Scale") is not None else 1,
                "offset": rm.get("Offset") if rm.get("Offset") is not None else 0,
                "unit": rm.get("Unit", "")
            },
            "description": leaf.get("Description", "")
        })

    return final


def build_structure_selection(flatten_nodes):
    structure_entries = []

    for leaf in flatten_nodes:
        sm = leaf.get("serviceMeta", {})

        structure_entries.append({
            "path": leaf.get("FullPath", ""),
            "arrayIndex": sm.get("parameterIndexInsideStructure", 0),
            "arrayName": sm.get("arrayName", "Whl"),
            "topStruct": sm.get("topStruct", "")
        })

    return structure_entries

def _build_structure_service_block(ecu, svc, db: Database):
    svc_name = getattr(svc, "short_name", "")

    pos_params = []
    for pr in getattr(svc, "positive_responses", []) or []:
        pos_params.extend(getattr(pr, "parameters", []) or [])

    did_val = None
    for p in pos_params:
        n = normalize_name(getattr(p, "short_name", ""))
        if n in ["DID", "DATAIDENTIFIER", "RECORDDATAIDENTIFIER"]:
            did_val = getattr(p, "coded_value", None)
            break

    if did_val is None:
        return None

    did_hex = f"0x{int(did_val):04X}"

    flatten_nodes = []
    structure_registry = {}

    for p in pos_params:
        flatten_nodes.extend(
            flatten_parameter(
                p, db, "", svc_name, structure_registry=structure_registry
            )
        )

    return {
        "service": svc_name,
        "did": did_hex,
        "semantic": get_semantic(svc),
        "description": getattr(svc, "long_name", ""),

        "selection": {
            "type": "structureLeaf",
            "structure": build_structure_selection(flatten_nodes)
        },

        "finalParameters": build_final_parameters_for_export(flatten_nodes)
    }

def _build_table_row_service_blocks(ecu, svc, db: Database):
    results = []

    for resp in getattr(svc, "positive_responses", []) or []:
        for param in getattr(resp, "parameters", []) or []:

            table = (
                getattr(param, "table", None)
                or safe_resolve(getattr(param, "table_ref", None), db)
            )
            if not table:
                continue

            for row in getattr(table, "rows", []) or []:
                key = getattr(row, "key", None)
                if key is None:
                    continue

                did_hex = f"0x{int(key):04X}"

                flatten_nodes = []
                for p in getattr(row, "parameters", []) or []:
                    flatten_nodes.extend(
                        flatten_parameter(
                            p, db, "", getattr(svc, "short_name", "")
                        )
                    )

                results.append({
                    "service": getattr(svc, "short_name", ""),
                    "did": did_hex,
                    "semantic": get_semantic(svc),
                    "description": getattr(row, "long_name", "") or "",

                    "selection": {
                        "type": "tableRow",
                        "table": {
                            "name": getattr(table, "short_name", ""),
                            "rowFullXPath":
                                f"{ecu.short_name}/"
                                f"{svc.short_name}/"
                                f"{table.short_name}/"
                                f"{row.short_name}"
                        }
                    },

                    "finalParameters": build_final_parameters_for_export(flatten_nodes)
                })

    return results


# ============================================================================
# NORMAL DID EXTRACTION
# ============================================================================

def extract_normal_dids(ecu, db: Database, target_list: List[Dict[str, Any]], only_sid: str):
    for svc in getattr(ecu, "services", []):
        sid = detect_service_sid(svc)
        if sid != only_sid:
            continue

        svc_name = getattr(svc, "short_name", "")
        desc = getattr(svc, "long_name", "")
        semantic = get_semantic(svc)

        req = getattr(svc, "request", None)

        pos_params = []
        for pr in getattr(svc, "positive_responses", []) or []:
            pos_params.extend(getattr(pr, "parameters", []) or [])

        all_params = (getattr(req, "parameters", []) or []) + pos_params
        if not all_params:
            continue

        did_val = None
        for p in all_params:
            n = normalize_name(getattr(p, "short_name", ""))
            if n in ["DID", "DATAIDENTIFIER", "RECORDDATAIDENTIFIER"]:
                did_val = getattr(p, "coded_value", None)
                break

        if did_val is None:
            continue

        did_hex = f"0x{int(did_val):04X}"

        param_blocks = []
        structure_registry = {}
        for p in pos_params:
            flatten_parameter(p, db, "", svc_name, structure_registry=structure_registry)

        target_list.append({
            "ECUVariant": normalize_name(ecu.short_name),
            "autoBaseVariant": auto_base_variant(ecu.short_name),
            "DID": did_hex,
            "ServiceName": svc_name,
            "Semantic": semantic,
            "Description": desc,
            "structureMetadata": structure_registry,
            "Parameters": param_blocks
        })


# ============================================================================
# TABLE KEY DID EXTRACTION (ENHANCED PRIORITY LOGIC)
# ============================================================================

def extract_tablekey_dids(ecu, db: Database, did_groups: List[Dict[str, Any]]):
    """
    Handles TABLE KEY DIDs (Tables with Row Keys mapped to DIDs)
    Detects structure DOP, row parameters, or scalar value rows
    Adds table metadata + enhanced row intelligence
    """

    for svc in getattr(ecu, "services", []) or []:
        svc_name = getattr(svc, "short_name", "")
        semantic = get_semantic(svc)

        for resp in getattr(svc, "positive_responses", []) or []:
            for param in getattr(resp, "parameters", []) or []:

                table = (
                    getattr(param, "table", None)
                    or safe_resolve(getattr(param, "table_ref", None), db)
                    or safe_resolve(getattr(param, "table_key_ref", None), db)
                )
                if not table:
                    continue

                rows = (
                    getattr(table, "rows", None)
                    or getattr(table, "table_rows_raw", None)
                    or []
                )

                for idx, row in enumerate(rows):

                    # Key resolve
                    key = getattr(row, "key", None) or getattr(row, "key_raw", None)
                    if key is None:
                        continue

                    try:
                        did_hex = f"0x{int(key):04X}"
                    except:
                        did_hex = str(key)

                    params = []

                    # ====================================================
                    # Determine if this row is a STRUCTURE row
                    # ====================================================
                    is_structure = False

                    dop = safe_resolve(getattr(row, "datatype", None), db)
                    if dop and getattr(dop, "structure", None):
                        is_structure = True

                    elif hasattr(row, "parameters") and row.parameters:
                        is_structure = True

                    elif hasattr(row, "values") and row.values:
                        for v in row.values:
                            if hasattr(v, "parameters") and v.parameters:
                                is_structure = True
                                break

                    # ====================================================
                    # PARAMETER EXTRACTION
                    # ====================================================

                    # PRIORITY-1 STRUCTURE DOP
                    if dop and getattr(dop, "structure", None):
                        try:
                            for sub in dop.structure.parameters:
                                params.extend(flatten_parameter(sub, db, "", svc_name))
                        except:
                            pass

                    # PRIORITY-2 ROW PARAMETERS
                    if hasattr(row, "parameters") and row.parameters:
                        try:
                            for sub in row.parameters:
                                params.extend(flatten_parameter(sub, db, "", svc_name))
                        except:
                            pass

                    # PRIORITY-3 ROW VALUES
                    if hasattr(row, "values") and row.values:
                        try:
                            for sub in row.values:
                                if hasattr(sub, "parameters"):
                                    for p in sub.parameters:
                                        params.extend(flatten_parameter(p, db, "", svc_name))
                        except:
                            pass

                    # PRIORITY-4 SCALAR VALUE ROW
                    if not params:
                        dop = safe_resolve(getattr(row, "datatype", None), db)
                        if dop:
                            para_type = get_physical_type(dop)

                            params.append({
                                "FullPath":
                                    f"{svc_name}.{getattr(table,'short_name','')}"
                                    f"[{idx}].{getattr(row,'short_name','VALUE')}",

                                "serviceMeta": {
                                    "paraType": para_type,
                                    "structureKey": "",
                                    "parameterIndexInsideStructure": 1
                                },

                                "responseMapping": {
                                    "specificParaName": getattr(row, "short_name", ""),
                                    "ParaType": para_type,
                                    "Scale": "",
                                    "Offset": "",
                                    "Unit": ""
                                },

                                "validationInfo": {
                                    "isValidatedService": True
                                },

                                "ValueExample": ""
                            })

                    # ====================================================
                    # Build table full XPath
                    # ====================================================
                    try:
                        ecu_name = getattr(ecu, "short_name", "")
                        tableRowFullXPath = (
                            f"{ecu_name}/"
                            f"{svc_name}/"
                            f"{getattr(table,'short_name','')}/"
                            f"{getattr(row,'short_name','')}"
                        )
                    except:
                        tableRowFullXPath = getattr(row, "short_name", "") or ""

                    # ====================================================
                    # OUTPUT BLOCK
                    # ====================================================
                    did_groups.append({
                        "ECUVariant": normalize_name(ecu.short_name),
                        "BaseVariant": auto_base_variant(ecu.short_name),
                        "DID": did_hex,
                        "ServiceName": svc_name,
                        "Semantic": semantic,
                        "Description": getattr(row, "long_name", "") or getattr(table, "short_name", ""),

                        "tableName": getattr(table, "short_name", ""),
                        "tableRowIndex": idx,
                        "tableRowDescription":
                            getattr(row, "long_name", "")
                            or getattr(row, "short_name", ""),
                        "tableRowShortName": getattr(row, "short_name", ""),
                        "tableRowFullXPath": tableRowFullXPath,
                        "isTableRowStructure": is_structure,

                        "structureMetadata": {},
                        "Parameters": params
                    })

# ============================================================================
# OUTPUT FORMATTER
# ============================================================================

def format_output(db, read_groups, write_groups):
    return {
        "project": getattr(db, "id", "ODX Project"),
        "ecus": [e.short_name for e in getattr(db, "ecus", [])],
        "read_did_groups": read_groups,
        "write_did_groups": write_groups
    }


# ============================================================================
# ENTRY POINT
# ============================================================================

def parse_pdx_to_dids(pdx_path: str):
    db = odxtools.load_file(pdx_path, use_weakrefs=True)

    try:
        db.refresh()
    except Exception as e:
        if "COMPARAM" in str(e) or "ODXLINK reference" in str(e) or "prot_stacks" in str(e):
            pass
        else:
            raise

    read_groups = []
    write_groups = []

    for ecu in getattr(db, "ecus", []) or []:
        extract_normal_dids(ecu, db, read_groups, "0x22")
        extract_normal_dids(ecu, db, write_groups, "0x2E")

        try:
            extract_tablekey_dids(ecu, db, read_groups)
        except Exception:
            logger.exception("TABLE KEY DID FAILED")

    # return format_output(db, read_groups, write_groups)
    return convert_existing_groups_to_final_json(read_groups)


def generate_final_odx_json(pdx_path: str):
    db = odxtools.load_file(pdx_path, use_weakrefs=True)

    try:
        db.refresh()
    except Exception:
        pass

    ecu_blocks = []

    for ecu in getattr(db, "ecus", []) or []:
        ecu_json = {
            "ecuVariant": ecu.short_name,
            "baseVariant": auto_base_variant(ecu.short_name),
            "services": []
        }

        for svc in getattr(ecu, "services", []) or []:
            sid = detect_service_sid(svc)

            if sid == "0x22":
                blk = _build_structure_service_block(ecu, svc, db)
                if blk:
                    ecu_json["services"].append(blk)

            table_items = _build_table_row_service_blocks(ecu, svc, db)
            if table_items:
                ecu_json["services"].extend(table_items)

        ecu_blocks.append(ecu_json)

    return ecu_blocks[0] if len(ecu_blocks) == 1 else ecu_blocks

def convert_existing_groups_to_final_json(read_groups):
    ecu_map = {}

    for g in read_groups:
        ecu = g["ECUVariant"]
        if ecu not in ecu_map:
            ecu_map[ecu] = {
                "ecuVariant": g["ECUVariant"],
                "baseVariant": g["BaseVariant"],
                "services": []
            }

        service_entry = {
            "service": g["ServiceName"],
            "did": g["DID"],
            "semantic": g["Semantic"],
            "description": g["Description"],
        }

        # ----------------------------
        # TABLE → tableRow
        # ----------------------------
        if g.get("tableName"):
            service_entry["selection"] = {
                "type": "tableRow",
                "table": {
                    "name": g["tableName"],
                    "rowFullXPath": g["tableRowFullXPath"]
                }
            }

        # ----------------------------
        # NORMAL RDBI → structureLeaf
        # ----------------------------
        else:
            service_entry["selection"] = {
                "type": "structureLeaf",
                "structure": [
                    {
                        "path": p.get("FullPath", ""),
                        "arrayIndex": p.get("serviceMeta", {}).get("parameterIndexInsideStructure", 0),
                        "arrayName": p.get("serviceMeta", {}).get("arrayName", ""),
                        "topStruct": p.get("serviceMeta", {}).get("topStruct", "")
                    }
                    for p in g["Parameters"]
                ]
            }

        # ----------------------------
        # FINAL PARAMETERS
        # ----------------------------
        final_params = []
        for p in g["Parameters"]:
            rm = p.get("responseMapping", {})
            sm = p.get("serviceMeta", {})

            factor = rm.get("Scale")
            offset = rm.get("Offset")
            if factor is None:
                factor = 1
            if offset is None:
                offset = 0

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

