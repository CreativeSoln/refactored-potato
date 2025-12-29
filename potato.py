// Find children of structure param
let childContainer = paramEl;

// if STRUCTURE wrapper exists, use it
const structEl = paramEl.getElementsByTagName("STRUCTURE")[0];
if (structEl) childContainer = structEl;

const childParams = getElements(childContainer, "PARAM");

if (childParams && childParams.length > 0) {
  param.children = childParams.map(p =>
    parseParam(p, "STRUCTURE", shortName, layerName, serviceShortName)
  );
}


const childParams = getElements(paramEl, "PARAM");
   if (childParams && childParams.length > 0) {
     param.children = childParams.map(p =>
       parseParam(p, "STRUCTURE", shortName, layerName, serviceShortName)
     );
   }

# ------------------------------------------------
# WRITE DID Response Rule (ISO 14229)
# ------------------------------------------------
if sid == "0x2E":
    # Response DOES NOT return payload
    sample_response = f"{pos_sid} {did_hi} {did_lo}"
else:
    # READ – response may have payload
    sample_response = f"{pos_sid} {did_hi} {did_lo} {payload_bytes}".strip()


# -----------------------------------------------
# Build Request / Response Header
# -----------------------------------------------
try:
    sid_int = int(sid, 16)
    pos_sid = f"{sid_int + 0x40:02X}"
except:
    sid_int = 0x22
    pos_sid = "62"

did_clean = did_hex.replace("0x", "").upper()
did_hi = did_clean[:2]
did_lo = did_clean[2:]

# ------------------------------------------------
# Compute payload byte length
# ------------------------------------------------
payload_len = max(1, int(sum(p.get("bitlength", 0) or 0 for p in final_parameters) / 8))

# Deterministic payload (01 02 03 …)
payload_bytes = " ".join([f"{(i+1):02X}" for i in range(payload_len)])

# ------------------------------------------------
# READ vs WRITE request formatting
# ------------------------------------------------
if sid == "0x2E":
    # WRITE: SID + DID + DATA
    sample_request = f"2E {did_hi} {did_lo} {payload_bytes}".strip()
else:
    # READ: SID + DID
    sample_request = f"{sid.replace('0x','').upper()} {did_hi} {did_lo}".strip()

# ------------------------------------------------
# Positive Response Always = SID+0x40 + DID
# ------------------------------------------------
sample_response = f"{pos_sid} {did_hi} {did_lo} {payload_bytes if sid=='0x2E' else ''}".strip()



def _build_runtime_block(self, sid, did_hex, final_parameters):
    """
    Build deterministic and realistic UDS runtime simulation data.
    - Request frame:  SID + DID
    - Response frame: POSITIVE SID + DID + payload
    - Payload bytes: generated based on parameter bit length
    - Decoded values: numeric where applicable
    """

    # -----------------------------
    # Resolve SID and POS SID
    # -----------------------------
    try:
        sid_int = int(sid.replace("0x", ""), 16)
    except Exception:
        sid_int = 0x22

    pos_sid = f"{sid_int + 0x40:02X}"

    # -----------------------------
    # DID Formatting
    # -----------------------------
    did_clean = did_hex.replace("0x", "").upper()
    if len(did_clean) < 4:
        did_clean = did_clean.rjust(4, "0")

    did_hi = did_clean[:2]
    did_lo = did_clean[2:]

    sample_request = f"{sid_int:02X} {did_hi} {did_lo}"

    # -----------------------------
    # Compute Payload Length
    # -----------------------------
    total_bits = sum(int(p.get("bitlength", 0) or 0) for p in final_parameters)
    if total_bits <= 0:
        total_bits = 16

    byte_len = max(1, total_bits // 8)

    # -----------------------------
    # Generate Realistic Payload
    # -----------------------------
    payload_bytes = []
    value_seed = int(did_clean, 16) % 255 or 1

    for i in range(byte_len):
        payload_bytes.append((value_seed + i) & 0xFF)

    payload_hex = " ".join(f"{b:02X}" for b in payload_bytes)

    sample_response = f"{pos_sid} {did_hi} {did_lo} {payload_hex}".strip()

    # -----------------------------
    # Build Decoded Sample
    # -----------------------------
    decoded = {}
    curr_val = 1

    for p in final_parameters:
        name = p.get("name") or p.get("arrayName") or "Value"

        try:
            bitlen = int(p.get("bitlength", 0) or 0)
        except:
            bitlen = 16

        scale = 1.0
        unit = ""

        if p.get("scaling"):
            scale = float(p["scaling"].get("factor", 1) or 1)
            unit = p["scaling"].get("unit", "") or ""

        value = (curr_val * scale)

        if unit:
            decoded[name] = float(value)
        else:
            decoded[name] = int(value)

        curr_val += 1

    # -----------------------------
    # Runtime Block Output
    # -----------------------------
    return {
        "supportsSimulation": True,
        "sampleRequestHex": sample_request,
        "sampleResponseHex": sample_response,
        "decodedSample": decoded
    }


def export_ecu(self, db, ecu):
    ecu_json = {
        "meta": {
            "schemaVersion": "1.1.0",
            "generatedBy": "ODX_JSON_EXPORTER",
            "generationTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sourcePDX": getattr(db, "id", "UNKNOWN")
        },

        "ecuInfo": {
            "ecuVariant": ecu.short_name,
            "baseVariant": auto_base_variant(ecu.short_name)
        },

        "read_did_groups": [],
        "write_did_groups": []
    }

    for svc in getattr(ecu, "services", []) or []:

        # -------------------------
        # READ DIDs (0x22)
        # -------------------------
        read_blk = self._build_structure_service_block(ecu, svc, db)
        if read_blk:
            ecu_json["read_did_groups"].append(read_blk)

        # -------------------------
        # READ — TABLE based DIDs
        # -------------------------
        table_items = self._build_table_row_blocks(ecu, svc, db)
        if table_items:
            ecu_json["read_did_groups"].extend(table_items)

        # -------------------------
        # WRITE DIDs (0x2E)
        # -------------------------
        write_blk = self._build_write_service_block(ecu, svc, db)
        if write_blk:
            ecu_json["write_did_groups"].append(write_blk)

    return ecu_json

def _build_write_service_block(self, ecu, svc, db):
    """
    Handles WriteDataByIdentifier (0x2E)
    Extracts DID + request payload parameters
    Builds runtime simulation + metadata
    """

    sid = detect_service_sid(svc)
    if sid != "0x2E":
        return None

    svc_name = getattr(svc, "short_name", "")

    req = getattr(svc, "request", None)
    if not req:
        return None

    params = getattr(req, "parameters", []) or []
    if not params:
        return None

    # ------------------------------
    # Extract DID
    # ------------------------------
    did_val = None
    for p in params:
        nm = getattr(p, "short_name", "").upper()
        if "DID" in nm or "IDENTIFIER" in nm:
            did_val = getattr(p, "coded_value", None)
            break

    if did_val is None:
        return None

    did_hex = f"0x{int(did_val):04X}"

    # ------------------------------
    # Extract Write Payload Fields
    # ------------------------------
    payload_params = []

    for p in params:
        nm = getattr(p, "short_name", "").upper()

        # Skip service id + DID field
        if "SERVICE" in nm:
            continue
        if "DID" in nm or "IDENTIFIER" in nm:
            continue

        payload_params.extend(
            flatten_parameter(p, db, "", svc_name)
        )

    final_parameters = self._build_final_parameters(payload_params)

    # ------------------------------
    # Runtime Simulation
    # ------------------------------
    runtime = self._build_runtime_block(
        sid,
        did_hex,
        final_parameters
    )

    runtime["sampleRequestHex"] += " " + runtime.pop("writePayloadHex", "")

    block = {
        "service": svc_name,
        "sid": sid,
        "did": did_hex,
        "direction": "WRITE",
        "semantic": get_semantic(svc),
        "description": getattr(svc, "long_name", "") or "",

        "security": {
            "requiresUnlock": False,
            "level": None
        },

        "runtime": runtime,

        "selection": {
            "type": "writePayload",
            "structure": [
                {
                    "path": leaf.get("FullPath", ""),
                    "arrayIndex": leaf.get("serviceMeta", {}).get("parameterIndexInsideStructure", 0),
                    "arrayName": leaf.get("serviceMeta", {}).get("arrayName", ""),
                    "topStruct": leaf.get("serviceMeta", {}).get("topStruct", "")
                }
                for leaf in payload_params
            ]
        },

        "finalParameters": final_parameters
    }

    return block


def _build_selection(self, flatten_nodes):
    return {
        "type": "structureLeaf",
        "structure": [
            {
                "path": leaf.get("FullPath", ""),
                "arrayIndex": leaf.get("serviceMeta", {}).get("parameterIndexInsideStructure", 0),
                "arrayName": leaf.get("serviceMeta", {}).get("arrayName", ""),
                "topStruct": leaf.get("serviceMeta", {}).get("topStruct", "")
            }
            for leaf in flatten_nodes
        ]
    }


def _build_final_parameters(self, flatten_nodes):
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
            "scaling": leaf.get("scaling", rm),
            "description": leaf.get("Description", "")
        })

    return final


def _build_table_row_blocks(self, ecu, svc, db):
    """
    Builds service blocks for TABLE-KEY DIDs.

    Supports:
        • table
        • table_ref
        • table_key_ref
        • table_rows_raw
        • datatype structure-based rows

    Ensures finalParameters + selection populate correctly.
    """

    results = []
    svc_name = getattr(svc, "short_name", "")
    sid = detect_service_sid(svc)

    for resp in getattr(svc, "positive_responses", []) or []:
        for param in getattr(resp, "parameters", []) or []:

            # --------------------------------------------------
            # Resolve Table Definition (Covers All OEM Styles)
            # --------------------------------------------------
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
            if not rows:
                continue

            # --------------------------------------------------
            # Process Each TABLE ROW
            # --------------------------------------------------
            for row in rows:
                key = getattr(row, "key", None) or getattr(row, "key_raw", None)
                if key is None:
                    continue

                try:
                    did_hex = f"0x{int(key):04X}"
                except Exception:
                    did_hex = str(key)

                flatten_nodes = []
                GROUP_INDEX.clear()

                # =====================================================
                #   PRIORITY-1 STRUCTURE DOP (Row holds datatype)
                # =====================================================
                dop = safe_resolve(getattr(row, "datatype", None), db)
                if dop and getattr(dop, "structure", None):
                    try:
                        for sub in dop.structure.parameters:
                            flatten_nodes.extend(
                                flatten_parameter(sub, db, "", svc_name)
                            )
                    except Exception:
                        pass

                # =====================================================
                #   PRIORITY-2 EXPLICIT ROW PARAMETERS
                # =====================================================
                if hasattr(row, "parameters") and row.parameters:
                    try:
                        for sub in row.parameters:
                            flatten_nodes.extend(
                                flatten_parameter(sub, db, "", svc_name)
                            )
                    except Exception:
                        pass

                # =====================================================
                # If Still Empty → Treat as Scalar Value Row
                # =====================================================
                if not flatten_nodes:
                    flatten_nodes.append({
                        "FullPath": f"{svc_name}.{table.short_name}.{row.short_name}",
                        "serviceMeta": {
                            "parameterIndexInsideStructure": 0
                        },
                        "responseMapping": {
                            "specificParaName": getattr(row, "short_name", "VALUE"),
                            "ParaType": "",
                        },
                        "bitLength": 8
                    })

                final_params = self._build_final_parameters(flatten_nodes)

                # --------------------------------------------------
                # Build Final BLOCK
                # --------------------------------------------------
                results.append({
                    "service": svc_name,
                    "sid": sid,
                    "did": did_hex,
                    "direction": "READ",
                    "semantic": get_semantic(svc),
                    "description": getattr(row, "long_name", "") or getattr(row, "short_name", ""),

                    "security": {
                        "requiresUnlock": False,
                        "level": None
                    },

                    "runtime": self._build_runtime_block(
                        sid,
                        did_hex,
                        final_params
                    ),

                    "selection": {
                        "type": "tableRow",
                        "table": {
                            "name": getattr(table, "short_name", ""),
                            "rowFullXPath":
                                f"{ecu.short_name}/"
                                f"{svc.short_name}/"
                                f"{getattr(table,'short_name','')}/"
                                f"{getattr(row,'short_name','')}"
                        }
                    },

                    "finalParameters": final_params
                })

    return results


def _build_runtime_block(self, sid, did_hex, final_parameters):
    """
    Builds realistic diagnostic runtime simulation output.
    Generates:
        • Valid UDS request frame
        • Valid UDS positive response frame
        • Meaningful decoded runtime sample
    Ensures:
        • Positive SID = SID + 0x40
        • DID preserved in response
        • Payload reflects datatype + bit length
        • Deterministic values (stable for SIL/HIL testing)
    """

    # -----------------------------------------------
    # Resolve SID → Positive SID
    # -----------------------------------------------
    try:
        sid_int = int(sid, 16)
        pos_sid = f"{sid_int + 0x40:02X}"
    except Exception:
        sid_int = 0x22
        pos_sid = "62"

    did_clean = did_hex.replace("0x", "").upper()

    request_hex = f"{sid.replace('0x','').upper()} {did_clean[:2]} {did_clean[2:]}"
    response_header = f"{pos_sid} {did_clean[:2]} {did_clean[2:]}"

    # -----------------------------------------------
    # Build Payload Bytes
    # -----------------------------------------------
    response_bytes = []
    decoded = {}

    for p in final_parameters:
        name = p.get("name", "")
        dtype = p.get("dataType", "")
        bitlen = p.get("bitlength", 0)
        idx = p.get("arrayIndex", 0)

        scaling = p.get("scaling", {})
        factor = scaling.get("factor", 1) if scaling else 1
        unit = scaling.get("unit", "")

        # determine byte width safely
        byte_width = max(1, bitlen // 8)
        if byte_width > 8:      # safety bound
            byte_width = 8

        # =====================================================
        # NUMERIC VALUES
        # =====================================================
        if "UINT" in dtype or "SINT" in dtype or "A_FLOAT" in dtype:
            base_value = 10 + idx

            try:
                physical = base_value * (factor if factor else 1)
            except Exception:
                physical = base_value

            if isinstance(physical, float):
                if physical.is_integer():
                    physical = int(physical)
                else:
                    physical = round(physical, 2)

            decoded[name] = physical

            try:
                enc_val = int(float(physical))
                encoded = enc_val.to_bytes(byte_width, "big", signed=False)
            except Exception:
                encoded = b"\x01"

            for b in encoded:
                response_bytes.append(f"{b:02X}")

        # =====================================================
        # ASCII VALUES
        # =====================================================
        elif "ASCII" in dtype:
            text = f"VAL{idx+1}"
            decoded[name] = text

            enc = text.encode("ascii")

            if len(enc) > byte_width:
                enc = enc[:byte_width]

            for b in enc:
                response_bytes.append(f"{b:02X}")

            while len(enc) < byte_width:
                response_bytes.append("20")  # space padding

        # =====================================================
        # FALLBACK
        # =====================================================
        else:
            value = 1 + idx
            decoded[name] = value
            response_bytes.append(f"{value:02X}")

    # -----------------------------------------------
    # Final Response Frame
    # -----------------------------------------------
    payload = " ".join(response_bytes)

    return {
        "supportsSimulation": True,
        "sampleRequestHex": request_hex,
        "sampleResponseHex": f"{response_header} {payload}".strip(),
        "decodedSample": decoded
    }


def _build_runtime_block(self, sid, did_hex, final_parameters):
    """
    Runtime simulation builder.
    Produces realistic request/response frames and decoded example values.
    Prevents payload overflow, validates parameter lengths and builds
    stable, bounded payload content suitable for test execution.
    """

    # -----------------------------------------------
    # Resolve SID and Positive Response SID
    # -----------------------------------------------
    try:
        sid_int = int(sid, 16)
        pos_sid = f"{sid_int + 0x40:02X}"
    except Exception:
        sid_int = 0x22
        pos_sid = "62"

    did_clean = did_hex.replace("0x", "").upper()

    sample_request = f"{sid.replace('0x','').upper()} {did_clean[:2]} {did_clean[2:]}"
    sample_response = f"{pos_sid} {did_clean[:2]} {did_clean[2:]}"

    # -----------------------------------------------
    # Compute total payload size safely
    # -----------------------------------------------
    total_bits = 0

    for p in final_parameters:
        bl = p.get("bitlength", 0)
        if isinstance(bl, int) and bl > 0:
            total_bits += bl

    # Convert to byte length
    byte_len = max(1, int(total_bits / 8))

    # HARD SAFETY LIMIT — prevents integer overflow
    if byte_len > 256:
        byte_len = 256

    # -----------------------------------------------
    # Build dummy payload (00 padded)
    # -----------------------------------------------
    payload = " ".join(["00"] * byte_len).strip()

    # -----------------------------------------------
    # Build decoded sample values
    # -----------------------------------------------
    decoded = {}

    for p in final_parameters:
        name = p.get("name", "")
        idx = p.get("arrayIndex", 0)
        scaling = p.get("scaling", {})
        unit = scaling.get("unit", "")

        # Simulated value logic
        if "km/h" in unit:
            value = 50 + (idx * 3)

        elif "mm" in unit:
            value = 1980 + (idx * 5)

        elif "ASCII" in p.get("dataType", ""):
            value = f"{name}_VAL"

        else:
            value = 1 + idx

        decoded[name] = value

    return {
        "supportsSimulation": True,
        "sampleRequestHex": sample_request,
        "sampleResponseHex": f"{sample_response} {payload}".strip(),
        "decodedSample": decoded
    }


def _build_runtime_block(self, sid, did_hex, final_parameters):
    """
    Builds a realistic Diagnostic Runtime Simulation block.

    This function generates:
        1) A valid UDS diagnostic service request frame (hex string)
        2) A logically correct UDS positive response frame (hex string)
        3) A decoded JSON view representing physical values

    Behavior Rules:
        • Positive Response SID = Request SID + 0x40
        • Response begins with: [Positive SID, DID_HI, DID_LO]
        • Remaining payload bytes are synthesized from parameter metadata
        • Payload length and values depend on parameter datatype & bit length
        • Encoded response bytes always match decodedSample content

    Generation Logic:
        ▸ Numeric Parameters (UINT / SINT / FLOAT):
            – Deterministic synthetic values based on index
            – Values respect scaling factor
            – Encoded to BIG-ENDIAN byte representation
        ▸ Text / ASCII Parameters:
            – Encoded as ASCII byte stream
        ▸ Unknown Types:
            – Minimal deterministic numeric fallback

    Inputs:
        sid (str)
            Hex UDS Service Identifier (e.g., "0x22")

        did_hex (str)
            Hex DID value (e.g., "0xF180")

        final_parameters (list)
            Flattened parameter list already containing:
                - name
                - dataType
                - bitlength
                - scaling (factor, unit)
                - arrayIndex

    Output JSON Structure:
        {
            "supportsSimulation": true,
            "sampleRequestHex": "<UDS Request Hex>",
            "sampleResponseHex": "<UDS Positive Response Hex>",
            "decodedSample": {
                "<parameter>": <decoded_value>,
                ...
            }
        }

    Guarantees:
        • Always produces syntactically valid UDS frames
        • Deterministic output (repeatable every run)
        • Never generates 0x00 filler unless required
        • Safe fallback for incomplete metadata

    This runtime section is intended for:
        ✓ ECU bench testing tools
        ✓ HIL / SIL diagnostic validation
        ✓ Software simulation / offline tester environments
        ✓ Developer visualization

    """


from odx_json_exporter import OdxDataExporter
import odxtools
import json


def export_final_json(pdx_path, out_file):
    db = odxtools.load_file(pdx_path, use_weakrefs=True)

    try:
        db.refresh()
    except:
        pass

    exporter = OdxDataExporter()

    final_output = []

    for ecu in getattr(db, "ecus", []) or []:
        ecu_json = exporter.export_ecu(db, ecu)
        final_output.append(ecu_json)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2)

    print(f"Export completed → {out_file}")


from typing import List, Dict, Any
from odxtools.database import Database

# ----------------- IMPORT HELPERS FROM YOUR EXISTING FILE -----------------
# normalize_name
# safe_resolve
# get_child_parameters_from_dop
# get_scale_offset_unit
# get_physical_type
# count_leaf_parameters
# --------------------------------------------------------------------------

# -------- Assign deterministic indexes --------
for idx, leaf in enumerate(temp):
    sm = leaf.setdefault("serviceMeta", {})
    sm["parameterIndexInsideStructure"] = idx

    print("INDEX ASSIGNED",
          leaf.get("FullPath"),
          "=>",
          sm["parameterIndexInsideStructure"])



# GLOBAL COUNTER (resets per service)
_GLOBAL_INDEX_COUNTER = 0


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
    global _GLOBAL_INDEX_COUNTER

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
    #                LEAF PARAMETER  (DIRECT PARAMETER)
    # =====================================================================
    if not has_children:
        scale, offset, unit = get_scale_offset_unit(dop)

        bitlen = 0
        try:
            if hasattr(dop, "diag_coded_type") and hasattr(dop.diag_coded_type, "bit_length"):
                bitlen = dop.diag_coded_type.bit_length
            elif hasattr(dop, "bit_length"):
                bitlen = dop.bit_length
        except:
            pass

        # ---------- GUARANTEED ARRAY INDEX ----------
        array_index = _GLOBAL_INDEX_COUNTER
        _GLOBAL_INDEX_COUNTER += 1

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
        "longName": current_struct_long
    }]

    # -------- Collect ALL leafs --------
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

    # -------- Register structure --------
    structure_key = ".".join(new_hierarchy)

    structure_registry[structure_key] = {
        "parameterCountInsideStructure": len(temp),
        "structureLevelDepth": struct_depth + 1,
        "structureHierarchy": new_hierarchy,
        "structureHierarchyPath": structure_key,
        "structureHierarchyDetailed": new_hierarchy_detail
    }

    results.extend(temp)
    return results



SKIP_PARAMS = {
    "SID",
    "SID_RQ",
    "SID_PR",
    "SERVICE_ID",
    "REQUESTSERVICEID",
    "RESPONSESERVICEID",

    "DID",
    "DATAIDENTIFIER",
    "DATA_IDENTIFIER",
    "RECORDDATAIDENTIFIER",
    "RECORD_DATA_IDENTIFIER",

    "RID",
    "ROUTINEIDENTIFIER",
    "ROUTINE_IDENTIFIER",

    "SUBFUNCTION",
    "SF",
}


def _looks_like_did(name: str):
    if not name:
        return False

    n = normalize_name(name)

    # Exact known common names
    if n in [
        "DID",
        "DATAIDENTIFIER",
        "RECORDDATAIDENTIFIER",
        "ID",
        "ROUTINEIDENTIFIER",
        "ROUTINE_IDENTIFIER"
    ]:
        return True

    # Common alternate spellings
    alt_patterns = [
        "DATA_IDENTIFIER",
        "DATAID",
        "DATA_ID",
        "RECORD_ID",
        "RECORD_IDENTIFIER",
        "DID_VALUE",
        "DIDID",
        "DIAGDATAID",
        "ECU_DID",
        "ECUDID",
        "ID_VALUE",
        "IDENTIFIER"
    ]

    if n in alt_patterns:
        return True

    # Heuristic fallback:
    # if name contains DID or IDENTIFIER anywhere
    if "DID" in n or "IDENTIFIER" in n:
        return True

    return False


def find_did_in_params(params):
    if not params:
        return None

    for p in params:
        name = normalize_name(getattr(p, "short_name", ""))

        #if name in ["DID", "DATAIDENTIFIER", "RECORDDATAIDENTIFIER", "ID"]:
        if _looks_like_did(name):
            # Try standard coded value
            v = getattr(p, "coded_value", None)

            # Fallback raw
            if v is None:
                v = getattr(p, "coded_value_raw", None)

            if v is None:
                continue

            try:
                return int(v)
            except Exception:
                pass

    return None


def extract_normal_dids(ecu, db: Database, target_list: List[Dict[str, Any]], only_sid: str):

    for svc in getattr(ecu, "services", []):

        sid = detect_service_sid(svc)
        if sid != only_sid:
            continue

        svc_name = getattr(svc, "short_name", "")
        desc = getattr(svc, "long_name", "")
        semantic = get_semantic(svc)

        req = getattr(svc, "request", None)

        # Collect POS response parameters
        pos_params = []
        for pr in getattr(svc, "positive_responses", []) or []:
            pos_params.extend(getattr(pr, "parameters", []) or [])

        req_params = getattr(req, "parameters", []) if req else []
        all_params = (req_params or []) + (pos_params or [])

        if not all_params:
            continue

        # ---------------------------------------------------------
        # DID DETECTION (Supports: POS → REQUEST → FALLBACK ALL)
        # ---------------------------------------------------------
        did_val = None

        # 1️⃣ Normal — DID in Positive Response
        did_val = find_did_in_params(pos_params)

        # 2️⃣ Some ECUs — DID only in Request
        if did_val is None:
            did_val = find_did_in_params(req_params)

        # 3️⃣ Last fallback — any parameter
        if did_val is None:
            did_val = find_did_in_params(all_params)

        if did_val is None:
            print(f"[WARN] NO DID FOUND for Service: {svc_name}")
            continue

        did_hex = f"0x{int(did_val):04X}"

        # ---------------------------------------------------------
        # PARAMETER FLATTENING
        # ---------------------------------------------------------
        param_blocks = []
        structure_registry = {}

        source_params = []

        # Prefer POS → then REQ → then ALL
        if pos_params:
            source_params = pos_params
        elif req_params:
            source_params = req_params
        else:
            source_params = all_params

        for p in source_params:
            param_blocks.extend(
                flatten_parameter(
                    p,
                    db,
                    "",
                    svc_name,
                    structure_registry=structure_registry
                )
            )

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



CkCert cert;

    // Load your certificate
    bool success = cert.LoadFromFile("myCert.der");
    if (!success) {
        printf("Failed to load cert\n");
        return 0;
    }

    // --- Put your Extension OID here ---
    const char *oid = "1.3.6.1.4.1.13159.1.2.5";   // example from your screenshot

    CkBinData bd;
    success = cert.GetExtensionVal(oid, bd);
    if (!success) {
        printf("Extension not found\n");
        return 0;
    }

    // bd now contains DER for: OCTET STRING
    CkAsn asn;
    success = asn.LoadDerBd(bd);
    if (!success) {
        printf("ASN load failed\n");
        return 0;
    }

    // If it is a simple printable OCTET STRING
    printf("Value: %s\n", asn.value());     // should print "dev" in your case

    // If it’s nested, you may need another decode:
    if (asn.get_NumSubItems() > 0) {
        CkAsn *inner = asn.GetSubItem(0);
        printf("Inner value: %s\n", inner->value());
        delete inner;
    }

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
