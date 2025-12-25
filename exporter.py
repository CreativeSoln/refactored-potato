import json
from datetime import datetime

from odx_utils import (
    auto_base_variant,
    detect_service_sid,
    get_semantic
)

from flatten_structure import (
    flatten_parameter,
    GROUP_INDEX
)


class OdxDataExporter:
    """
    Exports final enhanced JSON format with:

    - meta schema
    - ecuInfo
    - services[]
      - sid
      - direction
      - security
      - runtime simulation
      - selection
      - finalParameters
    """

    def __init__(self):
        pass


    # ========================================================
    # Helpers
    # ========================================================
    def _direction_from_sid(self, sid: str):
        if sid == "0x22":
            return "READ"
        if sid == "0x2E":
            return "WRITE"
        return "UNKNOWN"


    def _build_runtime_block(self, sid, did_hex, final_parameters):
        """
        Build realistic runtime simulation block.
        """

        # -----------------------------------------------
        # UDS READ example
        # -----------------------------------------------
        try:
            sid_int = int(sid, 16)
            pos_sid = f"{sid_int + 0x40:02X}"
        except:
            sid_int = 0x22
            pos_sid = "62"

        did_clean = did_hex.replace("0x", "").upper()

        sample_request = f"{sid.replace('0x','').upper()} {did_clean[:2]} {did_clean[2:]}"
        sample_response = f"{pos_sid} {did_clean[:2]} {did_clean[2:]}"

        # -----------------------------------------------
        # Payload length + dummy bytes
        # -----------------------------------------------
        payload_len = int(sum(p.get("bitlength", 0) for p in final_parameters) / 8)
        payload = " ".join(["00"] * payload_len).strip()

        # -----------------------------------------------
        # Build decoded sample
        # -----------------------------------------------
        decoded = {}

        for p in final_parameters:
            nm = p.get("name", "")
            scale = (
                p.get("scaling", {}).get("factor", 1)
                if p.get("scaling") else 1
            )
            unit = (
                p.get("scaling", {}).get("unit", "")
                if p.get("scaling") else ""
            )

            idx = p.get("arrayIndex", 0)

            if "km/h" in unit:
                val = 50 + (idx * 3)

            elif "mm" in unit:
                val = 1980 + (idx * 5)

            elif "ASCII" in p.get("dataType", ""):
                val = f"{nm}_VAL"

            else:
                val = 1 + idx

            decoded[nm] = float(val) if isinstance(val, (int, float)) else val

        block = {
            "supportsSimulation": True,
            "sampleRequestHex": sample_request,
            "sampleResponseHex": (sample_response + " " + payload).strip(),
            "decodedSample": decoded
        }

        return block


    # ========================================================
    # EXPORT FOR EACH SERVICE
    # ========================================================
    def _build_structure_service_block(self, ecu, svc, db):
        """
        Services with normal positive response parameters
        """
        svc_name = getattr(svc, "short_name", "")
        sid = detect_service_sid(svc)

        # reset array counter
        GROUP_INDEX.clear()

        pos_params = []
        for pr in getattr(svc, "positive_responses", []) or []:
            pos_params.extend(getattr(pr, "parameters", []) or [])

        if not pos_params:
            return None

        did_val = None
        for p in pos_params:
            nm = getattr(p, "short_name", "").upper()
            if "DID" in nm:
                did_val = getattr(p, "coded_value", None)
                break

        if did_val is None:
            return None

        did_hex = f"0x{int(did_val):04X}"

        flatten_nodes = []
        for p in pos_params:
            flatten_nodes.extend(
                flatten_parameter(p, db, "", svc_name)
            )

        final_parameters = self._build_final_parameters(flatten_nodes)
        selection = self._build_selection(flatten_nodes)

        block = {
            "service": svc_name,
            "sid": sid,
            "did": did_hex,
            "direction": self._direction_from_sid(sid),
            "semantic": get_semantic(svc),
            "description": getattr(svc, "long_name", "") or "",

            "security": {
                "requiresUnlock": False,
                "level": None
            },

            "runtime": self._build_runtime_block(sid, did_hex, final_parameters),

            "selection": selection,
            "finalParameters": final_parameters
        }

        return block


    # ========================================================
    # TABLE ROW DID BUILDER
    # ========================================================
    def _build_table_row_blocks(self, ecu, svc, db):
        results = []

        for resp in getattr(svc, "positive_responses", []) or []:
            for param in getattr(resp, "parameters", []) or []:

                table = getattr(param, "table", None)
                if not table:
                    continue

                for row in getattr(table, "rows", []) or []:

                    key = getattr(row, "key", None)
                    if key is None:
                        continue

                    did_hex = f"0x{int(key):04X}"

                    GROUP_INDEX.clear()

                    flatten_nodes = []
                    for p in getattr(row, "parameters", []) or []:
                        flatten_nodes.extend(
                            flatten_parameter(p, db, "", getattr(svc, "short_name", ""))
                        )

                    final_params = self._build_final_parameters(flatten_nodes)

                    block = {
                        "service": getattr(svc, "short_name", ""),
                        "sid": detect_service_sid(svc),
                        "did": did_hex,
                        "direction": "READ",
                        "semantic": get_semantic(svc),
                        "description": getattr(row, "long_name", "") or "",

                        "security": {
                            "requiresUnlock": False,
                            "level": None
                        },

                        "runtime": self._build_runtime_block(
                            detect_service_sid(svc),
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
                                    f"{table.short_name}/"
                                    f"{row.short_name}"
                            }
                        },

                        "finalParameters": final_params
                    }

                    results.append(block)

        return results


    # ========================================================
    # PARAMETER FORMATTERS
    # ========================================================
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


    # ========================================================
    # PUBLIC API
    # ========================================================
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

            "services": []
        }

        for svc in getattr(ecu, "services", []) or []:

            blk = self._build_structure_service_block(ecu, svc, db)
            if blk:
                ecu_json["services"].append(blk)

            table_items = self._build_table_row_blocks(ecu, svc, db)
            if table_items:
                ecu_json["services"].extend(table_items)

        return ecu_json
