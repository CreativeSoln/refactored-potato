import json
import odxtools

from odx_utils import detect_service_sid, get_semantic

def validate_json_against_pdx(pdx_path, json_path):
    db = odxtools.load_file(pdx_path, use_weakrefs=True)
    db.refresh()

    with open(json_path, "r") as f:
        json_data = json.load(f)

    errors = []

    for ecu in db.ecus:
        ecu_name = ecu.short_name

        # locate ECU block in JSON
        ecu_json = None
        if isinstance(json_data, list):
            for e in json_data:
                if e["ecuInfo"]["ecuVariant"] == ecu_name:
                    ecu_json = e
                    break
        else:
            ecu_json = json_data

        if ecu_json is None:
            errors.append(f"ECU missing in JSON: {ecu_name}")
            continue

        # -------- Validate READ DIDs --------
        json_reads = ecu_json.get("read_did_groups", [])
        json_dids = {x["did"]: x for x in json_reads}

        for svc in ecu.services:
            sid = detect_service_sid(svc)
            if sid != "0x22":
                continue

            did = _resolve_did_value(svc)
            if not did:
                continue

            if did not in json_dids:
                errors.append(f"Missing READ DID in JSON: {ecu_name} {did}")

        # -------- Validate WRITE DIDs --------
        json_writes = ecu_json.get("write_did_groups", [])
        json_wdid = {x["did"]: x for x in json_writes}

        for svc in ecu.services:
            sid = detect_service_sid(svc)
            if sid != "0x2E":
                continue

            did = _resolve_did_value(svc)
            if not did:
                continue

            if did not in json_wdid:
                errors.append(f"Missing WRITE DID in JSON: {ecu_name} {did}")

    return errors


def _resolve_did_value(service):
    # Supports normal + split + table key DIDs
    pos_params = []
    for pr in getattr(service, "positive_responses", []) or []:
        pos_params.extend(getattr(pr, "parameters", []) or [])

    for p in pos_params:
        name = (getattr(p, "short_name", "") or "").upper()
        if "DID" in name:
            try:
                return f"0x{int(p.coded_value):04X}"
            except:
                pass

    return None

#python odx_json_validator.py --pdx my.pdx --json output.json
