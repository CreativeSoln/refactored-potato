import json
import sys
import argparse
from datetime import datetime

import odxtools

from odx_utils import (
    detect_service_sid,
    get_semantic
)


# =========================================================
# Helpers
# =========================================================
def hex_to_bytes(hex_str: str):
    """
    Safely convert space-separated HEX string to bytes.
    Ignores invalid tokens and ensures each byte is within 0-255.
    """

    if not hex_str or not isinstance(hex_str, str):
        return b""

    parts = (
        hex_str.replace("\n", " ")
               .replace("\r", " ")
               .replace("  ", " ")
               .strip()
               .split(" ")
    )

    out = []

    for p in parts:
        p = p.strip()

        # Skip blanks
        if not p:
            continue

        # Ensure exactly 2 hex chars
        if len(p) > 2:
            # Example: "620100" ➝ split into ["62","01","00"]
            while p:
                chunk = p[:2]
                p = p[2:]
                try:
                    out.append(int(chunk, 16))
                except:
                    pass
            continue

        try:
            v = int(p, 16)
            if 0 <= v <= 255:
                out.append(v)
        except:
            # Ignore garbage values instead of crashing
            continue

    return bytes(out)


def decode_payload(payload_bytes, final_params):
    """
    Decodes binary payload using JSON finalParameters rules:
    - bitlength
    - endianness
    - scaling
    Returns dict of decoded values
    """
    idx = 0
    result = {}

    for p in final_params:
        bitlen = p.get("bitlength", 0)
        bytelen = max(1, bitlen // 8)

        raw = payload_bytes[idx: idx + bytelen]
        idx += bytelen

        if not raw:
            result[p["name"]] = None
            continue

        endian = "little" if p.get("endianness", "INTEL") == "INTEL" else "big"
        val = int.from_bytes(raw, endian)

        scaling = p.get("scaling", {})
        scale = scaling.get("factor", 1) or 1
        offset = scaling.get("offset", 0) or 0

        phys = (val * scale) + offset

        if isinstance(phys, float):
            result[p["name"]] = round(phys, 3)
        else:
            result[p["name"]] = phys

    return result


# =========================================================
# Runtime Validation
# =========================================================
def validate_runtime(service_json, errors):
    runtime = service_json.get("runtime", {})
    if not runtime.get("supportsSimulation"):
        return

    resp_hex = runtime.get("sampleResponseHex", "")
    payload = hex_to_bytes(resp_hex)

    if len(payload) < 3:
        errors.append(f"Runtime response too short DID={service_json.get('did')}")
        return

    # Strip SID + DID (3 bytes)
    payload_only = payload[3:]

    decoded_runtime = runtime.get("decodedSample", {})
    recomputed = decode_payload(payload_only, service_json.get("finalParameters", []))

    for k, v in recomputed.items():
        json_val = decoded_runtime.get(k)

        if str(json_val) != str(v):
            errors.append(
                f"Binary decode mismatch DID={service_json.get('did')} param={k} expected={v} got={json_val}"
            )


# =========================================================
# DID Resolver
# =========================================================
def resolve_did_value(service):
    pos_params = []
    for pr in getattr(service, "positive_responses", []) or []:
        pos_params.extend(getattr(pr, "parameters", []) or [])

    # handle split DID + combined names + table keys
    hi = None
    lo = None

    for p in pos_params:
        name = (getattr(p, "short_name", "") or "").upper()

        if "DID" in name or "DATAIDENTIFIER" in name or "RECORDDATAIDENTIFIER" in name:
            try:
                return f"0x{int(p.coded_value):04X}"
            except:
                pass

        if "HI" in name:
            hi = getattr(p, "coded_value", None)

        if "LO" in name:
            lo = getattr(p, "coded_value", None)

    if hi is not None and lo is not None:
        try:
            did = (int(hi) << 8) | int(lo)
            return f"0x{did:04X}"
        except:
            return None

    return None


# =========================================================
# Main Validator
# =========================================================
def validate_json_against_pdx(pdx_path, json_path):
    errors = []

    db = odxtools.load_file(pdx_path, use_weakrefs=True)
    db.refresh()

    with open(json_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    # multi ECU JSON support
    def get_ecu_json(name):
        if isinstance(json_data, list):
            for e in json_data:
                if e["ecuInfo"]["ecuVariant"] == name:
                    return e
        else:
            if json_data["ecuInfo"]["ecuVariant"] == name:
                return json_data
        return None

    # ====================================================
    # ECU Loop
    # ====================================================
    for ecu in db.ecus:
        ecu_name = ecu.short_name
        ecu_json = get_ecu_json(ecu_name)

        if ecu_json is None:
            errors.append(f"ECU missing in JSON: {ecu_name}")
            continue

        json_reads = ecu_json.get("read_did_groups", [])
        json_writes = ecu_json.get("write_did_groups", [])

        read_map = {x["did"]: x for x in json_reads}
        write_map = {x["did"]: x for x in json_writes}

        # ====================================================
        # READ DIDs
        # ====================================================
        for svc in ecu.services:
            sid = detect_service_sid(svc)
            if sid != "0x22":
                continue

            did = resolve_did_value(svc)
            if not did:
                continue

            if did not in read_map:
                errors.append(f"Missing READ DID in JSON: ECU={ecu_name} DID={did}")

        # ====================================================
        # WRITE DIDs
        # ====================================================
        for svc in ecu.services:
            sid = detect_service_sid(svc)
            if sid != "0x2E":
                continue

            did = resolve_did_value(svc)
            if not did:
                continue

            if did not in write_map:
                errors.append(f"Missing WRITE DID in JSON: ECU={ecu_name} DID={did}")

        # ====================================================
        # Runtime + Binary decode verification
        # ====================================================
        for svc in json_reads:
            validate_runtime(svc, errors)

        for svc in json_writes:
            runtime = svc.get("runtime", {})
            resp = runtime.get("sampleResponseHex", "")

            parts = resp.split()

            if len(parts) != 3:
                errors.append(f"WRITE DID response length invalid DID={svc.get('did')}")

            if not parts[0].startswith("6E"):
                errors.append(f"WRITE DID wrong positive response SID DID={svc.get('did')}")

    return errors


# =========================================================
# CLI + Reports
# =========================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate generated JSON against PDX")
    parser.add_argument("--pdx", required=True, help="Input PDX file")
    parser.add_argument("--json", required=True, help="Generated JSON file")
    parser.add_argument("--report", default="validation_report.json")
    parser.add_argument("--html", default="validation_report.html")

    args = parser.parse_args()

    errors = validate_json_against_pdx(args.pdx, args.json)

    print("\n================ VALIDATION REPORT ================\n")
    if not errors:
        print("STATUS : PASS")
        print("RESULT : JSON perfectly matches PDX")
    else:
        print("STATUS : FAIL")
        print(f"ERROR COUNT : {len(errors)}\n")
        for e in errors:
            print(" -", e)

    print("\n===================================================\n")

    # JSON Report
    report = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "PASS" if not errors else "FAIL",
        "errorCount": len(errors),
        "errors": errors
    }

    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)

    # HTML Report
    html = "<h1>ODX JSON Validation Report</h1>"
    html += f"<p>Status: {'PASS' if not errors else 'FAIL'}</p>"
    html += f"<p>Error Count: {len(errors)}</p><ul>"
    for e in errors:
        html += f"<li>{e}</li>"
    html += "</ul>"

    with open(args.html, "w") as f:
        f.write(html)

    print(f"JSON report saved: {args.report}")
    print(f"HTML report saved: {args.html}")

    sys.exit(0 if not errors else 1)
