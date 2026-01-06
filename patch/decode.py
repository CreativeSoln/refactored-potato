"""
binary_decode.py

Binary decoding utilities for ODX PARAM trees.
Works with parsed output from parser.py and models.py.

Supported:
- Bit / byte aligned params
- STRUCTURE params
- TABLE-KEY params
- Linear & enum COMPU-METHODs
"""

from typing import Dict, List, Any


# ============================================================================
# PARAM TREE UTILITIES
# ============================================================================

def flatten_params(params) -> List:
    """
    Flatten PARAM tree into leaf PARAMs only.
    """
    out = []
    for p in params or []:
        if getattr(p, "children", None):
            out.extend(flatten_params(p.children))
        else:
            out.append(p)
    return out


# ============================================================================
# BIT EXTRACTION
# ============================================================================

def extract_bits(
    data: bytes,
    byte_pos: int,
    bit_pos: int,
    bit_len: int,
) -> int:
    """
    Extract arbitrary bit-length value from byte array.
    MSB-first, ODX-compliant.
    """
    start_bit = (byte_pos * 8) + bit_pos
    value = 0

    for i in range(bit_len):
        bit_index = start_bit + i
        byte_index = bit_index // 8
        bit_in_byte = 7 - (bit_index % 8)

        if byte_index >= len(data):
            break

        bit = (data[byte_index] >> bit_in_byte) & 1
        value = (value << 1) | bit

    return value


# ============================================================================
# COMPU-METHOD HANDLING
# ============================================================================

def apply_compu_method(raw: int, compu_method) -> Any:
    """
    Apply COMPU-METHOD scaling or enum mapping.
    """
    if not compu_method or not getattr(compu_method, "scales", None):
        return raw

    for scale in compu_method.scales:
        lo = getattr(scale, "lowerLimit", None)
        hi = getattr(scale, "upperLimit", None)

        if lo is not None and raw < lo:
            continue
        if hi is not None and raw > hi:
            continue

        # ENUM / TEXT
        if getattr(scale, "textValue", None) is not None:
            return scale.textValue

        # LINEAR
        factor = getattr(scale, "factor", None)
        offset = getattr(scale, "offset", None)

        if factor is not None:
            return (raw * factor) + (offset or 0)

    return raw


# ============================================================================
# SINGLE PARAM DECODE
# ============================================================================

def decode_param(
    param,
    payload: bytes,
    compu_methods_by_id: Dict[str, Any],
):
    """
    Decode a single PARAM from payload.
    """
    if not param.bitLength:
        return None

    try:
        raw = extract_bits(
            payload,
            int(param.bytePosition or 0),
            int(param.bitPosition or 0),
            int(param.bitLength),
        )
    except Exception:
        return None

    param.rawValue = raw
    param.rawHex = f"0x{raw:X}"

    cm = None
    if getattr(param, "compuMethodRef", None):
        cm = compu_methods_by_id.get(param.compuMethodRef)

    value = apply_compu_method(raw, cm)
    param.value = value

    return value


# ============================================================================
# RESPONSE DECODING
# ============================================================================

def decode_response(
    service,
    response_bytes: bytes,
    compu_methods_by_id: Dict[str, Any],
    strip_sid_did: bool = True,
) -> Dict[str, Any]:
    """
    Decode a positive response message for a service.

    Returns:
        { "Full.Param.Path": decoded_value }
    """

    if not service.posResponses:
        return {}

    # Remove SID + DID (default UDS: 1 + 2 bytes)
    payload = response_bytes
    if strip_sid_did and len(response_bytes) >= 3:
        payload = response_bytes[3:]

    params = flatten_params(service.posResponses[0].params)
    decoded: Dict[str, Any] = {}

    for p in params:
        val = decode_param(p, payload, compu_methods_by_id)
        if val is not None:
            key = f"{p.parentName}.{p.shortName}" if p.parentName else p.shortName
            decoded[key] = val

    return decoded


# ============================================================================
# TABLE-KEY SUPPORT (OPTIONAL)
# ============================================================================

def select_table_row(table_param, payload: bytes, compu_methods_by_id):
    """
    Evaluate TABLE-KEY discriminator and return active TABLE-ROW.
    """
    if not table_param.children:
        return None

    key_param = table_param.children[0]  # KEY is always first
    key_value = decode_param(key_param, payload, compu_methods_by_id)

    for row in table_param.children[1:]:
        row_key = row.attrs.get("TABLE-ROW-KEY")
        if row_key is not None and str(row_key) == str(key_value):
            return row

    return None



from binary_decode import decode_response

decoded = decode_response(
    service=my_service,
    response_bytes=bytes.fromhex("62 21 D1 0F 32 0F 35 0F 38 0F 3B"),
    compu_methods_by_id=db.compuMethodsById,
)

for k, v in decoded.items():
    print(k, "=", v)
