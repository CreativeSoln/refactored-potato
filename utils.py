from typing import Any
from odxtools.database import Database

def normalize_name(text: str) -> str:
    if not text:
        return ""
    import re
    text = text.replace(".", "_")
    return re.sub(r"[^A-Za-z0-9_]", "_", text).upper()


def safe_resolve(ref, db: Database):
    try:
        if not ref:
            return None
        return db.odxlinks.resolve_lenient(ref)
    except Exception:
        return None


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
