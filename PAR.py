from __future__ import annotations

import uuid
import re
import html
import xml.etree.ElementTree as ET
from dataclasses import asdict
from typing import List, Dict, Tuple, Optional, Set

from models import (
    OdxParam,
    OdxUnit,
    OdxCompuScale,
    OdxCompuMethod,
    OdxDataObjectProp,
    OdxDTC,
    OdxMessage,
    OdxService,
    OdxLayer,
    OdxContainer,
    OdxDatabase,
    OdxTableRow
)

# =============================================================================
# XML helpers
# =============================================================================

def local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def get_all_attrs(el: Optional[ET.Element]) -> Dict[str, str]:
    return dict(el.attrib) if el is not None else {}

def get_attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    return el.attrib.get(name, default) if el is not None else default

def get_text_local(el: Optional[ET.Element], name: str) -> str:
    if el is None:
        return ""
    for c in el:
        if local_name(c.tag) == name:
            return "".join(c.itertext()).strip()
    return ""

def find_child(el: Optional[ET.Element], name: str) -> Optional[ET.Element]:
    if el is None:
        return None
    for c in el:
        if local_name(c.tag) == name:
            return c
    return None

def find_children(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    if el is None:
        return []
    return [c for c in el if local_name(c.tag) == name]

def findall_descendants(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    if el is None:
        return []
    return [n for n in el.iter() if local_name(n.tag) == name]

def extract_coded_value(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    for t in ("CODED-VALUE", "V"):
        for n in el.iter():
            if local_name(n.tag) == t and (n.text or "").strip():
                return n.text.strip()
    return el.attrib.get("CODED-VALUE", "")

# =============================================================================
# STRUCTURE harvesting
# =============================================================================

def harvest_structures(layer_el: ET.Element) -> Tuple[Dict[str, List[ET.Element]], Dict[str, List[ET.Element]]]:
    by_id, by_sn = {}, {}
    for st in findall_descendants(layer_el, "STRUCTURE"):
        sid = get_attr(st, "ID")
        ssn = get_text_local(st, "SHORT-NAME")
        params = find_children(find_child(st, "PARAMS"), "PARAM") or findall_descendants(st, "PARAM")
        if sid:
            by_id[sid] = params
        if ssn:
            by_sn[ssn] = params
    return by_id, by_sn

# =============================================================================
# PARSER
# =============================================================================

class ODXParser:

    # -------------------------------------------------------------------------
    # XML root
    # -------------------------------------------------------------------------

    def parse_xml_bytes(self, raw: bytes) -> ET.Element:
        raw = raw[raw.find(b"<"):]
        try:
            return ET.fromstring(raw)
        except ET.ParseError:
            text = html.unescape(raw.decode("utf-8", errors="ignore"))
            return ET.fromstring(text)

    def _ensure_container(self, root: ET.Element) -> ET.Element:
        if local_name(root.tag) == "DIAG-LAYER-CONTAINER":
            return root
        found = findall_descendants(root, "DIAG-LAYER-CONTAINER")
        if found:
            return found[0]
        print("[odxparser] warning: diag-layer-container not found; using root element")
        return root

    # -------------------------------------------------------------------------
    # PARAM helpers
    # -------------------------------------------------------------------------

    def _get_param_elements(self, parent_el: ET.Element) -> List[ET.Element]:
        params_el = find_child(parent_el, "PARAMS")
        return find_children(params_el, "PARAM") if params_el else find_children(parent_el, "PARAM")

    def _validate_bit_offsets(self, p: OdxParam) -> None:
        warnings = []
        try:
            bp = int(p.bitPosition) if p.bitPosition else None
            bl = int(p.bitLength) if p.bitLength else None
            if bp is not None and bp > 7:
                warnings.append("Invalid BIT-POSITION")
            if bp is not None and bl is not None and bp + bl > 8:
                warnings.append("Bit overflow")
        except ValueError:
            warnings.append("Non-integer bit/byte values")
        if warnings:
            p.attrs.setdefault("__warnings__", []).extend(warnings)

    def flatten_param_tree(self, roots: List[OdxParam]) -> List[OdxParam]:
        out, stack = [], list(roots)
        while stack:
            p = stack.pop(0)
            out.append(p)
            stack[0:0] = getattr(p, "children", [])
        return out

    def _dedup_params_by_path(self, params: List[OdxParam]) -> List[OdxParam]:
        seen, out = set(), []
        for p in params:
            key = f"{p.serviceShortName}|{p.parentName}|{p.shortName}"
            if key not in seen:
                seen.add(key)
                out.append(p)
        return out

    # -------------------------------------------------------------------------
    # PARAM parsing (STRUCTURE + TABLE fixed)
    # -------------------------------------------------------------------------

    def parse_param(
        self,
        param_el: ET.Element,
        parent_type: str,
        parent_path: str,
        layer_name: str,
        service_name: str,
        dop_by_id: Dict[str, OdxDataObjectProp],
        struct_by_id: Dict[str, List[ET.Element]],
        struct_by_sn: Dict[str, List[ET.Element]],
        table_by_id: Dict[str, Dict],
    ) -> OdxParam:

        short = get_text_local(param_el, "SHORT-NAME")
        pid = f"{layer_name}::{service_name}::{parent_type}::{short}::{uuid.uuid4().hex[:8]}"

        p = OdxParam(
            id=pid,
            shortName=short,
            semantic=get_attr(param_el, "SEMANTIC"),
            parentType=parent_type,
            parentName=parent_path,
            layerName=layer_name,
            serviceShortName=service_name,
            attrs=get_all_attrs(param_el),
        )

        # ---- STRUCTURE via DOP ----
        dop_ref = get_attr(find_child(param_el, "DOP-REF"), "ID-REF")
        dop = dop_by_id.get(dop_ref)
        next_path = f"{parent_path}.{short}" if parent_path else short

        if dop and dop.structureParams:
            for ch in dop.structureParams:
                p.children.append(
                    self.parse_param(ch, "STRUCTURE", next_path, layer_name, service_name,
                                     dop_by_id, struct_by_id, struct_by_sn, table_by_id)
                )

        # ---- STRUCTURE via STRUCTURE-REF ----
        struct_ref = find_child(param_el, "STRUCTURE-REF")
        if struct_ref:
            sid = get_attr(struct_ref, "ID-REF")
            ssn = get_text_local(struct_ref, "SHORT-NAME")
            for ch in struct_by_id.get(sid, []) + struct_by_sn.get(ssn, []):
                p.children.append(
                    self.parse_param(ch, "STRUCTURE", next_path, layer_name, service_name,
                                     dop_by_id, struct_by_id, struct_by_sn, table_by_id)
                )

        # ---- TABLE expansion ----
        tbl_ref = find_child(param_el, "TABLE-REF")
        if tbl_ref:
            tbl = table_by_id.get(get_attr(tbl_ref, "ID-REF"))
            if tbl:
                for row in tbl["rows"]:
                    row_p = OdxParam(
                        id=f"{pid}::{row['shortName']}",
                        shortName=row["shortName"],
                        semantic="TABLE-ROW",
                        parentType="TABLE",
                        parentName=next_path,
                        layerName=layer_name,
                        serviceShortName=service_name,
                        attrs={"TABLE": tbl["shortName"]},
                    )
                    for ch in row["structParams"]:
                        row_p.children.append(
                            self.parse_param(ch, "STRUCTURE", f"{next_path}.{row['shortName']}",
                                             layer_name, service_name,
                                             dop_by_id, struct_by_id, struct_by_sn, table_by_id)
                        )
                    p.children.append(row_p)

        self._validate_bit_offsets(p)
        return p

    # -------------------------------------------------------------------------
    # LAYER parsing (REQUEST / POS / NEG fixed)
    # -------------------------------------------------------------------------

    def _parse_layer(self, layer_el: ET.Element, layer_type: str) -> OdxLayer:
        layer_sn = get_text_local(layer_el, "SHORT-NAME")

        struct_by_id, struct_by_sn = harvest_structures(layer_el)
        dop_by_id = {}

        for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
            dop_by_id[get_attr(d, "ID")] = OdxDataObjectProp(
                id=get_attr(d, "ID"),
                shortName=get_text_local(d, "SHORT-NAME"),
                structureParams=struct_by_id.get(get_attr(d, "ID"), [])
            )

        table_by_id = {}
        for t in findall_descendants(layer_el, "TABLE"):
            rows = []
            for tr in findall_descendants(t, "TABLE-ROW"):
                ref = find_child(tr, "STRUCTURE-REF")
                rows.append({
                    "shortName": get_text_local(tr, "SHORT-NAME"),
                    "structParams": struct_by_id.get(get_attr(ref, "ID-REF"), []) if ref else []
                })
            table_by_id[get_attr(t, "ID")] = {
                "shortName": get_text_local(t, "SHORT-NAME"),
                "rows": rows
            }

        services = []
        for svc in findall_descendants(layer_el, "DIAG-SERVICE"):
            svc_sn = get_text_local(svc, "SHORT-NAME")
            req_el = find_child(svc, "REQUEST")
            req = None

            if req_el:
                params = [
                    self.parse_param(p, "REQUEST", svc_sn, layer_sn, svc_sn,
                                     dop_by_id, struct_by_id, struct_by_sn, table_by_id)
                    for p in self._get_param_elements(req_el)
                ]
                req = OdxMessage(shortName="REQ", params=params)

            services.append(OdxService(shortName=svc_sn, request=req))

        return OdxLayer(
            layerType=layer_type,
            id=get_attr(layer_el, "ID"),
            shortName=layer_sn,
            services=services,
        )

    # -------------------------------------------------------------------------
    # CONTAINER + MERGE
    # -------------------------------------------------------------------------

    def parse_container(self, root: ET.Element) -> OdxContainer:
        root = self._ensure_container(root)
        c = OdxContainer()
        for ev in findall_descendants(root, "ECU-VARIANT"):
            c.ecuVariants.append(self._parse_layer(ev, "ECU-VARIANT"))
        return c

    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
        db = OdxDatabase()
        for c in containers:
            db.ecuVariants.extend(c.ecuVariants)

        for lay in db.ecuVariants:
            for svc in lay.services:
                flat = self.flatten_param_tree(svc.request.params if svc.request else [])
                flat = self._dedup_params_by_path(flat)
                db.allParams.extend(flat)

        return db
