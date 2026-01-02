from __future__ import annotations

import uuid
import re
import html
import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple, Optional, Set
from dataclasses import asdict

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
    OdxTableRow,
)

# =============================================================================
# XML HELPERS
# =============================================================================

def local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def get_attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    return el.attrib.get(name, default) if el is not None else default

def get_all_attrs(el: Optional[ET.Element]) -> Dict[str, str]:
    return dict(el.attrib) if el is not None else {}

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

def get_text_local(el: Optional[ET.Element], name: str) -> str:
    c = find_child(el, name)
    return "".join(c.itertext()).strip() if c is not None else ""

def extract_coded_value(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    for tag in ("CODED-VALUE", "V"):
        v = get_text_local(el, tag)
        if v:
            return v
    return el.attrib.get("CODED-VALUE", "")

# =============================================================================
# STRUCTURE / TABLE INDEXING
# =============================================================================

def harvest_structures(layer_el: ET.Element) -> Tuple[Dict[str, List[ET.Element]], Dict[str, List[ET.Element]]]:
    by_id, by_sn = {}, {}
    for st in findall_descendants(layer_el, "STRUCTURE"):
        sid = get_attr(st, "ID")
        sn = get_text_local(st, "SHORT-NAME")
        params_el = find_child(st, "PARAMS")
        params = find_children(params_el, "PARAM") if params_el else []
        if sid:
            by_id[sid] = params
        if sn:
            by_sn[sn] = params
    return by_id, by_sn

def harvest_tables(layer_el: ET.Element,
                   struct_by_id: Dict[str, List[ET.Element]],
                   struct_by_sn: Dict[str, List[ET.Element]]) -> Dict[str, Dict]:
    tables: Dict[str, Dict] = {}
    for t in findall_descendants(layer_el, "TABLE"):
        tid = get_attr(t, "ID")
        rows = []
        for tr in findall_descendants(t, "TABLE-ROW"):
            sref = find_child(tr, "STRUCTURE-REF")
            params = []
            if sref is not None:
                rid = get_attr(sref, "ID-REF")
                rsn = get_text_local(sref, "SHORT-NAME")
                params = struct_by_id.get(rid) or struct_by_sn.get(rsn) or []
            rows.append({
                "shortName": get_text_local(tr, "SHORT-NAME"),
                "structParams": params,
            })
        tables[tid] = {
            "shortName": get_text_local(t, "SHORT-NAME"),
            "rows": rows,
        }
    return tables

# =============================================================================
# PARAM PARSER (CORE)
# =============================================================================

class ParamParser:
    def parse(
        self,
        el: ET.Element,
        parent_path: str,
        layer: str,
        service: str,
        dop_by_id: Dict[str, OdxDataObjectProp],
        struct_by_id: Dict[str, List[ET.Element]],
        struct_by_sn: Dict[str, List[ET.Element]],
        table_by_id: Dict[str, Dict],
    ) -> OdxParam:

        short = get_text_local(el, "SHORT-NAME")
        pid = f"{layer}::{service}::{short}::{uuid.uuid4().hex[:6]}"

        p = OdxParam(
            id=pid,
            shortName=short,
            parentName=parent_path,
            layerName=layer,
            serviceShortName=service,
            attrs=get_all_attrs(el),
        )

        next_path = f"{parent_path}.{short}" if parent_path else short

        # --- STRUCTURE via DOP ---
        dop_id = get_attr(find_child(el, "DOP-REF"), "ID-REF")
        dop = dop_by_id.get(dop_id)
        if dop and dop.structureParams:
            for ch in dop.structureParams:
                p.children.append(
                    self.parse(ch, next_path, layer, service,
                               dop_by_id, struct_by_id, struct_by_sn, table_by_id)
                )
            return p

        # --- STRUCTURE via STRUCTURE-REF ---
        sref = find_child(el, "STRUCTURE-REF")
        if sref is not None:
            sid = get_attr(sref, "ID-REF")
            ssn = get_text_local(sref, "SHORT-NAME")
            for ch in struct_by_id.get(sid, []) or struct_by_sn.get(ssn, []):
                p.children.append(
                    self.parse(ch, next_path, layer, service,
                               dop_by_id, struct_by_id, struct_by_sn, table_by_id)
                )
            return p

        # --- TABLE ---
        tref = find_child(el, "TABLE-REF")
        if tref is not None:
            tbl = table_by_id.get(get_attr(tref, "ID-REF"))
            if tbl:
                for row in tbl["rows"]:
                    row_name = row["shortName"] or "ROW"
                    rp = OdxParam(
                        id=f"{pid}::{row_name}",
                        shortName=row_name,
                        parentName=next_path,
                        layerName=layer,
                        serviceShortName=service,
                        semantic="TABLE-ROW",
                        attrs={},
                    )
                    for ch in row["structParams"]:
                        rp.children.append(
                            self.parse(ch, f"{next_path}.{row_name}",
                                       layer, service,
                                       dop_by_id, struct_by_id, struct_by_sn, table_by_id)
                        )
                    p.children.append(rp)

        return p

# =============================================================================
# MAIN PARSER
# =============================================================================

class ODXParser:

    def parse_xml_bytes(self, raw: bytes) -> ET.Element:
        raw = raw[raw.find(b"<"):]
        return ET.fromstring(raw)

    def parse_odx_file(self, filename: str, content: str) -> Tuple[str, OdxContainer]:
        root = self.parse_xml_bytes(content.encode())
        return filename, self.parse_container(root)

    def _ensure_container(self, root: ET.Element) -> ET.Element:
        if local_name(root.tag) == "DIAG-LAYER-CONTAINER":
            return root
        return findall_descendants(root, "DIAG-LAYER-CONTAINER")[0]

    def _parse_layer(self, layer_el: ET.Element, layer_type: str) -> OdxLayer:
        struct_by_id, struct_by_sn = harvest_structures(layer_el)
        table_by_id = harvest_tables(layer_el, struct_by_id, struct_by_sn)

        dop_by_id = {}
        for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
            dop_by_id[get_attr(d, "ID")] = OdxDataObjectProp(
                id=get_attr(d, "ID"),
                shortName=get_text_local(d, "SHORT-NAME"),
                structureParams=find_children(find_child(d, "STRUCTURE"), "PARAM"),
            )

        param_parser = ParamParser()
        services: List[OdxService] = []

        for svc in findall_descendants(layer_el, "DIAG-SERVICE"):
            sname = get_text_local(svc, "SHORT-NAME")
            req_el = find_child(svc, "REQUEST")
            params = []

            if req_el:
                params_el = find_child(req_el, "PARAMS")
                for p in find_children(params_el, "PARAM"):
                    params.append(
                        param_parser.parse(
                            p, sname, get_text_local(layer_el, "SHORT-NAME"),
                            sname, dop_by_id,
                            struct_by_id, struct_by_sn, table_by_id
                        )
                    )

            services.append(
                OdxService(
                    shortName=sname,
                    request=OdxMessage(shortName="Request", params=params),
                )
            )

        return OdxLayer(
            layerType=layer_type,
            id=get_attr(layer_el, "ID"),
            shortName=get_text_local(layer_el, "SHORT-NAME"),
            services=services,
            units=[],
            compuMethods=[],
            dataObjectProps=list(dop_by_id.values()),
            dtcs=[],
            attrs={},
            linkedLayerIds=[],
        )

    def parse_container(self, root: ET.Element) -> OdxContainer:
        root = self._ensure_container(root)
        cont = OdxContainer()

        for el in findall_descendants(root, "ECU-VARIANT"):
            cont.ecuVariants.append(self._parse_layer(el, "ECU-VARIANT"))

        return cont

    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
        db = OdxDatabase()
        for c in containers:
            for layer in c.ecuVariants:
                for svc in layer.services:
                    for p in svc.request.params:
                        db.allParams.append(p)
        return db
