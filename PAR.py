# -*- coding: utf-8 -*-
from __future__ import annotations

import uuid
import html
import logging
import xml.etree.ElementTree as ET
from dataclasses import is_dataclass, fields
from typing import List, Dict, Tuple, Optional, Set
from collections.abc import KeysView, ValuesView, ItemsView

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

logger = logging.getLogger(__name__)

# =====================================================================================
# safe_asdict
# =====================================================================================

def safe_asdict(obj):
    if is_dataclass(obj):
        return {f.name: safe_asdict(getattr(obj, f.name)) for f in fields(obj)}
    if isinstance(obj, dict):
        return {safe_asdict(k): safe_asdict(v) for k, v in obj.items()}
    if isinstance(obj, (KeysView, ValuesView, ItemsView)):
        return list(obj)
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [safe_asdict(v) for v in obj]
    if isinstance(obj, ET.Element):
        return obj
    return obj

# =====================================================================================
# XML helpers (Pylance-safe)
# =====================================================================================

def local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

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
    return [x for x in el.iter() if local_name(x.tag) == name]

def get_text_local(el: Optional[ET.Element], name: str) -> str:
    if el is None:
        return ""
    for c in el:
        if local_name(c.tag) == name:
            txt = "".join(c.itertext())
            return txt.strip() if isinstance(txt, str) else ""
    return ""

def get_attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    if el is None:
        return default
    v = el.attrib.get(name)
    return v if isinstance(v, str) else default

def get_attr_ci(el: Optional[ET.Element], *names: str) -> str:
    if el is None:
        return ""
    low = {k.lower(): v for k, v in el.attrib.items()}
    for n in names:
        v = low.get(n.lower())
        if isinstance(v, str):
            return v
    return ""

def first_text(el: Optional[ET.Element], names: List[str]) -> str:
    if el is None:
        return ""
    for n in names:
        for x in el.iter():
            if local_name(x.tag) == n:
                txt = x.text
                if isinstance(txt, str):
                    txt = txt.strip()
                    if txt:
                        return txt
    return ""

def extract_coded_value(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    return (
        first_text(el, ["CODED-VALUE"])
        or first_text(el, ["V"])
        or get_attr_ci(el, "CODED-VALUE")
        or ""
    )

# =====================================================================================
# Robust XML bytes parsing
# =====================================================================================

def _try_parse_bytes(raw: bytes) -> ET.Element:
    i = raw.find(b"<")
    raw = raw if i <= 0 else raw[i:]
    for enc in ("utf-8", "utf-16", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            txt = raw.decode(enc, errors="ignore")
            txt = html.unescape(txt)
            txt = txt[txt.find("<"):]
            return ET.fromstring(txt.encode("utf-8"))
        except Exception:
            continue
    return ET.fromstring(raw)

# =====================================================================================
# STRUCTURE harvesting
# =====================================================================================

def harvest_structures(layer_el: ET.Element) -> Tuple[Dict[str, List[ET.Element]], Dict[str, List[ET.Element]]]:
    by_id: Dict[str, List[ET.Element]] = {}
    by_sn: Dict[str, List[ET.Element]] = {}

    for s in findall_descendants(layer_el, "STRUCTURE"):
        sid = get_attr(s, "ID")
        ssn = get_text_local(s, "SHORT-NAME")
        pb = find_child(s, "PARAMS")
        params = find_children(pb, "PARAM") if pb else findall_descendants(s, "PARAM")

        if sid:
            by_id[sid] = params
        if ssn:
            by_sn[ssn] = params

    return by_id, by_sn

# =====================================================================================
# MAIN PARSER
# =====================================================================================

class ODXParser:

    # ------------------------------------------------------------------
    # Public entrypoints
    # ------------------------------------------------------------------

    def parse_odx(self, file_path: str) -> Tuple[str, OdxContainer]:
        with open(file_path, "rb") as f:
            raw = f.read()
        name = file_path.split("/")[-1].split("\\")[-1]
        return self.parse_odx_bytes(name, raw)

    def parse_odx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        root = _try_parse_bytes(content)
        return filename, self.parse_container(root)

    # ------------------------------------------------------------------
    # Container / merge
    # ------------------------------------------------------------------

    def parse_container(self, root: ET.Element) -> OdxContainer:
        cont = OdxContainer()
        for ev in findall_descendants(root, "ECU-VARIANT"):
            cont.ecuVariants.append(self._parse_layer(ev, "ECU-VARIANT"))
        return cont

    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
        db = OdxDatabase()
        for c in containers:
            db.ecuVariants.extend(c.ecuVariants)

        for _ in range(2):
            for layer in db.ecuVariants:
                self._resolve_links_for_layer(layer)
                self._dedup_services(layer)

        for layer in db.ecuVariants:
            for svc in layer.services:
                if svc.request:
                    for p in svc.request.params:
                        p.layerName = layer.shortName
                        db.allParams.append(p)
                for msg in svc.posResponses + svc.negResponses:
                    for p in msg.params:
                        p.layerName = layer.shortName
                        db.allParams.append(p)

        return db

    # ------------------------------------------------------------------
    # PARAM parsing (WITH TABLE-KEY)
    # ------------------------------------------------------------------

    def _try_parse_param(self, *a, **kw) -> Optional[OdxParam]:
        try:
            return self.parse_param(*a, **kw)
        except AssertionError:
            raise
        except Exception as e:
            logger.warning("PARAM skipped: %s", e)
            return None

    def parse_param(
        self,
        param_el: ET.Element,
        parentType: str,
        parentPath: str,
        layerName: str,
        serviceShortName: str,
        dop_by_id: Dict[str, OdxDataObjectProp],
        dop_by_sn: Dict[str, OdxDataObjectProp],
        dop_meta_by_id: Dict[str, Dict],
        struct_by_id: Dict[str, List[ET.Element]],
        struct_by_sn: Dict[str, List[ET.Element]],
        table_by_id: Dict[str, Dict],
    ) -> OdxParam:

        short = get_text_local(param_el, "SHORT-NAME")
        assert short, "PARAM without SHORT-NAME"

        diag = find_child(param_el, "DIAG-CODED-TYPE")
        phys = find_child(param_el, "PHYSICAL-TYPE")
        dop_ref = find_child(param_el, "DOP-REF")
        dop_sn = find_child(param_el, "DOP-SNREF")

        pid = f"{layerName}::{serviceShortName}::{parentType}::{short}::{uuid.uuid4().hex[:6]}"

        p = OdxParam(
            id=pid,
            shortName=short,
            longName=get_text_local(param_el, "LONG-NAME"),
            description=get_text_local(param_el, "DESC"),
            semantic=get_attr(param_el, "SEMANTIC"),
            bytePosition=get_text_local(param_el, "BYTE-POSITION"),
            bitPosition=get_text_local(param_el, "BIT-POSITION"),
            bitLength=get_text_local(diag, "BIT-LENGTH"),
            baseDataType=get_attr(diag, "BASE-DATA-TYPE"),
            physicalBaseType=get_attr(phys, "BASE-DATA-TYPE"),
            isHighLowByteOrder=(
                get_attr(diag, "IS-HIGHLOW-BYTE-ORDER")
                or get_attr(diag, "IS-HIGH-LOW-BYTE-ORDER")
            ),
            codedConstValue=extract_coded_value(param_el),
            dopRefId=get_attr(dop_ref, "ID-REF"),
            dopSnRefName=get_text_local(dop_sn, "SHORT-NAME"),
            parentType=parentType,
            parentName=parentPath,
            layerName=layerName,
            serviceShortName=serviceShortName,
            attrs=dict(param_el.attrib),
        )

        next_path = f"{parentPath}.{short}" if parentPath else short

        # ---------- DOP STRUCTURE ----------
        dop = dop_by_id.get(p.dopRefId) or dop_by_sn.get(p.dopSnRefName)
        if dop and dop.structureParams:
            for c in dop.structureParams:
                cp = self._try_parse_param(
                    c, "STRUCTURE", next_path,
                    layerName, serviceShortName,
                    dop_by_id, dop_by_sn, dop_meta_by_id,
                    struct_by_id, struct_by_sn, table_by_id
                )
                if cp:
                    p.children.append(cp)

        # ---------- TABLE-KEY ----------
        table_key_el = find_child(diag, "TABLE-KEY") if diag else None
        if table_key_el is not None:
            table_ref = find_child(table_key_el, "TABLE-REF")
            table_id = get_attr(table_ref, "ID-REF") if table_ref else ""

            table = table_by_id.get(table_id)
            assert table is not None, f"TABLE-KEY references missing TABLE id={table_id}"

            key_value = extract_coded_value(table_key_el)
            assert key_value, f"Empty TABLE-KEY for param {p.shortName}"

            matched = None
            for r in table["rows"]:
                if r["key"] == key_value:
                    matched = r
                    break

            assert matched is not None, (
                f"No TABLE-ROW for key={key_value} table={table_id}"
            )

            for c in matched["structParams"]:
                cp = self._try_parse_param(
                    c, "STRUCTURE", next_path,
                    layerName, serviceShortName,
                    dop_by_id, dop_by_sn, dop_meta_by_id,
                    struct_by_id, struct_by_sn, table_by_id
                )
                if cp:
                    p.children.append(cp)

        return p

    # ------------------------------------------------------------------
    # LAYER parsing (TABLE harvesting)
    # ------------------------------------------------------------------

    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
        layer_short = get_text_local(layer_el, "SHORT-NAME")

        struct_by_id, struct_by_sn = harvest_structures(layer_el)

        dop_by_id: Dict[str, OdxDataObjectProp] = {}
        dop_by_sn: Dict[str, OdxDataObjectProp] = {}

        for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
            dop = OdxDataObjectProp(
                id=get_attr(d, "ID"),
                shortName=get_text_local(d, "SHORT-NAME"),
                longName=get_text_local(d, "LONG-NAME"),
                description=get_text_local(d, "DESC"),
                structureParams=[]
            )
            s = find_child(d, "STRUCTURE")
            if s:
                dop.structureParams = find_children(find_child(s, "PARAMS"), "PARAM")
            dop_by_id[dop.id] = dop
            dop_by_sn[dop.shortName] = dop

        # ---------- TABLE harvesting ----------
        table_by_id: Dict[str, Dict] = {}
        for tbl in findall_descendants(layer_el, "TABLE"):
            tid = get_attr(tbl, "ID")
            key_dop_ref = get_attr(find_child(tbl, "KEY-DOP-REF"), "ID-REF")

            rows = []
            for tr in find_children(tbl, "TABLE-ROW"):
                s = find_child(tr, "STRUCTURE")
                pb = find_child(s, "PARAMS") if s else None
                rows.append({
                    "key": get_attr(tr, "KEY"),
                    "structParams": find_children(pb, "PARAM") if pb else [],
                })

            if tid:
                assert rows, f"TABLE {tid} has no TABLE-ROW"
                table_by_id[tid] = {
                    "keyDopRefId": key_dop_ref,
                    "rows": rows,
                }

        services: List[OdxService] = []

        for svc in findall_descendants(layer_el, "DIAG-SERVICE"):
            svc_short = get_text_local(svc, "SHORT-NAME")

            req = None
            req_el = find_child(svc, "REQUEST")
            if req_el:
                params = []
                for p in find_children(find_child(req_el, "PARAMS"), "PARAM"):
                    rp = self._try_parse_param(
                        p, "REQUEST", svc_short,
                        layer_short, svc_short,
                        dop_by_id, dop_by_sn, {},
                        struct_by_id, struct_by_sn, table_by_id
                    )
                    if rp:
                        params.append(rp)
                req = OdxMessage(id=get_attr(req_el, "ID"), shortName="REQUEST", params=params)

            pos_resps: List[OdxMessage] = []
            neg_resps: List[OdxMessage] = []

            for el in find_children(svc, "POS-RESPONSE"):
                params = []
                for p in findall_descendants(el, "PARAM"):
                    rp = self._try_parse_param(
                        p, "POS_RESPONSE", f"{svc_short}.POS",
                        layer_short, svc_short,
                        dop_by_id, dop_by_sn, {},
                        struct_by_id, struct_by_sn, table_by_id
                    )
                    if rp:
                        params.append(rp)
                pos_resps.append(OdxMessage(id=get_attr(el, "ID"), shortName="POS", params=params))

            for el in find_children(svc, "NEG-RESPONSE"):
                params = []
                for p in findall_descendants(el, "PARAM"):
                    rp = self._try_parse_param(
                        p, "NEG_RESPONSE", f"{svc_short}.NEG",
                        layer_short, svc_short,
                        dop_by_id, dop_by_sn, {},
                        struct_by_id, struct_by_sn, table_by_id
                    )
                    if rp:
                        params.append(rp)
                neg_resps.append(OdxMessage(id=get_attr(el, "ID"), shortName="NEG", params=params))

            services.append(
                OdxService(
                    id=get_attr(svc, "ID"),
                    shortName=svc_short,
                    request=req,
                    posResponses=pos_resps,
                    negResponses=neg_resps,
                )
            )

        return OdxLayer(
            layerType=layerType,
            id=get_attr(layer_el, "ID"),
            shortName=layer_short,
            services=services,
        )

    # ------------------------------------------------------------------
    # Inheritance helpers (minimal, safe)
    # ------------------------------------------------------------------

    def _resolve_links_for_layer(
        self,
        layer: OdxLayer,
        id_map: Optional[Dict[str, object]] = None,
        visited: Optional[Set[str]] = None,
    ) -> None:
        return

    def _dedup_services(self, layer: OdxLayer) -> None:
        seen = set()
        uniq = []
        for svc in layer.services:
            if svc.shortName in seen:
                continue
            seen.add(svc.shortName)
            uniq.append(svc)
        layer.services = uniq
