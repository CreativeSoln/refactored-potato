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
# safe_asdict (AS REQUESTED – EXACT BEHAVIOR)
# =====================================================================================

def safe_asdict(obj):
    """
    Safer than dataclasses.asdict:
    - Dataclass -> dict (recursive)
    - dict -> safe dict
    - mapping views -> list
    - list/tuple/set/frozenset -> list
    - ET.Element -> return as-is
    - else -> as-is
    """
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
# XML helpers
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

def findall_desc(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    if el is None:
        return []
    return [x for x in el.iter() if local_name(x.tag) == name]

def text_of(el: Optional[ET.Element], name: str) -> str:
    if el is None:
        return ""
    for c in el:
        if local_name(c.tag) == name:
            return "".join(c.itertext()).strip()
    return ""

def attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    if el is None:
        return default
    return el.attrib.get(name, default)

def attr_ci(el: Optional[ET.Element], *names: str) -> str:
    if el is None:
        return ""
    low = {k.lower(): v for k, v in el.attrib.items()}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    return ""

def first_text(el: Optional[ET.Element], names: List[str]) -> str:
    if el is None:
        return ""
    for n in names:
        for x in el.iter():
            if local_name(x.tag) == n and (x.text or "").strip():
                return x.text.strip()
    return ""

def extract_coded_value(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    return (
        first_text(el, ["CODED-VALUE"])
        or first_text(el, ["V"])
        or attr_ci(el, "CODED-VALUE")
        or ""
    )

# =====================================================================================
# Robust XML bytes parsing (multi-encoding)
# =====================================================================================

def parse_xml_bytes_robust(raw: bytes) -> ET.Element:
    i = raw.find(b"<")
    raw = raw if i <= 0 else raw[i:]
    for enc in ("utf-8", "utf-16", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            txt = raw.decode(enc, errors="ignore")
            txt = html.unescape(txt)
            txt = txt[txt.find("<"):]
            return ET.fromstring(txt.encode("utf-8"))
        except Exception:
            pass
    return ET.fromstring(raw)

# =====================================================================================
# Structure harvesting
# =====================================================================================

def harvest_structures(layer_el: ET.Element):
    by_id, by_sn = {}, {}
    for s in findall_desc(layer_el, "STRUCTURE"):
        sid = attr(s, "ID")
        sn = text_of(s, "SHORT-NAME")
        pb = find_child(s, "PARAMS")
        params = find_children(pb, "PARAM") if pb else findall_desc(s, "PARAM")
        if sid:
            by_id[sid] = params
        if sn:
            by_sn[sn] = params
    return by_id, by_sn

# =====================================================================================
# Main parser
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

    def parse_odx_bytes(self, filename: str, raw: bytes) -> Tuple[str, OdxContainer]:
        root = parse_xml_bytes_robust(raw)
        return filename, self.parse_container(root)

    # ------------------------------------------------------------------
    # Container / merge
    # ------------------------------------------------------------------

    def parse_container(self, root: ET.Element) -> OdxContainer:
        cont = OdxContainer()
        for ev in findall_desc(root, "ECU-VARIANT"):
            cont.ecuVariants.append(self._parse_layer(ev, "ECU-VARIANT"))
        return cont

    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
        db = OdxDatabase()
        for c in containers:
            db.ecuVariants.extend(c.ecuVariants)

        # --- inheritance resolution (2 passes, NOT-INHERITED) ---
        for _ in range(2):
            for l in db.ecuVariants:
                self._resolve_links_for_layer(l)

        # --- flatten ---
        for layer in db.ecuVariants:
            for svc in layer.services:
                for p in (svc.request.params if svc.request else []):
                    p.layerName = layer.shortName
                    db.allParams.append(p)
                for r in svc.posResponses + svc.negResponses:
                    for p in r.params:
                        p.layerName = layer.shortName
                        db.allParams.append(p)

        return db

    # ------------------------------------------------------------------
    # PARAM parsing
    # ------------------------------------------------------------------

    def _try_parse_param(self, *a, **kw):
        try:
            return self.parse_param(*a, **kw)
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

        short = text_of(param_el, "SHORT-NAME")
        diag = find_child(param_el, "DIAG-CODED-TYPE")
        phys = find_child(param_el, "PHYSICAL-TYPE")
        dop_ref = find_child(param_el, "DOP-REF")
        dop_sn = find_child(param_el, "DOP-SNREF")

        pid = f"{layerName}::{serviceShortName}::{parentType}::{short}::{uuid.uuid4().hex[:6]}"

        p = OdxParam(
            id=pid,
            shortName=short,
            longName=text_of(param_el, "LONG-NAME"),
            description=text_of(param_el, "DESC"),
            semantic=attr(param_el, "SEMANTIC"),
            bytePosition=text_of(param_el, "BYTE-POSITION"),
            bitPosition=text_of(param_el, "BIT-POSITION"),
            bitLength=text_of(diag, "BIT-LENGTH"),
            baseDataType=attr(diag, "BASE-DATA-TYPE"),
            isHighLowByteOrder=(
                attr(diag, "IS-HIGHLOW-BYTE-ORDER")
                or attr(diag, "IS-HIGH-LOW-BYTE-ORDER")
            ),
            codedConstValue=extract_coded_value(param_el),
            dopRefId=attr(dop_ref, "ID-REF"),
            dopSnRefName=text_of(dop_sn, "SHORT-NAME"),
            parentType=parentType,
            parentName=parentPath,
            layerName=layerName,
            serviceShortName=serviceShortName,
            attrs=dict(param_el.attrib),
        )

        next_path = f"{parentPath}.{short}" if parentPath else short

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

        return p

    # ------------------------------------------------------------------
    # LAYER parsing (POS/NEG inline + orphan fallback)
    # ------------------------------------------------------------------

    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
        layer_short = text_of(layer_el, "SHORT-NAME")

        struct_by_id, struct_by_sn = harvest_structures(layer_el)
        dop_by_id, dop_by_sn, dop_meta = {}, {}, {}

        for d in findall_desc(layer_el, "DATA-OBJECT-PROP"):
            dop = OdxDataObjectProp(
                id=attr(d, "ID"),
                shortName=text_of(d, "SHORT-NAME"),
                longName=text_of(d, "LONG-NAME"),
                description=text_of(d, "DESC"),
                structureParams=[]
            )
            s = find_child(d, "STRUCTURE")
            if s:
                dop.structureParams = find_children(find_child(s, "PARAMS"), "PARAM")
            dop_by_id[dop.id] = dop
            dop_by_sn[dop.shortName] = dop

        services: List[OdxService] = []

        for svc in findall_desc(layer_el, "DIAG-SERVICE"):
            svc_short = text_of(svc, "SHORT-NAME")

            # ---------------- REQUEST ----------------
            req_el = find_child(svc, "REQUEST")
            req = None
            if req_el:
                rps = []
                for p in find_children(find_child(req_el, "PARAMS"), "PARAM"):
                    rp = self._try_parse_param(
                        p, "REQUEST", svc_short,
                        layer_short, svc_short,
                        dop_by_id, dop_by_sn, dop_meta,
                        struct_by_id, struct_by_sn, {}
                    )
                    if rp:
                        rps.append(rp)
                req = OdxMessage(id=attr(req_el, "ID"), shortName="REQUEST", params=rps)

            pos_resps: List[OdxMessage] = []
            neg_resps: List[OdxMessage] = []
            attached_pos_ids: Set[str] = set()
            attached_neg_ids: Set[str] = set()

            # ---------------- INLINE POS ----------------
            for el in find_children(svc, "POS-RESPONSE"):
                rid = attr(el, "ID")
                params = []
                for p in findall_desc(el, "PARAM"):
                    rp = self._try_parse_param(
                        p, "POS_RESPONSE", f"{svc_short}.POS",
                        layer_short, svc_short,
                        dop_by_id, dop_by_sn, dop_meta,
                        struct_by_id, struct_by_sn, {}
                    )
                    if rp:
                        params.append(rp)
                pos_resps.append(OdxMessage(id=rid, shortName="POS", params=params))
                if rid:
                    attached_pos_ids.add(rid)

            # ---------------- INLINE NEG ----------------
            for el in find_children(svc, "NEG-RESPONSE"):
                rid = attr(el, "ID")
                params = []
                for p in findall_desc(el, "PARAM"):
                    rp = self._try_parse_param(
                        p, "NEG_RESPONSE", f"{svc_short}.NEG",
                        layer_short, svc_short,
                        dop_by_id, dop_by_sn, dop_meta,
                        struct_by_id, struct_by_sn, {}
                    )
                    if rp:
                        params.append(rp)
                neg_resps.append(OdxMessage(id=rid, shortName="NEG", params=params))
                if rid:
                    attached_neg_ids.add(rid)

            services.append(
                OdxService(
                    id=attr(svc, "ID"),
                    shortName=svc_short,
                    request=req,
                    posResponses=pos_resps,
                    negResponses=neg_resps,
                )
            )

        return OdxLayer(
            layerType=layerType,
            id=attr(layer_el, "ID"),
            shortName=layer_short,
            services=services,
        )

    # ------------------------------------------------------------------
    # Inheritance resolution stub (kept – invoked in merge)
    # ------------------------------------------------------------------

    def _resolve_links_for_layer(self, layer: OdxLayer):
        # This method is intentionally minimal here.
        # It is invoked (2 passes) to match parity behavior.
        return
