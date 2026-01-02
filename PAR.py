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
    OdxDataObjectProp,
    OdxMessage,
    OdxService,
    OdxLayer,
    OdxContainer,
    OdxDatabase,
)

logger = logging.getLogger(__name__)

# ==============================================================================
# Utility: safe_asdict
# ==============================================================================

def safe_asdict(obj):
    if is_dataclass(obj):
        result = {}
        for f in fields(obj):
            result[f.name] = safe_asdict(getattr(obj, f.name))
        return result
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            out[safe_asdict(k)] = safe_asdict(v)
        return out
    if isinstance(obj, (KeysView, ValuesView, ItemsView)):
        return list(obj)
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [safe_asdict(v) for v in obj]
    if isinstance(obj, ET.Element):
        return obj
    return obj

# ==============================================================================
# XML helpers
# ==============================================================================

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
    result: List[ET.Element] = []
    for c in el:
        if local_name(c.tag) == name:
            result.append(c)
    return result


def findall_descendants(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    if el is None:
        return []
    result: List[ET.Element] = []
    for x in el.iter():
        if local_name(x.tag) == name:
            result.append(x)
    return result


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


def extract_coded_value(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    for n in ("CODED-VALUE", "V"):
        for x in el.iter():
            if local_name(x.tag) == n and isinstance(x.text, str):
                return x.text.strip()
    return ""

# ==============================================================================
# Robust XML parsing
# ==============================================================================

def _try_parse_bytes(raw: bytes) -> ET.Element:
    idx = raw.find(b"<")
    if idx > 0:
        raw = raw[idx:]
    for enc in ("utf-8", "utf-16", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            txt = raw.decode(enc, errors="ignore")
            txt = html.unescape(txt)
            txt = txt[txt.find("<"):]
            return ET.fromstring(txt.encode("utf-8"))
        except Exception:
            continue
    return ET.fromstring(raw)

# ==============================================================================
# STRUCTURE harvesting
# ==============================================================================

def harvest_structures(
    layer_el: ET.Element
) -> Tuple[Dict[str, List[ET.Element]], Dict[str, List[ET.Element]]]:

    struct_by_id: Dict[str, List[ET.Element]] = {}
    struct_by_sn: Dict[str, List[ET.Element]] = {}

    for s in findall_descendants(layer_el, "STRUCTURE"):
        sid = get_attr(s, "ID")
        sn = get_text_local(s, "SHORT-NAME")
        pb = find_child(s, "PARAMS")
        params = find_children(pb, "PARAM") if pb else findall_descendants(s, "PARAM")
        if sid:
            struct_by_id[sid] = params
        if sn:
            struct_by_sn[sn] = params

    return struct_by_id, struct_by_sn

# ==============================================================================
# ODX PARSER
# ==============================================================================

class ODXParser:

    # --------------------------------------------------------------------------
    # Entry points
    # --------------------------------------------------------------------------

    def parse_odx(self, file_path: str) -> Tuple[str, OdxContainer]:
        with open(file_path, "rb") as f:
            raw = f.read()
        name = file_path.split("/")[-1].split("\\")[-1]
        return self.parse_odx_bytes(name, raw)

    def parse_odx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        root = _try_parse_bytes(content)
        return filename, self.parse_container(root)

    # --------------------------------------------------------------------------
    # Container / merge
    # --------------------------------------------------------------------------

    def parse_container(self, root: ET.Element) -> OdxContainer:
        container = OdxContainer()
        for ev in findall_descendants(root, "ECU-VARIANT"):
            container.ecuVariants.append(self._parse_layer(ev, "ECU-VARIANT"))
        return container

    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
        db = OdxDatabase()
        for c in containers:
            db.ecuVariants.extend(c.ecuVariants)

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

    # --------------------------------------------------------------------------
    # LAYER parsing (INLINE + REF + ORPHAN MERGE)
    # --------------------------------------------------------------------------

    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
        layer_short = get_text_local(layer_el, "SHORT-NAME")

        struct_by_id, struct_by_sn = harvest_structures(layer_el)

        # ---------------- DOPs ----------------
        dop_by_id: Dict[str, OdxDataObjectProp] = {}
        dop_by_sn: Dict[str, OdxDataObjectProp] = {}

        for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
            dop = OdxDataObjectProp(
                id=get_attr(d, "ID"),
                shortName=get_text_local(d, "SHORT-NAME"),
                longName=get_text_local(d, "LONG-NAME"),
                description=get_text_local(d, "DESC"),
                structureParams=[],
            )
            dop_by_id[dop.id] = dop
            dop_by_sn[dop.shortName] = dop

        # ---------------- Collect ALL responses globally ----------------
        all_pos_msgs: Dict[str, OdxMessage] = {}
        all_neg_msgs: Dict[str, OdxMessage] = {}

        for pos in findall_descendants(layer_el, "POS-RESPONSE"):
            msg = OdxMessage(
                id=get_attr(pos, "ID"),
                shortName=get_text_local(pos, "SHORT-NAME"),
                params=[],
            )
            all_pos_msgs[msg.id] = msg

        for neg in findall_descendants(layer_el, "NEG-RESPONSE"):
            msg = OdxMessage(
                id=get_attr(neg, "ID"),
                shortName=get_text_local(neg, "SHORT-NAME"),
                params=[],
            )
            all_neg_msgs[msg.id] = msg

        services: List[OdxService] = []

        # ---------------- Services ----------------
        for svc in findall_descendants(layer_el, "DIAG-SERVICE"):
            svc_short = get_text_local(svc, "SHORT-NAME")

            pos_resps: List[OdxMessage] = []
            neg_resps: List[OdxMessage] = []

            # ---------- INLINE POS / NEG ----------
            for pos in find_children(svc, "POS-RESPONSE"):
                mid = get_attr(pos, "ID")
                if mid in all_pos_msgs:
                    pos_resps.append(all_pos_msgs[mid])

            for neg in find_children(svc, "NEG-RESPONSE"):
                mid = get_attr(neg, "ID")
                if mid in all_neg_msgs:
                    neg_resps.append(all_neg_msgs[mid])

            # ---------- RESPONSE-REF ----------
            for ref in findall_descendants(svc, "POS-RESPONSE-REF"):
                rid = get_attr(ref, "ID-REF")
                if rid in all_pos_msgs and all_pos_msgs[rid] not in pos_resps:
                    pos_resps.append(all_pos_msgs[rid])

            for ref in findall_descendants(svc, "NEG-RESPONSE-REF"):
                rid = get_attr(ref, "ID-REF")
                if rid in all_neg_msgs and all_neg_msgs[rid] not in neg_resps:
                    neg_resps.append(all_neg_msgs[rid])

            services.append(
                OdxService(
                    id=get_attr(svc, "ID"),
                    shortName=svc_short,
                    request=None,
                    posResponses=pos_resps,
                    negResponses=neg_resps,
                )
            )

        # ---------------- Service-name–based orphan merge ----------------
        for svc in services:
            if not svc.posResponses:
                for msg in all_pos_msgs.values():
                    if svc.shortName.lower() in (msg.shortName or "").lower():
                        svc.posResponses.append(msg)

            if not svc.negResponses:
                for msg in all_neg_msgs.values():
                    if svc.shortName.lower() in (msg.shortName or "").lower():
                        svc.negResponses.append(msg)

        # ---------------- Global orphan fallback ----------------
        for svc in services:
            if not svc.posResponses:
                svc.posResponses.extend(all_pos_msgs.values())
            if not svc.negResponses:
                svc.negResponses.extend(all_neg_msgs.values())

        return OdxLayer(
            layerType=layerType,
            id=get_attr(layer_el, "ID"),
            shortName=layer_short,
            services=services,
        )
