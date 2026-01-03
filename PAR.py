# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import zipfile
import html
import logging
import xml.etree.ElementTree as ET

from typing import List, Dict, Tuple, Optional
from dataclasses import is_dataclass, fields
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

# =============================================================================
# Utility: safe_asdict
# =============================================================================

def safe_asdict(obj):
    if is_dataclass(obj):
        return {f.name: safe_asdict(getattr(obj, f.name)) for f in fields(obj)}
    if isinstance(obj, dict):
        return {safe_asdict(k): safe_asdict(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [safe_asdict(v) for v in obj]
    if isinstance(obj, (KeysView, ValuesView, ItemsView)):
        return list(obj)
    return obj


# =============================================================================
# XML helpers
# =============================================================================

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
            return "".join(c.itertext()).strip()
    return ""


def get_attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    if el is None:
        return default
    return el.attrib.get(name, default)


# =============================================================================
# Robust XML parsing (encoding / BOM safe)
# =============================================================================

def _try_parse_bytes(raw: bytes) -> ET.Element:
    # Skip garbage before first '<'
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

    # Final fallback (will raise)
    return ET.fromstring(raw)


# =============================================================================
# ODX Parser
# =============================================================================

class ODXParser:

    # -------------------------------------------------------------------------
    # Public entry point
    # -------------------------------------------------------------------------

    def parse_odx(self, file_path: str) -> Tuple[str, OdxContainer]:
        with open(file_path, "rb") as f:
            raw = f.read()

        name = file_path.split("/")[-1].split("\\")[-1]

        # ---- ZIP-based PDX handling ----
        if raw[:4] == b"PK\x03\x04":
            return name, self._parse_pdx_zip(raw)

        # ---- Plain XML ODX ----
        return self.parse_odx_bytes(name, raw)

    def parse_odx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        root = _try_parse_bytes(content)
        return filename, self.parse_container(root)

    # -------------------------------------------------------------------------
    # PDX ZIP handling
    # -------------------------------------------------------------------------

    def _parse_pdx_zip(self, raw: bytes) -> OdxContainer:
        containers: List[OdxContainer] = []

        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            odx_files = [
                n for n in zf.namelist()
                if n.lower().endswith((".odx-d", ".odx-c", ".odx-e", ".xml"))
            ]

            if not odx_files:
                raise ValueError("PDX contains no ODX XML files")

            for name in odx_files:
                content = zf.read(name)
                root = _try_parse_bytes(content)
                containers.append(self.parse_container(root))

        return self.merge_containers(containers)

    # -------------------------------------------------------------------------
    # Container parsing
    # -------------------------------------------------------------------------

    def parse_container(self, root: ET.Element) -> OdxContainer:
        container = OdxContainer()

        for ev in findall_descendants(root, "ECU-VARIANT"):
            container.ecuVariants.append(self._parse_layer(ev, "ECU-VARIANT"))

        for bv in findall_descendants(root, "BASE-VARIANT"):
            container.baseVariants.append(self._parse_layer(bv, "BASE-VARIANT"))

        return container

    # -------------------------------------------------------------------------
    # Merge containers
    # -------------------------------------------------------------------------

    def merge_containers(self, containers: List[OdxContainer]) -> OdxContainer:
        merged = OdxContainer()

        for c in containers:
            merged.ecuVariants.extend(c.ecuVariants)
            merged.baseVariants.extend(c.baseVariants)
            merged.protocols.extend(c.protocols)
            merged.functionalGroups.extend(c.functionalGroups)
            merged.ecuSharedData.extend(c.ecuSharedData)

        return merged

    # -------------------------------------------------------------------------
    # Layer parsing
    # -------------------------------------------------------------------------

    def _parse_layer(self, layer_el: ET.Element, layer_type: str) -> OdxLayer:
        layer = OdxLayer(
            layerType=layer_type,
            id=get_attr(layer_el, "ID"),
            shortName=get_text_local(layer_el, "SHORT-NAME"),
            longName=get_text_local(layer_el, "LONG-NAME"),
            description=get_text_local(layer_el, "DESC"),
            services=[],
        )

        # -------------------------
        # Collect responses
        # -------------------------
        pos_msgs: Dict[str, OdxMessage] = {}
        neg_msgs: Dict[str, OdxMessage] = {}

        for pos in findall_descendants(layer_el, "POS-RESPONSE"):
            mid = get_attr(pos, "ID")
            pos_msgs[mid] = OdxMessage(
                id=mid,
                shortName=get_text_local(pos, "SHORT-NAME"),
                params=[],
            )

        for neg in findall_descendants(layer_el, "NEG-RESPONSE"):
            mid = get_attr(neg, "ID")
            neg_msgs[mid] = OdxMessage(
                id=mid,
                shortName=get_text_local(neg, "SHORT-NAME"),
                params=[],
            )

        # -------------------------
        # Services
        # -------------------------
        for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
            svc = OdxService(
                id=get_attr(svc_el, "ID"),
                shortName=get_text_local(svc_el, "SHORT-NAME"),
                longName=get_text_local(svc_el, "LONG-NAME"),
                description=get_text_local(svc_el, "DESC"),
                request=None,
                posResponses=[],
                negResponses=[],
            )

            # Response references
            for ref in findall_descendants(svc_el, "POS-RESPONSE-REF"):
                rid = get_attr(ref, "ID-REF")
                if rid in pos_msgs:
                    svc.posResponses.append(pos_msgs[rid])

            for ref in findall_descendants(svc_el, "NEG-RESPONSE-REF"):
                rid = get_attr(ref, "ID-REF")
                if rid in neg_msgs:
                    svc.negResponses.append(neg_msgs[rid])

            layer.services.append(svc)

        return layer


__all__ = ["ODXParser"]
