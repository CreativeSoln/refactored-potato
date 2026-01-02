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

# ==============================================================================
# Utility: safe_asdict
# ==============================================================================

def safe_asdict(obj):
    """
    Dataclass-safe recursive conversion.
    Prevents ET.Element and dataclass mutation errors.
    """
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
# XML helpers (expanded for clarity & Pylance)
# ==============================================================================

def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


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
            if isinstance(txt, str):
                return txt.strip()

    return ""


def get_attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    if el is None:
        return default

    val = el.attrib.get(name)

    if isinstance(val, str):
        return val

    return default


def get_attr_ci(el: Optional[ET.Element], *names: str) -> str:
    if el is None:
        return ""

    lowered = {}
    for k, v in el.attrib.items():
        lowered[k.lower()] = v

    for n in names:
        v = lowered.get(n.lower())
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

    value = first_text(el, ["CODED-VALUE"])
    if value:
        return value

    value = first_text(el, ["V"])
    if value:
        return value

    value = get_attr_ci(el, "CODED-VALUE")
    if value:
        return value

    return ""

# ==============================================================================
# Robust XML byte parsing
# ==============================================================================

def _try_parse_bytes(raw: bytes) -> ET.Element:
    """
    Attempt multiple encodings and entity fixes before failing.
    """
    idx = raw.find(b"<")
    if idx > 0:
        raw = raw[idx:]

    encodings = [
        "utf-8",
        "utf-16",
        "utf-16-le",
        "utf-16-be",
        "latin-1",
    ]

    for enc in encodings:
        try:
            text = raw.decode(enc, errors="ignore")
            text = html.unescape(text)
            text = text[text.find("<"):]
            return ET.fromstring(text.encode("utf-8"))
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

    structures = findall_descendants(layer_el, "STRUCTURE")

    for s in structures:
        sid = get_attr(s, "ID")
        short_name = get_text_local(s, "SHORT-NAME")

        params_block = find_child(s, "PARAMS")
        if params_block is not None:
            params = find_children(params_block, "PARAM")
        else:
            params = findall_descendants(s, "PARAM")

        if sid:
            struct_by_id[sid] = params

        if short_name:
            struct_by_sn[short_name] = params

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
        container = self.parse_container(root)
        return filename, container

    # --------------------------------------------------------------------------
    # Container / merge
    # --------------------------------------------------------------------------

    def parse_container(self, root: ET.Element) -> OdxContainer:
        container = OdxContainer()

        ecu_variants = findall_descendants(root, "ECU-VARIANT")
        for ev in ecu_variants:
            layer = self._parse_layer(ev, "ECU-VARIANT")
            container.ecuVariants.append(layer)

        return container

    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
        database = OdxDatabase()

        for c in containers:
            for layer in c.ecuVariants:
                database.ecuVariants.append(layer)

        # Two-pass inheritance (as per reference tools)
        for _ in range(2):
            for layer in database.ecuVariants:
                self._resolve_links_for_layer(layer)
                self._dedup_services(layer)

        # Flatten params
        for layer in database.ecuVariants:
            for svc in layer.services:
                if svc.request:
                    for p in svc.request.params:
                        p.layerName = layer.shortName
                        database.allParams.append(p)

                for msg in svc.posResponses + svc.negResponses:
                    for p in msg.params:
                        p.layerName = layer.shortName
                        database.allParams.append(p)

        return database

    # --------------------------------------------------------------------------
    # PARAM parsing (TABLE-KEY ENABLED)
    # --------------------------------------------------------------------------

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

        short_name = get_text_local(param_el, "SHORT-NAME")
        assert short_name, "PARAM without SHORT-NAME"

        diag_coded = find_child(param_el, "DIAG-CODED-TYPE")
        phys_type = find_child(param_el, "PHYSICAL-TYPE")

        dop_ref = find_child(param_el, "DOP-REF")
        dop_snref = find_child(param_el, "DOP-SNREF")

        pid = (
            f"{layerName}::{serviceShortName}::{parentType}"
            f"::{short_name}::{uuid.uuid4().hex[:6]}"
        )

        param = OdxParam(
            id=pid,
            shortName=short_name,
            longName=get_text_local(param_el, "LONG-NAME"),
            description=get_text_local(param_el, "DESC"),
            semantic=get_attr(param_el, "SEMANTIC"),
            bytePosition=get_text_local(param_el, "BYTE-POSITION"),
            bitPosition=get_text_local(param_el, "BIT-POSITION"),
            bitLength=get_text_local(diag_coded, "BIT-LENGTH"),
            baseDataType=get_attr(diag_coded, "BASE-DATA-TYPE"),
            physicalBaseType=get_attr(phys_type, "BASE-DATA-TYPE"),
            isHighLowByteOrder=(
                get_attr(diag_coded, "IS-HIGHLOW-BYTE-ORDER")
                or get_attr(diag_coded, "IS-HIGH-LOW-BYTE-ORDER")
            ),
            codedConstValue=extract_coded_value(param_el),
            dopRefId=get_attr(dop_ref, "ID-REF"),
            dopSnRefName=get_text_local(dop_snref, "SHORT-NAME"),
            parentType=parentType,
            parentName=parentPath,
            layerName=layerName,
            serviceShortName=serviceShortName,
            attrs=dict(param_el.attrib),
        )

        next_path = f"{parentPath}.{short_name}" if parentPath else short_name

        # ---------------- DOP STRUCTURE ----------------
        dop = dop_by_id.get(param.dopRefId) or dop_by_sn.get(param.dopSnRefName)
        if dop and dop.structureParams:
            for child_el in dop.structureParams:
                child_param = self._try_parse_param(
                    child_el,
                    "STRUCTURE",
                    next_path,
                    layerName,
                    serviceShortName,
                    dop_by_id,
                    dop_by_sn,
                    dop_meta_by_id,
                    struct_by_id,
                    struct_by_sn,
                    table_by_id,
                )
                if child_param:
                    param.children.append(child_param)

        # ---------------- TABLE-KEY ----------------
        table_key_el = find_child(diag_coded, "TABLE-KEY") if diag_coded else None
        if table_key_el is not None:
            table_ref = find_child(table_key_el, "TABLE-REF")
            table_id = get_attr(table_ref, "ID-REF")

            table = table_by_id.get(table_id)
            assert table is not None, f"Missing TABLE id={table_id}"

            key_value = extract_coded_value(table_key_el)
            assert key_value, f"Empty TABLE-KEY for {short_name}"

            matched_row = None
            for row in table["rows"]:
                if row["key"] == key_value:
                    matched_row = row
                    break

            assert matched_row is not None, (
                f"No TABLE-ROW for key={key_value} table={table_id}"
            )

            for struct_param_el in matched_row["structParams"]:
                child_param = self._try_parse_param(
                    struct_param_el,
                    "STRUCTURE",
                    next_path,
                    layerName,
                    serviceShortName,
                    dop_by_id,
                    dop_by_sn,
                    dop_meta_by_id,
                    struct_by_id,
                    struct_by_sn,
                    table_by_id,
                )
                if child_param:
                    param.children.append(child_param)

        return param

    # --------------------------------------------------------------------------
    # LAYER parsing (TABLE harvesting)
    # --------------------------------------------------------------------------

    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
        layer_short_name = get_text_local(layer_el, "SHORT-NAME")

        struct_by_id, struct_by_sn = harvest_structures(layer_el)

        dop_by_id: Dict[str, OdxDataObjectProp] = {}
        dop_by_sn: Dict[str, OdxDataObjectProp] = {}

        dop_elements = findall_descendants(layer_el, "DATA-OBJECT-PROP")
        for d in dop_elements:
            dop = OdxDataObjectProp(
                id=get_attr(d, "ID"),
                shortName=get_text_local(d, "SHORT-NAME"),
                longName=get_text_local(d, "LONG-NAME"),
                description=get_text_local(d, "DESC"),
                structureParams=[],
            )

            struct_el = find_child(d, "STRUCTURE")
            if struct_el:
                params_block = find_child(struct_el, "PARAMS")
                if params_block:
                    dop.structureParams = find_children(params_block, "PARAM")

            dop_by_id[dop.id] = dop
            dop_by_sn[dop.shortName] = dop

        # ---------------- TABLE harvesting ----------------
        table_by_id: Dict[str, Dict] = {}

        tables = findall_descendants(layer_el, "TABLE")
        for tbl in tables:
            table_id = get_attr(tbl, "ID")

            rows: List[Dict] = []

            row_elements = find_children(tbl, "TABLE-ROW")
            for tr in row_elements:
                key = get_attr(tr, "KEY")

                struct_el = find_child(tr, "STRUCTURE")
                params_block = find_child(struct_el, "PARAMS") if struct_el else None
                params = find_children(params_block, "PARAM") if params_block else []

                rows.append({
                    "key": key,
                    "structParams": params,
                })

            if table_id:
                assert rows, f"TABLE {table_id} has no TABLE-ROW"
                table_by_id[table_id] = {
                    "rows": rows
                }

        services: List[OdxService] = []

        service_elements = findall_descendants(layer_el, "DIAG-SERVICE")
        for svc in service_elements:
            svc_short = get_text_local(svc, "SHORT-NAME")

            # ---------- REQUEST ----------
            request_msg = None
            req_el = find_child(svc, "REQUEST")
            if req_el:
                req_params: List[OdxParam] = []
                params_block = find_child(req_el, "PARAMS")

                if params_block:
                    for p in find_children(params_block, "PARAM"):
                        parsed = self._try_parse_param(
                            p,
                            "REQUEST",
                            svc_short,
                            layer_short_name,
                            svc_short,
                            dop_by_id,
                            dop_by_sn,
                            {},
                            struct_by_id,
                            struct_by_sn,
                            table_by_id,
                        )
                        if parsed:
                            req_params.append(parsed)

                request_msg = OdxMessage(
                    id=get_attr(req_el, "ID"),
                    shortName="REQUEST",
                    params=req_params,
                )

            # ---------- RESPONSES ----------
            pos_responses: List[OdxMessage] = []
            neg_responses: List[OdxMessage] = []

            for pos in find_children(svc, "POS-RESPONSE"):
                params: List[OdxParam] = []
                for p in findall_descendants(pos, "PARAM"):
                    parsed = self._try_parse_param(
                        p,
                        "POS_RESPONSE",
                        f"{svc_short}.POS",
                        layer_short_name,
                        svc_short,
                        dop_by_id,
                        dop_by_sn,
                        {},
                        struct_by_id,
                        struct_by_sn,
                        table_by_id,
                    )
                    if parsed:
                        params.append(parsed)

                pos_responses.append(
                    OdxMessage(
                        id=get_attr(pos, "ID"),
                        shortName="POS",
                        params=params,
                    )
                )

            for neg in find_children(svc, "NEG-RESPONSE"):
                params: List[OdxParam] = []
                for p in findall_descendants(neg, "PARAM"):
                    parsed = self._try_parse_param(
                        p,
                        "NEG_RESPONSE",
                        f"{svc_short}.NEG",
                        layer_short_name,
                        svc_short,
                        dop_by_id,
                        dop_by_sn,
                        {},
                        struct_by_id,
                        struct_by_sn,
                        table_by_id,
                    )
                    if parsed:
                        params.append(parsed)

                neg_responses.append(
                    OdxMessage(
                        id=get_attr(neg, "ID"),
                        shortName="NEG",
                        params=params,
                    )
                )

            services.append(
                OdxService(
                    id=get_attr(svc, "ID"),
                    shortName=svc_short,
                    request=request_msg,
                    posResponses=pos_responses,
                    negResponses=neg_responses,
                )
            )

        return OdxLayer(
            layerType=layerType,
            id=get_attr(layer_el, "ID"),
            shortName=layer_short_name,
            services=services,
        )

    # --------------------------------------------------------------------------
    # Inheritance helpers (kept minimal intentionally)
    # --------------------------------------------------------------------------

    def _resolve_links_for_layer(
        self,
        layer: OdxLayer,
        id_map: Optional[Dict[str, object]] = None,
        visited: Optional[Set[str]] = None,
    ) -> None:
        return

    def _dedup_services(self, layer: OdxLayer) -> None:
        seen: Set[str] = set()
        unique: List[OdxService] = []

        for svc in layer.services:
            if svc.shortName in seen:
                continue
            seen.add(svc.shortName)
            unique.append(svc)

        layer.services = unique
