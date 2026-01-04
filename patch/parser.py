
from __future__ import annotations
import uuid
import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple, Optional, Any

from models import (
    OdxParam, OdxMessage, OdxService, OdxLayer, OdxContainer, OdxDatabase, OdxDataObjectProp
)

# --- XML helpers (namespace-agnostic) ---

def local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def get_attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    return el.attrib.get(name, default) if el is not None else default

def get_text(el: Optional[ET.Element], name: str) -> str:
    if el is None:
        return ""
    for c in el:
        if local_name(c.tag) == name:
            return (c.text or "").strip()
    # fallback
    found = el.find(f'.//{{*}}{name}')
    return (found.text or "").strip() if found is not None and found.text else ""

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
    return [n for n in el.iter() if local_name(n.tag) == name]

# --- STRUCTURE harvesting ---

def harvest_structures(layer_el: ET.Element) -> Tuple[Dict[str, List[ET.Element]], Dict[str, List[ET.Element]]]:
    by_id: Dict[str, List[ET.Element]] = {}
    by_sn: Dict[str, List[ET.Element]] = {}
    for st in findall_desc(layer_el, "STRUCTURE"):
        sid = get_attr(st, "ID")
        sn = get_text(st, "SHORT-NAME")
        params_block = find_child(st, "PARAMS")
        params = find_children(params_block, "PARAM") if params_block is not None else []
        if sid:
            by_id[sid] = params
        if sn:
            by_sn[sn] = params
    return by_id, by_sn

class ODXParser:
    def parse_xml_bytes(self, content: bytes) -> ET.Element:
        return ET.fromstring(content)

    def parse_odx_bytes(self, name: str, raw: bytes) -> Tuple[str, OdxContainer]:
        root = self.parse_xml_bytes(raw)
        container = self.parse_container(root)
        return (name, container)

    # --- PARAM parsing (expands STRUCTURE children) ---
    def parse_param(
        self,
        param_el: ET.Element,
        parentType: str,
        parentPath: str,
        layerName: str,
        serviceShortName: str,
        dop_by_id: Dict[str, OdxDataObjectProp],
        dop_by_sn: Dict[str, OdxDataObjectProp],
        dop_meta_by_id: Dict[str, Dict[str, str]],
        struct_by_id: Dict[str, List[ET.Element]],
        struct_by_sn: Dict[str, List[ET.Element]],
        table_by_id: Dict[str, Dict],
    ) -> OdxParam:
        shortName = get_text(param_el, "SHORT-NAME")
        semantic = get_attr(param_el, "SEMANTIC") or get_text(param_el, "SEMANTIC")
        dop_ref = find_child(param_el, "DOP-REF")
        dop_ref_id = get_attr(dop_ref, "ID-REF") if dop_ref is not None else ""
        pid = f"{layerName}:{serviceShortName}:{parentType}:{shortName}:{uuid.uuid4().hex[:8]}"
        dct = find_child(param_el, "DIAG-CODED-TYPE")

        p = OdxParam(
            id=pid,
            shortName=shortName,
            longName=get_text(param_el, "LONG-NAME"),
            description=get_text(param_el, "DESC") or get_text(param_el, "DESCRIPTION"),
            semantic=semantic,
            bytePosition=get_text(param_el, "BYTE-POSITION"),
            bitPosition=get_text(param_el, "BIT-POSITION"),
            bitLength=get_text(dct, "BIT-LENGTH"),
            baseDataType=get_text(dct, "BASE-DATA-TYPE"),
            dopRefId=dop_ref_id,
            parentType=parentType,
            parentName=parentPath,
            layerName=layerName,
            serviceShortName=serviceShortName,
        )
        # value-first
        p.value = get_text(param_el, "VALUE") or ""
        if not p.value:
            p.displayHex = get_text(param_el, "DISPLAY-HEX") or ""
        p.displayValue = get_text(param_el, "DISPLAY-VALUE") or ""

        next_path = f"{parentPath}.{shortName}" if parentPath else shortName

        # 1) Resolve DOP by ID/SN
        dop = None
        if dop_ref_id:
            dop = dop_by_id.get(dop_ref_id) or dop_by_sn.get(dop_ref_id)

        # 2) If DOP-REF directly points to STRUCTURE ID, expand from struct map
        if dop is None and dop_ref_id in struct_by_id:
            for child_el in struct_by_id[dop_ref_id]:
                child = self.parse_param(
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
                p.children.append(child)

        # 3) Standard DOP → STRUCTURE expansion (via DataObjectProp.structureParams)
        if dop and getattr(dop, "structureParams", None):
            for child_el in dop.structureParams:
                child = self.parse_param(
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
                p.children.append(child)

        return p

    def parse_container(self, root: ET.Element) -> OdxContainer:
        cont = OdxContainer()
        for ev in findall_desc(root, "ECU-VARIANT"):
            cont.ecuVariants.append(self._parse_layer(ev, "ECU-VARIANT"))
        for bv in findall_desc(root, "BASE-VARIANT"):
            cont.baseVariants.append(self._parse_layer(bv, "BASE-VARIANT"))
        return cont

    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
        struct_by_id, struct_by_sn = harvest_structures(layer_el)
        dop_by_id: Dict[str, OdxDataObjectProp] = {}
        for dop_el in findall_desc(layer_el, "DATA-OBJECT-PROP"):
            dop = OdxDataObjectProp(
                id=get_attr(dop_el, "ID"),
                shortName=get_text(dop_el, "SHORT-NAME"),
                structureParams=find_children(find_child(dop_el, "STRUCTURE"), "PARAM"),
            )
            if dop.id:
                dop_by_id[dop.id] = dop

        services: List[OdxService] = []
        for svc_el in findall_desc(layer_el, "DIAG-SERVICE"):
            svc_sn = get_text(svc_el, "SHORT-NAME")
            request = None

            # REQUEST
            req_el = find_child(svc_el, "REQUEST")
            if req_el is not None:
                params = []
                for p_el in findall_desc(req_el, "PARAM"):
                    params.append(
                        self.parse_param(
                            p_el,
                            "REQUEST",
                            svc_sn,
                            get_text(layer_el, "SHORT-NAME"),
                            svc_sn,
                            dop_by_id,
                            {},
                            {},
                            struct_by_id,
                            struct_by_sn,
                            {},
                        )
                    )
                request = OdxMessage(
                    id=get_attr(req_el, "ID"),
                    shortName=get_text(req_el, "SHORT-NAME"),
                    params=params,
                )

            # POS-RESPONSE
            pos_resps: List[OdxMessage] = []
            for pr in findall_desc(svc_el, "POS-RESPONSE"):
                params = []
                for p_el in findall_desc(pr, "PARAM"):
                    params.append(
                        self.parse_param(
                            p_el,
                            "POS_RESPONSE",
                            svc_sn,
                            get_text(layer_el, "SHORT-NAME"),
                            svc_sn,
                            dop_by_id,
                            {},
                            {},
                            struct_by_id,
                            struct_by_sn,
                            {},
                        )
                    )
                pos_resps.append(
                    OdxMessage(
                        id=get_attr(pr, "ID"),
                        shortName=get_text(pr, "SHORT-NAME"),
                        params=params,
                    )
                )

            # NEG-RESPONSE (optional)
            neg_resps: List[OdxMessage] = []
            for nr in findall_desc(svc_el, "NEG-RESPONSE"):
                params = []
                for p_el in findall_desc(nr, "PARAM"):
                    params.append(
                        self.parse_param(
                            p_el,
                            "NEG_RESPONSE",
                            svc_sn,
                            get_text(layer_el, "SHORT-NAME"),
                            svc_sn,
                            dop_by_id,
                            {},
                            {},
                            struct_by_id,
                            struct_by_sn,
                            {},
                        )
                    )
                neg_resps.append(
                    OdxMessage(
                        id=get_attr(nr, "ID"),
                        shortName=get_text(nr, "SHORT-NAME"),
                        params=params,
                    )
                )

            # Compute DID and SID (for DID-centric UI)
            did_hex = ""
            sid_val = None
            if request:
                for rp in request.params:
                    sem = (rp.semantic or "").upper()
                    if sem in {"DATA-ID", "MATCHING-REQUEST-PARAM", "DID"} and not did_hex:
                        did_hex = rp.value or rp.displayHex or rp.displayValue or ""
                    if sem in {"SERVICE-ID", "SID", "SERVICE"} and sid_val is None:
                        txt = rp.value or rp.displayHex or rp.displayValue or ""
                        try:
                            sid_val = int(txt, 16) if txt.lower().startswith("0x") else int(txt)
                        except Exception:
                            sid_val = None

            services.append(
                OdxService(
                    id=get_attr(svc_el, "ID"),
                    shortName=svc_sn,
                    request=request,
                    posResponses=pos_resps,
                    negResponses=neg_resps,
                    requestDidHex=did_hex,
                    sid=sid_val,
                    semantic=get_text(svc_el, "SEMANTIC"),
                    description=get_text(svc_el, "DESC") or get_text(svc_el, "DESCRIPTION"),
                )
            )

        return OdxLayer(
            layerType=layerType,
            id=get_attr(layer_el, "ID"),
            shortName=get_text(layer_el, "SHORT-NAME"),
            services=services,
        )
