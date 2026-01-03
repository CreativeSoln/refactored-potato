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

# ---------------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------------

def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def get_all_attrs(el: Optional[ET.Element]) -> Dict[str, str]:
    return {} if el is None else dict(el.attrib)

def get_attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    if el is None:
        return default
    return el.attrib.get(name, default)

def get_text_local(el: Optional[ET.Element], name: str) -> str:
    if el is None:
        return ""
    for c in el:
        if local_name(c.tag) == name:
            return "".join(c.itertext()).strip()
    return ""

def get_elements(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    if el is None:
        return []
    out = []
    for c in el:
        if local_name(c.tag) == name:
            out.append(c)
    return out

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
    out = []
    for c in el:
        if local_name(c.tag) == name:
            out.append(c)
    return out

def findall_descendants(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    if el is None:
        return []
    return [n for n in el.iter() if local_name(n.tag) == name]


def first_text(el: Optional[ET.Element], tag_names: List[str]) -> str:
    """
    Find first text for any of the tags (searching descendants).
    """
    if el is None:
        return ""

    for t in tag_names:
        for node in el.iter():
            if local_name(node.tag) == t:
                txt = (node.text or "").strip()
                if txt:
                    return txt
    return ""


def get_attr_ci(el: Optional[ET.Element], *names: str) -> str:
    """
    Case-insensitive attribute getter for first matching name.
    """
    if el is None or not el.attrib:
        return ""

    low = {k.lower(): v for k, v in el.attrib.items()}

    for n in names:
        v = low.get(n.lower())
        if v:
            return v

    return ""


def extract_coded_value(scope: Optional[ET.Element]) -> str:
    """
    Robust extraction of coded value:
    - prefer <CODED-VALUE>, fallback to <V>,
      and finally 'CODED-VALUE' attribute on scope.
    """
    if scope is None:
        return ""

    cv = first_text(scope, ["CODED-VALUE"])
    if cv:
        return cv

    v = first_text(scope, ["V"])
    if v:
        return v

    a = get_attr_ci(scope, "CODED-VALUE")
    return a or ""

def slice_from_first_lt(raw: bytes) -> bytes:
    i = raw.find(b"<")
    return raw if i <= 0 else raw[i:]


def _try_parse_bytes(raw: bytes) -> ET.Element:
    raw1 = slice_from_first_lt(raw)
    try:
        return ET.fromstring(raw1)
    except ET.ParseError:
        pass

    for enc in ("utf-8", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1"):
        try:
            text = raw1.decode(enc, errors="strict")
        except UnicodeDecodeError:
            continue

        if "<" in text and ">" in text and r"\<" not in text[:200]:
            text = html.unescape(text)
            m = re.search(r"<", text)
            if m:
                text = text[m.start():]

            try:
                return ET.fromstring(text.encode("utf-8"))
            except ET.ParseError:
                continue

    text = raw1.decode("utf-8", errors="ignore")
    m = re.search(r"<", text)
    if m:
        text = text[m.start():]

    return ET.fromstring(text.encode("utf-8"))

def harvest_structures(layer_el: ET.Element) -> Tuple[Dict[str, List[ET.Element]], Dict[str, List[ET.Element]]]:

    by_id: Dict[str, List[ET.Element]] = {}
    by_sn: Dict[str, List[ET.Element]] = {}

    struct_elems = (
        findall_descendants(layer_el, "STRUCTURE")
        + findall_descendants(layer_el, "STRUCT")
        + findall_descendants(layer_el, "STRUCTURE-DEF")
        + findall_descendants(layer_el, "DATA-STRUCTURE-DEF")
    )

    for st in struct_elems:
        sid = get_attr(st, "ID")
        ssn = get_text_local(st, "SHORT-NAME")

        params_block = find_child(st, "PARAMS")
        if params_block is not None:
            params = find_children(params_block, "PARAM")
        else:
            params = find_children(st, "PARAM")
            if not params:
                params = findall_descendants(st, "PARAM")

        if sid:
            by_id[sid] = params
        if ssn:
            by_sn[ssn] = params

    return by_id, by_sn


# ---------------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------------

class ODXParser:

    # ================================================
    # XML root parser
    # ================================================
    def parse_xml_bytes(self, content: bytes) -> ET.Element:
        return _try_parse_bytes(content)

    def parse_xml(self, content: str) -> ET.Element:
        return self.parse_xml_bytes(content.encode("utf-8", errors="ignore"))

    from typing import Optional, Dict


    def _try_parse_param(
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
    ) -> Optional[OdxParam]:
        try:
            return self.parse_param(
                param_el,
                parentType,
                parentPath,
                layerName,
                serviceShortName,
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
        except Exception as ex:
            print(f"[WARN] Skipping PARAM: {ex}")
            return None
    

    def _fill_from_dop_if_missing(
            self,
            p: OdxParam,
            dop: Optional[OdxDataObjectProp],
            dop_meta_by_id: Dict[str, Dict[str, str]],
        ) -> None:
            meta: Dict[str, str] = {}

            if dop:
                did = getattr(dop, "id", "")
                meta = dop_meta_by_id.get(did, {}) if did else {}

            base = getattr(dop, "baseDataType", "") if dop else ""
            phys = getattr(dop, "physicalBaseDataType", "") if dop else ""
            bitlen = getattr(dop, "bitLength", "") if dop else ""

            if not p.baseDataType and base:
                p.baseDataType = base

            if not p.physicalBaseType and phys:
                p.physicalBaseType = phys

            if not p.bitLength and bitlen:
                p.bitLength = bitlen

            minlen = meta.get("minLength", "")
            maxlen = meta.get("maxLength", "")

            if not p.minLength and minlen:
                p.minLength = minlen

            if not p.maxLength and maxlen:
                p.maxLength = maxlen

    def _annotate_service_name(
            self,
            params: List[OdxParam],
            svc_short: str
        ) -> None:
        """Set serviceShortName on params and their children."""
        if not params:
            return

        stack: List[OdxParam] = [p for p in params if isinstance(p, OdxParam)]

        while stack:
            node = stack.pop()
            node.serviceShortName = svc_short

            for c in getattr(node, "children", []) or []:
                if isinstance(c, OdxParam):
                    stack.append(c)


    def _prefix_path(
        self,
        params: List[OdxParam],
        prefix: str
    ) -> None:
        """Prefix parentName for each param (and children) with 'prefix.'."""
        if not params or not prefix:
            return

        stack: List[OdxParam] = [p for p in params if isinstance(p, OdxParam)]

        while stack:
            node = stack.pop()
            base = node.parentName or ""
            node.parentName = f"{prefix}.{base}" if base else prefix

            for c in getattr(node, "children", []) or []:
                if isinstance(c, OdxParam):
                    stack.append(c)

    

    def _parse_dop_with_struct_map(
        self,
        dop_el: ET.Element,
        struct_by_id: Dict[str, List[ET.Element]],
        struct_by_sn: Dict[str, List[ET.Element]],
    ) -> Tuple[OdxDataObjectProp, Dict[str, str]]:

            diagCodedType = find_child(dop_el, "DIAG-CODED-TYPE")
            physType = find_child(dop_el, "PHYSICAL-TYPE")
            unitRef = find_child(dop_el, "UNIT-REF")
            compuMethod = find_child(dop_el, "COMPU-METHOD")

            structure = find_child(dop_el, "STRUCTURE")
            structure_ref = find_child(dop_el, "STRUCTURE-REF")

            struct_params: List[ET.Element] = []

            if structure is not None:
                params_block = find_child(structure, "PARAMS")
                if params_block is not None:
                    struct_params = find_children(params_block, "PARAM")
                else:
                    struct_params = find_children(structure, "PARAM")
                    if not struct_params:
                        struct_params = findall_descendants(structure, "PARAM")

            if not struct_params and structure_ref is not None:
                ref_id = get_attr(structure_ref, "ID-REF")
                ref_sn = get_text_local(structure_ref, "SHORT-NAME")

                if ref_id and ref_id in struct_by_id:
                    struct_params = struct_by_id[ref_id]
                elif ref_sn and ref_sn in struct_by_sn:
                    struct_params = struct_by_sn[ref_sn]

            dd = OdxDataObjectProp(
                id=get_attr(dop_el, "ID"),
                shortName=get_text_local(dop_el, "SHORT-NAME"),
                longName=get_text_local(dop_el, "LONG-NAME"),
                description=get_text_local(dop_el, "DESC"),
                baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE") if diagCodedType is not None else "",
                bitLength=get_text_local(diagCodedType, "BIT-LENGTH") if diagCodedType is not None else "",
                physicalBaseDataType=get_attr(physType, "BASE-DATA-TYPE") if physType is not None else "",
                unitRefId=get_attr(unitRef, "ID-REF") if unitRef is not None else "",
                compuCategory=get_text_local(compuMethod, "CATEGORY") if compuMethod is not None else "",
                structureParams=struct_params,
            )

            meta = {
                "minLength": get_text_local(diagCodedType, "MIN-LENGTH") if diagCodedType is not None else "",
                "maxLength": get_text_local(diagCodedType, "MAX-LENGTH") if diagCodedType is not None else "",
            }

            return dd, meta


    def _parse_dop(self, dop_el: ET.Element) -> OdxDataObjectProp:
        diagCodedType = find_child(dop_el, "DIAG-CODED-TYPE")
        physType      = find_child(dop_el, "PHYSICAL-TYPE")
        unitRef       = find_child(dop_el, "UNIT-REF")
        compuMethod   = find_child(dop_el, "COMPU-METHOD")
        structure     = find_child(dop_el, "STRUCTURE")

        return OdxDataObjectProp(
            id=get_attr(dop_el, "ID"),
            shortName=get_text_local(dop_el, "SHORT-NAME"),
            longName=get_text_local(dop_el, "LONG-NAME"),
            description=get_text_local(dop_el, "DESC"),

            baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE")
                if diagCodedType is not None else "",
            bitLength=get_text_local(diagCodedType, "BIT-LENGTH")
                if diagCodedType is not None else "",
            physicalBaseDataType=get_attr(physType, "BASE-DATA-TYPE")
                if physType is not None else "",
            unitRefId=get_attr(unitRef, "ID-REF")
                if unitRef is not None else "",
            compuCategory=get_text_local(compuMethod, "CATEGORY")
                if compuMethod is not None else "",
            structureParams=get_elements(structure, "PARAM")
                if structure is not None else [],
        )

    def _parse_dtc(self, dtc_el: ET.Element) -> OdxDTC:
        return OdxDTC(
            id=get_attr(dtc_el, "ID"),
            shortName=get_text_local(dtc_el, "SHORT-NAME"),
            longName=get_text_local(dtc_el, "LONG-NAME"),
            description=get_text_local(dtc_el, "DESC"),
            troubleCode=get_text_local(dtc_el, "TROUBLE-CODE"),
            displayTroubleCode=get_text_local(dtc_el, "DISPLAY-TROUBLE-CODE"),
            level=get_text_local(dtc_el, "LEVEL"),
        )


    # ================================================
    # Ensure container root
    # ================================================
    def _ensure_container(self, root: ET.Element) -> ET.Element:
        rn = local_name(root.tag)
        if rn == "DIAG-LAYER-CONTAINER":
            return root

        dlc = find_child(root, "DIAG-LAYER-CONTAINER")
        if dlc is not None:
            return dlc
        
        matches = findall_descendants(root, "DIAG-LAYER-CONTAINER")
        if matches:
            return matches[0]

        return root

    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
        layer_short = get_text_local(layer_el, "SHORT-NAME")

        # ------------------------------------------------------------
        # STRUCTURES
        # ------------------------------------------------------------
        struct_by_id, struct_by_sn = harvest_structures(layer_el)

        # ------------------------------------------------------------
        # DOPs + meta
        # ------------------------------------------------------------
        dop_by_id: Dict[str, OdxDataObjectProp] = {}
        dop_by_sn: Dict[str, OdxDataObjectProp] = {}
        dop_meta_by_id: Dict[str, Dict[str, str]] = {}

        for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
            dd, meta = self._parse_dop_with_struct_map(d, struct_by_id, struct_by_sn)
            dop_by_id[dd.id] = dd
            dop_meta_by_id[dd.id] = meta
            if dd.shortName:
                dop_by_sn[dd.shortName] = dd

        # ------------------------------------------------------------
        # TABLES (for TABLE-KEY)
        # ------------------------------------------------------------
        table_by_id: Dict[str, Dict] = {}

        for t in findall_descendants(layer_el, "TABLE"):
            tid = get_attr(t, "ID")
            tsn = get_text_local(t, "SHORT-NAME")
            key_dop_ref = get_attr(find_child(t, "KEY-DOP-REF"), "ID-REF")

            rows = []
            for tr in findall_descendants(t, "TABLE-ROW"):
                rid = get_attr(tr, "ID")
                rsn = get_text_local(tr, "SHORT-NAME")
                rkey = get_text_local(tr, "KEY")

                struct_ref = find_child(tr, "STRUCTURE-REF")
                struct_params: List[ET.Element] = []

                if struct_ref is not None:
                    ref_id = get_attr(struct_ref, "ID-REF")
                    ref_sn = get_text_local(struct_ref, "SHORT-NAME")

                    if ref_id and ref_id in struct_by_id:
                        struct_params = struct_by_id[ref_id]
                    elif ref_sn and ref_sn in struct_by_sn:
                        struct_params = struct_by_sn[ref_sn]

                rows.append({
                    "id": rid,
                    "shortName": rsn,
                    "key": rkey,
                    "structParams": struct_params,
                })

            if tid:
                table_by_id[tid] = {
                    "shortName": tsn,
                    "keyDopRefId": key_dop_ref,
                    "rows": rows,
                }

        # ------------------------------------------------------------
        # Units / Compu / DTC
        # ------------------------------------------------------------
        units: List[OdxUnit] = [
            self._parse_unit(u)
            for u in findall_descendants(layer_el, "UNIT")
        ]

        compu_methods: List[OdxCompuMethod] = [
            self._parse_compu_method(c)
            for c in findall_descendants(layer_el, "COMPU-METHOD")
        ]

        dtcs: List[OdxDTC] = [
            self._parse_dtc(d)
            for d in findall_descendants(layer_el, "DTC")
        ]

        # ------------------------------------------------------------
        # Message maps
        # ------------------------------------------------------------
        request_map: Dict[str, OdxMessage] = {}
        pos_resp_map: Dict[str, OdxMessage] = {}
        neg_resp_map: Dict[str, OdxMessage] = {}

        # ------------------------------------------------------------
        # Standalone REQUESTS
        # ------------------------------------------------------------
        for req in findall_descendants(layer_el, "REQUEST"):
            rid = get_attr(req, "ID")
            rshort = get_text_local(req, "SHORT-NAME")
            root_path = rshort or ""

            rparams: List[OdxParam] = []
            for p_el in findall_descendants(req, "PARAM"):
                rp = self._try_parse_param(
                    p_el,
                    "REQUEST",
                    root_path,
                    layer_short,
                    "",
                    dop_by_id,
                    dop_by_sn,
                    dop_meta_by_id,
                    struct_by_id,
                    struct_by_sn,
                    table_by_id,
                )
                if rp is not None:
                    rparams.append(rp)

            request_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(req, "LONG-NAME"),
                params=rparams,
            )

        # ------------------------------------------------------------
        # Standalone POS-RESPONSE
        # ------------------------------------------------------------
        for res in findall_descendants(layer_el, "POS-RESPONSE"):
            rid = get_attr(res, "ID")
            rshort = get_text_local(res, "SHORT-NAME")
            root_path = rshort or ""

            rparams: List[OdxParam] = []
            for p_el in findall_descendants(res, "PARAM"):
                rp = self._try_parse_param(
                    p_el,
                    "POS_RESPONSE",
                    root_path,
                    layer_short,
                    "",
                    dop_by_id,
                    dop_by_sn,
                    dop_meta_by_id,
                    struct_by_id,
                    struct_by_sn,
                    table_by_id,
                )
                if rp is not None:
                    rparams.append(rp)

            pos_resp_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(res, "LONG-NAME"),
                params=rparams,
            )

        # ------------------------------------------------------------
        # Standalone NEG-RESPONSE
        # ------------------------------------------------------------
        for res in findall_descendants(layer_el, "NEG-RESPONSE"):
            rid = get_attr(res, "ID")
            rshort = get_text_local(res, "SHORT-NAME")
            root_path = rshort or ""

            rparams: List[OdxParam] = []
            for p_el in findall_descendants(res, "PARAM"):
                rp = self._try_parse_param(
                    p_el,
                    "NEG_RESPONSE",
                    root_path,
                    layer_short,
                    "",
                    dop_by_id,
                    dop_by_sn,
                    dop_meta_by_id,
                    struct_by_id,
                    struct_by_sn,
                    table_by_id,
                )
                if rp is not None:
                    rparams.append(rp)

            neg_resp_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(res, "LONG-NAME"),
                params=rparams,
            )

        # ------------------------------------------------------------
        # SERVICES (inline + references)
        # ------------------------------------------------------------
        services: List[OdxService] = []
        attached_pos_ids: Set[str] = set()
        attached_neg_ids: Set[str] = set()

        for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
            svc_attrs = get_all_attrs(svc_el)
            svc_short = get_text_local(svc_el, "SHORT-NAME")

            request_ref = find_child(svc_el, "REQUEST-REF")
            request_ref_id = get_attr(request_ref, "ID-REF") if request_ref is not None else ""

            pos_ref_ids = [
                get_attr(r, "ID-REF")
                for r in find_children(svc_el, "POS-RESPONSE-REF")
            ]

            neg_ref_ids = [
                get_attr(r, "ID-REF")
                for r in find_children(svc_el, "NEG-RESPONSE-REF")
            ]

            inline_req = find_child(svc_el, "REQUEST")
            inline_pos = find_children(svc_el, "POS-RESPONSE")
            inline_neg = find_children(svc_el, "NEG-RESPONSE")

            # ----------------------------
            # REQUEST
            # ----------------------------
            request = None
            if request_ref_id and request_ref_id in request_map:
                request = request_map[request_ref_id]
                prefix = f"{svc_short}.{request.shortName or 'Request'}" if svc_short else (request.shortName or "")
                self._prefix_path(request.params, prefix)
                self._annotate_service_name(request.params, svc_short)

            elif inline_req is not None:
                rshort = get_text_local(inline_req, "SHORT-NAME") or (svc_short + "_req")
                root_path = svc_short if svc_short else ""
                root_path = f"{root_path}.{rshort}" if rshort else root_path

                rparams: List[OdxParam] = []
                for p_el in findall_descendants(inline_req, "PARAM"):
                    rp = self._try_parse_param(
                        p_el,
                        "REQUEST",
                        root_path,
                        layer_short,
                        svc_short,
                        dop_by_id,
                        dop_by_sn,
                        dop_meta_by_id,
                        struct_by_id,
                        struct_by_sn,
                        table_by_id,
                    )
                    if rp is not None:
                        rparams.append(rp)

                self._annotate_service_name(rparams, svc_short)
                request = OdxMessage(
                    id=get_attr(inline_req, "ID"),
                    shortName=rshort,
                    longName=get_text_local(inline_req, "LONG-NAME"),
                    params=rparams,
                )

            # ----------------------------
            # POS RESPONSES
            # ----------------------------
            pos_responses: List[OdxMessage] = []

            for rid in pos_ref_ids:
                rr = pos_resp_map.get(rid)
                if rr:
                    prefix = f"{svc_short}.{rr.shortName or 'PosResponse'}" if svc_short else (rr.shortName or "")
                    self._prefix_path(rr.params, prefix)
                    self._annotate_service_name(rr.params, svc_short)
                    pos_responses.append(rr)
                    attached_pos_ids.add(rid)

            for el in inline_pos:
                rshort = get_text_local(el, "SHORT-NAME") or (svc_short + "_pos")
                root_path = svc_short if svc_short else ""
                root_path = f"{root_path}.{rshort}" if rshort else root_path

                rparams: List[OdxParam] = []
                for p_el in findall_descendants(el, "PARAM"):
                    rp = self._try_parse_param(
                        p_el,
                        "POS_RESPONSE",
                        root_path,
                        layer_short,
                        svc_short,
                        dop_by_id,
                        dop_by_sn,
                        dop_meta_by_id,
                        struct_by_id,
                        struct_by_sn,
                        table_by_id,
                    )
                    if rp is not None:
                        rparams.append(rp)

                self._annotate_service_name(rparams, svc_short)
                pos_responses.append(
                    OdxMessage(
                        id=get_attr(el, "ID"),
                        shortName=rshort,
                        longName=get_text_local(el, "LONG-NAME"),
                        params=rparams,
                    )
                )

            # ----------------------------
            # NEG RESPONSES
            # ----------------------------
            neg_responses: List[OdxMessage] = []

            for rid in neg_ref_ids:
                rr = neg_resp_map.get(rid)
                if rr:
                    prefix = f"{svc_short}.{rr.shortName or 'NegResponse'}" if svc_short else (rr.shortName or "")
                    self._prefix_path(rr.params, prefix)
                    self._annotate_service_name(rr.params, svc_short)
                    neg_responses.append(rr)
                    attached_neg_ids.add(rid)

            for el in inline_neg:
                rshort = get_text_local(el, "SHORT-NAME") or (svc_short + "_neg")
                root_path = svc_short if svc_short else ""
                root_path = f"{root_path}.{rshort}" if rshort else root_path

                rparams: List[OdxParam] = []
                for p_el in findall_descendants(el, "PARAM"):
                    rp = self._try_parse_param(
                        p_el,
                        "NEG_RESPONSE",
                        root_path,
                        layer_short,
                        svc_short,
                        dop_by_id,
                        dop_by_sn,
                        dop_meta_by_id,
                        struct_by_id,
                        struct_by_sn,
                        table_by_id,
                    )
                    if rp is not None:
                        rparams.append(rp)

                self._annotate_service_name(rparams, svc_short)
                neg_responses.append(
                    OdxMessage(
                        id=get_attr(el, "ID"),
                        shortName=rshort,
                        longName=get_text_local(el, "LONG-NAME"),
                        params=rparams,
                    )
                )

            services.append(
                OdxService(
                    id=svc_attrs.get("ID", ""),
                    shortName=svc_short,
                    longName=get_text_local(svc_el, "LONG-NAME"),
                    description=get_text_local(svc_el, "DESC"),
                    semantic=svc_attrs.get("SEMANTIC", ""),
                    addressing=svc_attrs.get("ADDRESSING", ""),
                    request=request,
                    posResponses=pos_responses,
                    negResponses=neg_responses,
                    attrs=svc_attrs,
                )
            )

        # ------------------------------------------------------------
        # Layer metadata
        # ------------------------------------------------------------
        parent_id = get_attr(find_child(layer_el, "PARENT-REF"), "ID-REF") or ""
        linked_ids = self._collect_links(layer_el)
        ni_sn, ni_ids = self._parse_not_inherited(layer_el)

        layer = OdxLayer(
            layerType=layerType,
            id=get_attr(layer_el, "ID", ""),
            shortName=layer_short,
            longName=get_text_local(layer_el, "LONG-NAME"),
            description=get_text_local(layer_el, "DESC"),
            parentId=parent_id,
            rxId=get_text_local(layer_el, "RECEIVE-ID"),
            txId=get_text_local(layer_el, "TRANSMIT-ID"),
            services=services,
            units=units,
            compuMethods=compu_methods,
            dataObjectProps=list(dop_by_id.values()),
            dtcs=dtcs,
            attrs=get_all_attrs(layer_el),
            linkedLayerIds=linked_ids,
        )

        if ni_sn:
            layer.attrs["NI_DIAGCOMM_SN"] = "|".join(sorted(ni_sn))
        if ni_ids:
            layer.attrs["NI_DIAGCOMM_ID"] = "|".join(sorted(ni_ids))

        return layer


    def _collect_links(self, layer_el: ET.Element) -> List[str]:
        """
        Collect IDs of referenced layers via:
        - DIAG-LAYER-LINKS / DIAG-LAYER-LINK  /*-REF @ID-REF
        - direct BASE-VARIANT-REF, PROTOCOL-REF, FUNCTIONAL-GROUP-REF, ECU-SHARED-DATA-REF
        """
        links: List[str] = []

        links_el = find_child(layer_el, "DIAG-LAYER-LINKS")
        if links_el is not None:
            for lnk in get_elements(links_el, "DIAG-LAYER-LINK"):
                for child in list(lnk):
                    tag = local_name(child.tag)
                    if tag.endswith("-REF"):
                        ref_id = get_attr(child, "ID-REF")
                        if ref_id:
                            links.append(ref_id)

        for tag in ("BASE-VARIANT-REF",
                    "PROTOCOL-REF",
                    "FUNCTIONAL-GROUP-REF",
                    "ECU-SHARED-DATA-REF"):
            el = find_child(layer_el, tag)
            if el is not None:
                ref_id = get_attr(el, "ID-REF")
                if ref_id:
                    links.append(ref_id)

        prefs = find_child(layer_el, "PARENT-REFS")
        if prefs is not None:
            for pref in find_children(prefs, "PARENT-REF"):
                ref_id = get_attr(pref, "ID-REF")
                if ref_id:
                    links.append(ref_id)

        # de-duplicate preserving order
        seen: Set[str] = set()
        uniq: List[str] = []
        for lid in links:
            if lid not in seen:
                uniq.append(lid)
                seen.add(lid)

        return uniq


    # ================================================
    # ---- MAIN CONTAINER PARSER ----
    # ================================================
    def parse_container(self, root: ET.Element) -> OdxContainer:
        container_el = self._ensure_container(root)

        cont = OdxContainer()

        protos = findall_descendants(container_el, "PROTOCOL")
        fgroups = findall_descendants(container_el, "FUNCTIONAL-GROUP")
        bvars = findall_descendants(container_el, "BASE-VARIANT")
        evars = findall_descendants(container_el, "ECU-VARIANT")
        shared = findall_descendants(container_el, "ECU-SHARED-DATA")

        print(f"[ODXParser] Found layers: PROTOCOL={len(protos)}, "
              f"FUNCTIONAL-GROUP={len(fgroups)}, "
              f"BASE-VARIANT={len(bvars)}, "
              f"ECU-VARIANT={len(evars)}, "
              f"ECU-SHARED-DATA={len(shared)}")

        for p in protos:
            cont.protocols.append(self._parse_layer(p, "PROTOCOL"))

        for fg in fgroups:
            cont.functionalGroups.append(self._parse_layer(fg, "FUNCTIONAL-GROUP"))

        for bv in bvars:
            cont.baseVariants.append(self._parse_layer(bv, "BASE-VARIANT"))

        for ev in evars:
            cont.ecuVariants.append(self._parse_layer(ev, "ECU-VARIANT"))

        for sd in shared:
            cont.ecuSharedData.append(self._parse_layer(sd, "ECU-SHARED-DATA"))

        return cont

    # ================================================
    # Public ODX file parse entrypoint
    # ================================================
    def parse_odx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        root = self.parse_xml_bytes(content)
        return filename, self.parse_container(root)

    def parse_odx_file(self, filename: str, content: str) -> Tuple[str, OdxContainer]:
        return self.parse_odx_bytes(filename, content.encode("utf-8", errors="ignore"))
        return root

    # =====================================================================================
    # UNIT PARSER 
    # =====================================================================================
    def _parse_unit(self, unit_el: ET.Element) -> OdxUnit:
        return OdxUnit(
            id=get_attr(unit_el, "ID"),
            shortName=get_text_local(unit_el, "SHORT-NAME"),
            longName=get_text_local(unit_el, "LONG-NAME"),
            displayName=get_text_local(unit_el, "DISPLAY-NAME"),
            factorSiToUnit=get_text_local(unit_el, "FACTOR-SI-TO-UNIT"),
            offsetSiToUnit=get_text_local(unit_el, "OFFSET-SI-TO-UNIT"),
            physicalDimensionRef=get_attr(find_child(unit_el, "PHYSICAL-DIMENSION-REF"), "ID-REF"),
        )

    # =====================================================================================
    # COMPU-METHOD PARSER 
    # =====================================================================================
    def _parse_compu_method(self, compu_el: ET.Element) -> OdxCompuMethod:

        internal_to_phys = find_child(compu_el, "COMPU-INTERNAL-TO-PHYS")
        scales: List[OdxCompuScale] = []

        if internal_to_phys is not None:
            for scale in get_elements(internal_to_phys, "COMPU-SCALE"):
                compuConst = find_child(scale, "COMPU-CONST")
                compuRational = find_child(scale, "COMPU-RATIONAL-COEFFS")

                scales.append(
                    OdxCompuScale(
                        lowerLimit=get_text_local(scale, "LOWER-LIMIT"),
                        upperLimit=get_text_local(scale, "UPPER-LIMIT"),
                        compuConstV=get_text_local(compuConst, "V") if compuConst is not None else "",
                        compuConstVT=get_text_local(compuConst, "VT") if compuConst is not None else "",
                        numerators=[(n.text or "") for n in get_elements(compuRational, "NUM")] if compuRational is not None else [],
                        denominators=[(d.text or "") for d in get_elements(compuRational, "DEN")] if compuRational is not None else [],
                    )
                )

        # ---- TEXTTABLE TABLE-ROWS SUPPORT ----
        table_rows: List[OdxTableRow] = []
        for tr in findall_descendants(compu_el, "TABLE-ROW"):
            table_rows.append(
                OdxTableRow(
                    id=get_attr(tr, "ID"),
                    shortName=get_text_local(tr, "SHORT-NAME"),
                    longName=get_text_local(tr, "LONG-NAME"),
                    description=get_text_local(tr, "DESC"),
                    key=get_text_local(tr, "KEY"),
                    structureRefId=get_attr(find_child(tr, "STRUCTURE-REF"), "ID-REF"),
                )
            )

        return OdxCompuMethod(
            id=get_attr(compu_el, "ID"),
            shortName=get_text_local(compu_el, "SHORT-NAME"),
            longName=get_text_local(compu_el, "LONG-NAME"),
            category=get_text_local(compu_el, "CATEGORY"),
            scales=scales,
            tableRows=table_rows
        )

    def flatten_service_params(self, service: OdxService) -> List[OdxParam]:
        out: List[OdxParam] = []

        if service.request:
            out.extend(service.request.params)

        for resp in service.posResponses:
            out.extend(resp.params)

        for resp in service.negResponses:
            out.extend(resp.params)

        return out

    
    def _parse_not_inherited(self, layer_el: ET.Element) -> Tuple[Set[str], Set[str]]:
        """
        Extract NOT-INHERITED-DIAG-COMMS from PARENT-REFS.
        Returns (excluded_service_short_names, excluded_service_ids)
        """
        excluded_sn: Set[str] = set()
        excluded_ids: Set[str] = set()

        prefs = find_child(layer_el, "PARENT-REFS")
        if prefs is None:
            return excluded_sn, excluded_ids

        for pref in find_children(prefs, "PARENT-REF"):
            not_inh = find_child(pref, "NOT-INHERITED-DIAG-COMMS")
            if not_inh is None:
                continue

            for nic in find_children(not_inh, "NOT-INHERITED-DIAG-COMM"):
                snref = find_child(nic, "DIAG-COMM-SNREF")
                if snref is not None:
                    sn = get_attr(snref, "SHORT-NAME") or get_text_local(snref, "SHORT-NAME")
                    excluded_sn.add(sn)

                idref = find_child(nic, "DIAG-COMM-REF")
                if idref is not None:
                    rid = get_attr(idref, "ID-REF")
                    if rid:
                        excluded_ids.add(rid)

        return excluded_sn, excluded_ids

    def parse_param(
            self,
            param_el: ET.Element,
            parentType: str,
            parentPath: str,   # full dot path
            layerName: str,
            serviceShortName: str,
            dop_by_id: Dict[str, OdxDataObjectProp],
            dop_by_sn: Dict[str, OdxDataObjectProp],
            dop_meta_by_id: Dict[str, Dict[str, str]],
            struct_by_id: Dict[str, List[ET.Element]],
            struct_by_sn: Dict[str, List[ET.Element]],
            table_by_id: Dict[str, Dict],
        ) -> OdxParam:

        attrs = get_all_attrs(param_el)

        codedConst = find_child(param_el, "CODED-CONST")
        physConst = find_child(param_el, "PHYS-CONST")
        dopRef = find_child(param_el, "DOP-REF")
        dopSnRef = find_child(param_el, "DOP-SNREF")
        compuRef = find_child(param_el, "COMPU-METHOD-REF")
        diagCodedType = find_child(param_el, "DIAG-CODED-TYPE")
        physType = find_child(param_el, "PHYSICAL-TYPE")

        shortName = get_text_local(param_el, "SHORT-NAME")
        semantic = (
            attrs.get("SEMANTIC")
            or attrs.get("semantic")
            or get_text_local(param_el, "SEMANTIC")
            or ""
        )

        coded_value = extract_coded_value(codedConst) if codedConst is not None else ""
        if not coded_value:
            coded_value = extract_coded_value(param_el)  # fallback

        pid = f"{layerName}::{serviceShortName}::{parentType}::{shortName}::{uuid.uuid4().hex[:9]}"

        p = OdxParam(
            id=pid,
            shortName=shortName,
            longName=get_text_local(param_el, "LONG-NAME"),
            description=get_text_local(param_el, "DESC"),
            semantic=semantic,
            bytePosition=get_text_local(param_el, "BYTE-POSITION"),
            bitPosition=get_text_local(param_el, "BIT-POSITION"),
            bitLength=get_text_local(diagCodedType, "BIT-LENGTH") if diagCodedType else "",
            minLength=get_text_local(diagCodedType, "MIN-LENGTH") if diagCodedType else "",
            maxLength=get_text_local(diagCodedType, "MAX-LENGTH") if diagCodedType else "",
            baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE") if diagCodedType else "",
            physicalBaseType=get_attr(physType, "BASE-DATA-TYPE") if physType else "",
            isHighLowByteOrder=get_attr(diagCodedType, "IS-HIGH-LOW-BYTE-ORDER") if diagCodedType else "",
            codedConstValue=coded_value,
            physConstValue=get_text_local(physConst, "V") if physConst else "",
            dopRefId=get_attr(dopRef, "ID-REF") if dopRef else "",
            dopSnRefName=get_text_local(dopSnRef, "SHORT-NAME") if dopSnRef else "",
            compuMethodRefId=get_attr(compuRef, "ID-REF") if compuRef else "",
            parentType=parentType,
            parentName=parentPath,
            layerName=layerName,
            serviceShortName=serviceShortName,
            attrs=attrs,
        )

        # --------------------------------------------------------
        # Resolve DOP & fill missing
        # --------------------------------------------------------
        dop: Optional[OdxDataObjectProp] = None
        if p.dopRefId:
            dop = dop_by_id.get(p.dopRefId)
        if dop is None and p.dopSnRefName:
            dop = dop_by_sn.get(p.dopSnRefName)

        self._fill_from_dop_if_missing(p, dop, dop_meta_by_id)

        # --------------------------------------------------------
        # Children dot path
        # --------------------------------------------------------
        next_path = f"{parentPath}.{shortName}" if parentPath else shortName

        # --------------------------------------------------------
        # (A) DOP owns structureParams
        # --------------------------------------------------------
        if dop and getattr(dop, "structureParams", None):
            for child_el in dop.structureParams:
                child = self._try_parse_param(
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
                if child is not None:
                    p.children.append(child)

        else:
            # ----------------------------------------------------
            # (B) DOP-REF points to STRUCTURE id/sn
            # ----------------------------------------------------
            struct_params: List[ET.Element] = []

            if p.dopRefId and p.dopRefId in struct_by_id:
                struct_params = struct_by_id[p.dopRefId]
            elif p.dopSnRefName and p.dopSnRefName in struct_by_sn:
                struct_params = struct_by_sn[p.dopSnRefName]

            if struct_params:
                for child_el in struct_params:
                    child = self._try_parse_param(
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
                    if child is not None:
                        p.children.append(child)

            else:
                # ------------------------------------------------
                # (C) Direct STRUCTURE-REF
                # ------------------------------------------------
                struct_ref = find_child(param_el, "STRUCTURE-REF")
                if struct_ref is not None:
                    ref_id = get_attr(struct_ref, "ID-REF")
                    ref_sn = get_text_local(struct_ref, "SHORT-NAME")

                    struct_params = (
                        struct_by_id.get(ref_id) if ref_id else None
                    ) or (
                        struct_by_sn.get(ref_sn) if ref_sn else None
                    ) or []

                    for child_el in struct_params:
                        child = self._try_parse_param(
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
                        if child is not None:
                            p.children.append(child)

        # --------------------------------------------------------
        # (D) TABLE-KEY expansion
        # --------------------------------------------------------
        table_ref = find_child(param_el, "TABLE-REF")
        if table_ref is not None:
            tbl_id = get_attr(table_ref, "ID-REF")
            tbl = table_by_id.get(tbl_id)

            if tbl:
                for key_dop, dop_meta in tbl.get("keyParams", {}).items():
                    row_label = dop_meta.get("shortName") or "Row"
                    row_short = f"{row_label}_{uuid.uuid4().hex[:9]}"

                    row_param = OdxParam(
                        id=f"{pid}::{row_short}",
                        shortName=row_short,
                        longName=row_label,
                        description="",
                        semantic="TABLE-ROW",
                        parentType="TABLE-KEY",
                        parentName=next_path,
                        layerName=layerName,
                        serviceShortName=serviceShortName,
                        attrs={"TABLE-SHORT-NAME": tbl.get("shortName", "")},
                    )

                    row_next_path = f"{next_path}.{row_short}"

                    for child_el in tbl.get("structParams", []):
                        child = self._try_parse_param(
                            child_el,
                            "STRUCTURE",
                            row_next_path,
                            layerName,
                            serviceShortName,
                            dop_by_id,
                            dop_by_sn,
                            dop_meta_by_id,
                            struct_by_id,
                            struct_by_sn,
                            table_by_id,
                        )
                        if child is not None:
                            row_param.children.append(child)

                    p.children.append(row_param)

        if p.children:
            print(f"[STRUCTURE/TABLE] Expanded {p.shortName} -> {len(p.children)} child param(s)")

        return p


    
    def flatten_layer_params(self, layer: OdxLayer) -> List[OdxParam]:
        out: List[OdxParam] = []
        for svc in layer.services:
            out.extend(self.flatten_service_params(svc))
        return out

    def _dedup_services(self, services: List[OdxService]) -> List[OdxService]:
        """
        Remove duplicate services while preserving order.
        Deduplicate primarily by:
            1) service ID if available
            2) otherwise fallback to SHORT-NAME
        """

        seen: Set[str] = set()
        result: List[OdxService] = []

        for svc in services:
            key = svc.id or svc.shortName
            if not key:
                # If somehow completely missing identifiers, keep it
                result.append(svc)
                continue

            if key in seen:
                continue

            seen.add(key)
            result.append(svc)

        return result

    def _get_not_inherited_sets(self, layer: OdxLayer) -> Tuple[Set[str], Set[str]]:
        sn = set(); id = set()
        if layer.attrs:
            s = layer.attrs.get("NI_DIAGCOMM_SN", "")
            if s:
                sn = {x for x in s.split("|") if x}
            i = layer.attrs.get("NI_DIAGCOMM_ID", "")
            if i:
                id = {x for x in i.split("|") if x} 
        return sn, id        
   
    def _resolve_links_for_layer(
        self,
        layer: OdxLayer,
        id_map: Dict[str, OdxLayer],
        visited: Set[str]
    ) -> None:
        """
        Extend 'layer' with content from referenced layers via linkedLayerIds.
        Prevent cycles using 'visited'.
        """

        if not layer.linkedLayerIds:
            return

        if layer.id in visited:
            return

        visited.add(layer.id)

        ni_sn, ni_ids = self._get_not_inherited_sets(layer)

        for ref_id in layer.linkedLayerIds:
            ref_layer = id_map.get(ref_id)
            if not ref_layer:
                continue
        
            # Recursively resolve the referenced layer first
            self._resolve_links_for_layer(ref_layer, id_map, visited)

            for ref_id in ref_layer.linkedLayerIds:
                ref = id_map.get(ref_id)
                if not ref:
                    continue
                self._resolve_links_for_layer(ref_layer, id_map, visited)
              
                if ni_sn or ni_ids:
                    # Filter services based on NOT-INHERITED sets
                    filtered_services = []
                    for svc in ref.services:
                        if (svc.shortName and svc.shortName in ni_sn) or (svc.id and svc.id in ni_ids):
                            continue
                        if(svc.id and svc.id in ni_ids):
                            continue
                        filtered_services.append(svc)
                    ref_layer.services.extend(filtered_services)
                else:
                    layer.services.extend(ref.services)
                
                layer.units.extend(ref.units)
                layer.compuMethods.extend(ref.compuMethods) 
                layer.dataObjectProps.extend(ref.dataObjectProps)
                layer.dtcs.extend(ref.dtcs) 
            
        # Deduplicate services after extending
        layer.services = self._dedup_services(layer.services)

    # =====================================================================================
    # MERGE CONTAINERS  (EXACT FROM YOUR SCREENSHOT)
    # =====================================================================================
    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:

        db = OdxDatabase()

        for c in containers:
            db.ecuVariants.extend(c.ecuVariants)
            db.baseVariants.extend(c.baseVariants)
            db.protocols.extend(c.protocols)
            db.functionalGroups.extend(c.functionalGroups)
            db.ecuSharedData.extend(c.ecuSharedData)

        # ---- Collect ALL layers
        all_layers = (
            db.ecuVariants
            + db.baseVariants
            + db.protocols
            + db.functionalGroups
            + db.ecuSharedData
        )

        id_map: Dict[str, OdxLayer] = {lay.id: lay for lay in all_layers if lay.id}

        # ---- two-pass link resolution 
        
        for _ in range(2):
            for lay in all_layers:
                self._resolve_links_for_layer(lay, id_map, set())

        # ---- FLATTEN + ANNOTATE ----
        for layer in all_layers:

            # Params
            for p in self.flatten_layer_params(layer):
                p.layerName = layer.shortName
                db.allParams.append(p)

            # Units
            for u in layer.units:
                dd = asdict(u)
                dd["layerName"] = layer.shortName
                db.allUnits.append(dd)

            # Compu Methods
            for cm in layer.compuMethods:
                dd = asdict(cm)
                dd["layerName"] = layer.shortName
                db.allCompuMethods.append(dd)

            # DOP
            for dop in layer.dataObjectProps:
                dd = asdict(dop)
                dd["layerName"] = layer.shortName
                dd.pop("structureParams", None)
                db.allDataObjects.append(dd)

            # DTC
            for dtc in layer.dtcs:
                dd = asdict(dtc)
                dd["layerName"] = layer.shortName
                db.allDTCs.append(dd)

        return db

# =====================================================================================
# END
# =====================================================================================
