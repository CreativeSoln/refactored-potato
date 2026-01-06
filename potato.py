from __future__ import annotations
import uuid
import re
import html
import logging
logger = logging.getLogger(__name__)
import xml.etree.ElementTree as ET
from dataclasses import asdict
from typing import List, Dict, Tuple, Optional, Set, Callable
import hashlib

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
from diagnostics.formatting import FormatterService

# ------------------------------ XML helpers ------------------------------
def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def get_all_attrs(el: Optional[ET.Element]) -> Dict[str, str]:
    return {} if el is None else dict(el.attrib)

def get_attr(el: Optional[ET.Element], name: str, default: str = '') -> str:
    if el is None:
        return default
    return el.attrib.get(name, default)

def get_text_local(el: Optional[ET.Element], name: str) -> str:
    if el is None:
        return ''
    for c in el:
        if local_name(c.tag) == name:
            return ''.join(c.itertext()).strip()
    return ''

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
    if el is None:
        return ''
    for t in tag_names:
        for node in el.iter():
            if local_name(node.tag) == t:
                txt = (node.text or '').strip()
                if txt:
                    return txt
    return ''

def get_attr_ci(el: Optional[ET.Element], *names: str) -> str:
    if el is None or not el.attrib:
        return ''
    low = {k.lower(): v for k, v in el.attrib.items()}
    for n in names:
        v = low.get(n.lower())
        if v:
            return v
    return ''

def extract_coded_value(scope: Optional[ET.Element]) -> str:
    if scope is None:
        return ''
    cv = first_text(scope, ['CODED-VALUE'])
    if cv:
        return cv
    v = first_text(scope, ['V'])
    if v:
        return v
    a = get_attr_ci(scope, 'CODED-VALUE')
    return a or ''

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
        if "<" in text and ">" in text and r"\\<" not in text[:200]:
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

# ------------------------------ Parser ------------------------------
class ODXParser:
    def __init__(self) -> None:
        self._fmt = FormatterService()

    import hashlib

    def _make_param_id(
        self,
        layerName: str,
        serviceShortName: str,
        parentType: str,
        parentPath: str,
        shortName: str,
        semantic: str,
    ) -> str:
        base = "|".join([
            layerName or "",
            serviceShortName or "",
            parentType or "",
            parentPath or "",
            shortName or "",
            semantic or "",
        ])
        digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
        return f"{layerName}::{serviceShortName}::{parentType}::{shortName}::{digest}"


    # --- injected helper (_log_and_prefix) from image ---
    def _log_and_prefix(self, params: List[OdxParam], prefix: str, context: str) -> None:
        if not params:
            logger.debug('[prefix:%s] no params to prefix', context)
            return
        logger.debug('[prefix:%s] prefix=%r count=%d', context, prefix, len(params))
        self._prefix_path(params, prefix)

    def _clone_message(self, src: OdxMessage) -> OdxMessage:
        return OdxMessage(
            id=src.id,
            shortName=src.shortName,
            longName=src.longName,
            params=list(src.params or []),
        )

    
    # XML root parser
    def parse_xml_bytes(self, content: bytes) -> ET.Element:
        return _try_parse_bytes(content)

    def parse_xml(self, content: str) -> ET.Element:
        return self.parse_xml_bytes(content.encode("utf-8", errors="ignore"))

    
    def _annotate_service_name(self, params: List[OdxParam], svc_short: str) -> None:
        if not params:
            return
        stack: List[OdxParam] = [p for p in params if isinstance(p, OdxParam)]
        while stack:
            node = stack.pop()
            node.serviceShortName = svc_short
            for c in getattr(node, 'children', []) or []:
                if isinstance(c, OdxParam):
                    stack.append(c)


    def _prefix_path(self, params: List[OdxParam], prefix: str) -> None:
        if not params or not prefix:
            logger.debug('[prefix] skip: params=%d prefix=%r', 0 if not params else len(params), prefix)
            return
        logger.debug('[prefix] applying prefix=%r to %d param(s)', prefix, len(params))
        stack: List[OdxParam] = [p for p in params if isinstance(p, OdxParam)]
        while stack:
            node = stack.pop()
            before = node.parentName or ''
            after = f'{prefix}.{before}' if before else prefix
            logger.debug('[prefix] %s: %r', getattr(node, 'shortName', '<param>'), before, after)
            node.parentName = after
            for c in getattr(node, 'children', []) or []:
                if isinstance(c, OdxParam):
                    stack.append(c)


    # --- param utilities ---
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
            logger.warning("Skipping PARAM: %s", ex, exc_info=True)
            return None

    def _fill_from_dop_if_missing(
        self,
        p: OdxParam,
        dop: Optional[OdxDataObjectProp],
        dop_meta_by_id: Dict[str, Dict[str, str]],
    ) -> None:
        meta: Dict[str, str] = {}
        base: str = ''
        phys: str = ''
        bitlen: str = ''
        if dop is not None:
            did = getattr(dop, 'id', '')
            if did:
                meta = dop_meta_by_id.get(did, {}) or {}
        base = getattr(dop, 'baseDataType', '') if dop else ''
        phys = getattr(dop, 'physicalBaseDataType', '') if dop else ''
        bitlen = getattr(dop, "bitLength", '') if dop else ''
        if not p.baseDataType and base:
            p.baseDataType = base
        if not p.physicalBaseType and phys:
            p.physicalBaseType = phys
        if not p.bitLength and bitlen:
            p.bitLength = bitlen
        minlen = meta.get("minLength", '')
        maxlen = meta.get("maxLength", '')
        if not p.minLength and minlen:
            p.minLength = minlen
        if not p.maxLength and maxlen:
            p.maxLength = maxlen

    
    def _parse_dop_with_struct_map(
        self,
        dop_el: ET.Element,
        struct_by_id: Dict[str, List[ET.Element]],
        struct_by_sn: Dict[str, List[ET.Element]],
    ) -> Tuple[OdxDataObjectProp, Dict[str, str]]:
        diagCodedType = find_child(dop_el, 'DIAG-CODED-TYPE')
        phySType = find_child(dop_el, 'PHYSICAL-TYPE')
        unitRef = find_child(dop_el, 'UNIT-REF')
        compuMethod = find_child(dop_el, 'COMPU-METHOD')
        structure = find_child(dop_el, 'STRUCTURE')
        structure_ref = find_child(dop_el, 'STRUCTURE-REF')
        struct_params: List[ET.Element] = []
        if structure is not None:
            params_block = find_child(structure, 'PARAMS')
            if params_block is not None:
                struct_params = find_children(params_block, 'PARAM')
            else:
                struct_params = find_children(structure, 'PARAM')
        if not struct_params:
            struct_params = findall_descendants(structure, 'PARAM') if structure is not None else []
        if structure_ref is not None and structure is not None:
            ref_id = get_attr(structure_ref, 'ID-REF')
            ref_sn = get_text_local(structure_ref, 'SHORT-NAME')
            if ref_id and ref_id in struct_by_id:
                struct_params = struct_by_id[ref_id]
            elif ref_sn and ref_sn in struct_by_sn:
                struct_params = struct_by_sn[ref_sn]

        dd = OdxDataObjectProp(
            id=get_attr(dop_el, "ID"),
            shortName=get_text_local(dop_el, 'SHORT-NAME'),
            longName=get_text_local(dop_el, 'LONG-NAME'),
            description=get_text_local(dop_el, 'DESC'),
            baseDataType=get_text_local(diagCodedType, 'BASE-DATA-TYPE') if diagCodedType is not None else '',
            bitLength=get_text_local(diagCodedType, 'BIT-LENGTH') if diagCodedType is not None else '',
            physicalBaseDataType=get_text_local(phySType, 'BASE-DATA-TYPE') if phySType is not None else '',
            unitRefId=get_attr(unitRef, 'ID-REF') if unitRef is not None else '',
            compuCategory=get_text_local(compuMethod, 'CATEGORY') if compuMethod is not None else '',
            structureParams=struct_params,
        )

        meta = {
            'minLength': get_text_local(diagCodedType, 'MIN-LENGTH') if diagCodedType is not None else '',
            'maxLength': get_text_local(diagCodedType, 'MAX-LENGTH') if diagCodedType is not None else '',
        }
        return dd, meta

    def _parse_dop(self, dop_el: ET.Element) -> OdxDataObjectProp:
        diagCodedType = find_child(dop_el, "DIAG-CODED-TYPE")
        physType = find_child(dop_el, "PHYSICAL-TYPE")
        unitRef = find_child(dop_el, "UNIT-REF")
        compuMethod = find_child(dop_el, "COMPU-METHOD")
        structure = find_child(dop_el, "STRUCTURE")
        return OdxDataObjectProp(
            id=get_attr(dop_el, "ID"),
            shortName=get_text_local(dop_el, "SHORT-NAME"),
            longName=get_text_local(dop_el, "LONG-NAME"),
            description=get_text_local(dop_el, "DESC"),
            baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE") if diagCodedType is not None else '',
            bitLength=get_text_local(diagCodedType, "BIT-LENGTH") if diagCodedType is not None else '',
            physicalBaseDataType=get_attr(physType, "BASE-DATA-TYPE") if physType is not None else '',
            unitRefId=get_attr(unitRef, "ID-REF") if unitRef is not None else '',
            compuCategory=get_text_local(compuMethod, "CATEGORY") if compuMethod is not None else '',
            structureParams=get_elements(structure, "PARAM") if structure is not None else [],
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

    # Ensure container root
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
    # LAYER PARSER
    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
        layer_short = get_text_local(layer_el, 'SHORT-NAME')

        # ---------------------------------------------------------
        # STRUCTURES
        # ---------------------------------------------------------
        struct_by_id, struct_by_sn = harvest_structures(layer_el)

        # ---------------------------------------------------------
        # DOPs + meta
        # ---------------------------------------------------------
        dop_by_id: Dict[str, OdxDataObjectProp] = {}
        dop_by_sn: Dict[str, OdxDataObjectProp] = {}
        dop_meta_by_id: Dict[str, Dict[str, str]] = {}

        for d in findall_descendants(layer_el, 'DATA-OBJECT-PROP'):
            dd, meta = self._parse_dop_with_struct_map(d, struct_by_id, struct_by_sn)
            if dd.id:
                dop_by_id[dd.id] = dd
                dop_meta_by_id[dd.id] = meta
            if dd.shortName:
                dop_by_sn[dd.shortName] = dd

        
        # ---------------------------------------------------------
        # UNITS / COMPU / DTC
        # ---------------------------------------------------------
        units: List[OdxUnit] = [self._parse_unit(u) for u in findall_descendants(layer_el, 'UNIT')]
        compu_methods: List[OdxCompuMethod] = [self._parse_compu_method(c) for c in findall_descendants(layer_el, 'COMPU-METHOD')]  
        dtcs: List[OdxDTC] = [self._parse_dtc(d) for d in findall_descendants(layer_el, 'DTC')] 

        parent_id = get_attr(find_child(layer_el, 'PARENT-REF'), 'ID-REF') or ''
        linked_ids = self._collect_links(layer_el)
        ni_sn, ni_ids = self._parse_not_inherited(layer_el)
        
        
        # ---------------------------------------------------------
        # TABLES (for TABLE-KEY)
        # ---------------------------------------------------------
        table_by_id: Dict[str, Dict] = {}
        for t in findall_descendants(layer_el, 'TABLE'):
            tid = get_attr(t, 'ID')
            tsn = get_text_local(t, 'SHORT-NAME')
            key_dop_ref = getattr(find_child(t, 'KEY-DOP-REF'), 'ID-REF')

            rows = []
            for tr in findall_descendants(t, 'TABLE-ROW'):
                rid = get_attr(tr, 'ID')
                rsn = get_text_local(tr, 'SHORT-NAME')  
                rkey = get_text_local(tr, 'KEY')
                sref = find_child(tr, 'STRUCTURE-REF')
                struct_params = []
                if sref is not None:
                    ref_id = get_attr(sref, 'ID-REF')
                    ref_sn = get_text_local(sref, 'SHORT-NAME')
                    struct_params = (struct_by_id.get(ref_id) or struct_by_sn.get(ref_sn) or [])
                rows.append({
                    'id': rid,
                    'shortName': rsn,
                    'key': rkey,
                    'structParams': struct_params
                })
            if tid:
                table_by_id[tid] = {
                    'shortName': tsn,
                    'keyDopRefId': key_dop_ref,
                    'rows': rows
                }

        # ---------------------------------------------------------
        # STANDALONE MESSAGE MAPS
        # ---------------------------------------------------------
        request_map: Dict[str, OdxMessage] = {}
        pos_resp_map: Dict[str, OdxMessage] = {}
        neg_resp_map: Dict[str, OdxMessage] = {}

        
        def collect_params(el, ptype, msg_short):
            found: List[ET.Element] = []
            params_block = find_child(el, 'PARAMS')
            if params_block is not None:
                found = find_children(params_block, 'PARAM')
            else:
                found = find_children(el, 'PARAM')
            if not found:
                found = findall_descendants(el, 'PARAM')
            logger.debug("[collect] %s %s: found %d PARAM node(s)", ptype, msg_short, len(found))
            out: List[OdxParam] = []
            for p_el in found:
                cp = self._try_parse_param(
                    p_el, ptype, msg_short, layer_short, msg_short,
                    dop_by_id, dop_by_sn, dop_meta_by_id,
                    struct_by_id, struct_by_sn, table_by_id
                )
                if cp:
                    out.append(cp)
                else:
                    shortN = get_text_local(p_el, 'SHORT-NAME') or "<no-short>"
                    semN = get_all_attrs(p_el).get('SEMANTIC', '')
                    logger.debug("[collect] %s %s: skip param short=%s semantic=%s", ptype, msg_short, shortN, semN)
            logger.debug("[collect] %s %s: parsed %d/%d PARAM(s)", ptype, msg_short, len(out), len(found))
            return out
    


        # REQUEST
        for req in findall_descendants(layer_el, 'REQUEST'):
            rid = get_attr(req, 'ID')
            rshort = get_text_local(req, 'SHORT-NAME')
            request_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(req, 'LONG-NAME'),
                params=collect_params(req, "REQUEST", rshort),
            )

        # POS-RESPONSE
        for res in findall_descendants(layer_el, 'POS-RESPONSE'):
            rid = get_attr(res, 'ID')
            rshort = get_text_local(res, 'SHORT-NAME')
            params = collect_params(res, 'POS_RESPONSE', rshort)
            pos_resp_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(res, 'LONG-NAME'),
                params=params,
            )

        # NEG-RESPONSE
        for res in findall_descendants(layer_el, 'NEG-RESPONSE'):
            rid = get_attr(res, 'ID')
            rshort = get_text_local(res, 'SHORT-NAME')
            params = collect_params(res, 'NEG_RESPONSE', rshort)
            neg_resp_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(res, "LONG-NAME"),
                params=params,
            )

        # ---------------------------------------------------------
        # SERVICES (inline + references + fallback)
        # ---------------------------------------------------------
        services: List[OdxService] = []

        
        for svc_el in findall_descendants(layer_el, 'DIAG-SERVICE'):
            svc_short = get_text_local(svc_el, 'SHORT-NAME')
            svc_key = (svc_short or '').lower()
            attrs = get_all_attrs(svc_el)
            logger.debug('[service] %s: processing REQUEST attach', svc_short)
            #request    
            req = None
            ref = find_child(svc_el, 'REQUEST-REF')
            if not ref:
                ref_refs = find_child(svc_el, 'REQUEST-REFS')
                if ref_refs:
                    ref = find_child(ref_refs, 'REQUEST-REF')

            if ref:
                base_id = get_attr(ref, 'ID-REF')
                base = request_map.get(base_id)
                logger.debug('[service] %s: REQUEST-REF -> %s found=%s', svc_short, base_id, bool(base))
                if base:
                    req = self._clone_message(base)
                    self._log_and_prefix(req.params, (svc_short + '.' + (req.shortName or 'Request')) if svc_short else (req.shortName or 'Request'), 'request-ref')
                    self._annotate_service_name(req.params, svc_short)
            else:
                inline = find_child(svc_el, 'REQUEST')
                if inline:
                    sn = get_text_local(inline, 'SHORT-NAME') or svc_short + '_req'
                    req_prefix = (svc_short + '.' + sn if svc_short else sn)
                    req = OdxMessage(
                        id=get_attr(inline, 'ID'),
                        shortName=sn,
                        longName=get_text_local(inline, 'LONG-NAME'),
                        params=collect_params(inline, 'REQUEST', sn)
                    )
                    self._annotate_service_name(req.params, svc_short)
                    self._log_and_prefix(req.params, req_prefix, 'request-inline')
            #request orphan fallback    
            if req is None and request_map:
                for rid, msg in list(request_map.items()):
                    mname = (msg.shortName or '').lower()
                    if svc_key and (svc_key in mname) or mname.startswith(svc_key):
                        req = self._clone_message(msg)
                        self._log_and_prefix(req.params, (svc_short + '.' + (msg.shortName or 'Request')) if svc_short else (msg.shortName or 'Request'), 'request-fallback')
                        self._annotate_service_name(req.params, svc_short)
                        logger.debug('[service] %s: REQUEST fallback -> %s', svc_short, msg.shortName)
                        break
            if req is None:
                logger.debug('[service] %s: REQUEST not attached (no ref/inline/fallback)', svc_short)

            # POS-RESPONSES
            pos: List[OdxMessage] = []
            for r in find_children(svc_el, 'POS-RESPONSE-REF'):
                rr = pos_resp_map.get(get_attr(r, 'ID-REF'))
                if rr:
                    msg = self._clone_message(rr)
                    self._log_and_prefix(msg.params, (svc_short + '.' + (msg.shortName or 'PosResponse')) if svc_short else (msg.shortName or 'PosResponse'), 'pos-ref')
                    self._annotate_service_name(msg.params, svc_short)
                    pos.append(msg)

            pos_refs = find_child(svc_el, 'POS-RESPONSE-REFS')
            if pos_refs:
                for r in find_children(pos_refs, 'POS-RESPONSE-REF'):
                    rr = pos_resp_map.get(get_attr(r, 'ID-REF'))
                    if rr:
                        msg = self._clone_message(rr)
                        self._log_and_prefix(msg.params, (svc_short + '.' + (msg.shortName or 'PosResponse')) if svc_short else (msg.shortName or 'PosResponse'), 'pos-refs')
                        self._annotate_service_name(msg.params, svc_short)
                        pos.append(msg)

            for el in find_children(svc_el, 'POS-RESPONSE'):
                sn = get_text_local(el, 'SHORT-NAME') or svc_short + '_pos'
                msg = OdxMessage(
                    id=get_attr(el, 'ID'),
                    shortName=sn,
                    longName=get_text_local(el, 'LONG-NAME'),
                    params=collect_params(el, 'POS_RESPONSE', sn)
                )
                self._annotate_service_name(msg.params, svc_short)
                self._log_and_prefix(msg.params, (svc_short + '.' + sn) if svc_short else sn, 'pos-inline')
                pos.append(msg)

            # Fallback POS
            if not pos:
                for rid, msg in list(pos_resp_map.items()):
                    if any(r.id == rid for r in pos):
                        continue
                    mname = (msg.shortName or '').lower()
                    if svc_key and (svc_key in mname or mname.startswith(svc_key)):
                        self._log_and_prefix(msg.params, (svc_short + ' ' + (msg.shortName or 'PosResponse')) if svc_short else (msg.shortName or 'PosResponse'), 'pos-fallback')
                        self._annotate_service_name(msg.params, svc_short)
                        pos.append(msg)

            
            # NEG
            neg: List[OdxMessage] = []
            for rr in find_children(svc_el, 'NEG-RESPONSE-REF'):
                rm = neg_resp_map.get(get_attr(rr, 'ID-REF'))
                if rm:
                    msg = self._clone_message(rm)
                    self._log_and_prefix(msg.params, (svc_short + '.' + (msg.shortName or 'NegResponse')) if svc_short else (msg.shortName or 'NegResponse'), 'neg-ref')
                    self._annotate_service_name(msg.params, svc_short)
                    neg.append(msg)

            neg_refs = find_child(svc_el, 'NEG-RESPONSE-REFS')
            if neg_refs:
                for rr in find_children(neg_refs, 'NEG-RESPONSE-REF'):
                    rm = neg_resp_map.get(get_attr(rr, 'ID-REF'))
                    if rm:
                        msg = self._clone_message(rm)
                        self._log_and_prefix(msg.params, (svc_short + '.' + (msg.shortName or 'NegResponse')) if svc_short else (msg.shortName or 'NegResponse'), 'neg-refs')
                        self._annotate_service_name(msg.params, svc_short)
                        neg.append(msg)

            for el in find_children(svc_el, 'NEG-RESPONSE'):
                sn = get_text_local(el, 'SHORT-NAME') or svc_short + '_neg'
                msg = OdxMessage(
                    id=get_attr(el, 'ID'),
                    shortName=sn,
                    longName=get_text_local(el, 'LONG-NAME'),
                    params=collect_params(el, 'NEG_RESPONSE', sn)
                )
                self._annotate_service_name(msg.params, svc_short)
                self._log_and_prefix(msg.params, (svc_short + '.' + sn) if svc_short else sn, 'neg-inline')
                neg.append(msg)
            
            # Fallback NEG
            for rid, msg in list(neg_resp_map.items()):
                if any(r.id == rid for r in neg):
                    mname = (msg.shortName or '').lower()
                    if svc_key and (svc_key in mname or mname.startswith(svc_key)):
                        self._log_and_prefix(msg.params, (svc_short + ' ' + (msg.shortName or 'NegResponse')) if svc_short else (msg.shortName or 'NegResponse'), 'neg-fallback')
                        self._annotate_service_name(msg.params, svc_short)
                        neg.append(msg)
        
            
            services.append(OdxService(
                id=attrs.get('ID', ''),
                shortName=svc_short,
                longName=get_text_local(svc_el, 'LONG-NAME'),
                description=get_text_local(svc_el, 'DESC'),
                semantic=attrs.get('SEMANTIC', ''),
                addressing=attrs.get('ADDRESSING', ''),
                request=req,
                posResponses=pos,
                negResponses=neg,
                attrs=attrs,
            ))

            # Summary log for attached param counts
            total_req = len(req.params) if req else 0
            total_pos = sum(len(m.params or []) for m in pos)
            total_neg = sum(len(m.params or []) for m in neg)
            logger.debug('[service] %s: attached REQ=%d, POS=%d param(s) across %d pos, NEG=%d across %d neg',
                        svc_short, total_req, total_pos, len(pos), total_neg, len(neg))

        layer = OdxLayer(
            layerType=layerType,
            id=get_attr(layer_el, 'ID'),
            shortName=layer_short,
            longName=get_text_local(layer_el, 'LONG-NAME'),
            description=get_text_local(layer_el, 'DESC'),
            parentId=parent_id,
            rxId=get_text_local(layer_el, 'RECEIVE-ID'),
            txId=get_text_local(layer_el, 'TRANSMIT-ID'),
            services=services,
            units=units,
            compuMethods=compu_methods,
            dataObjectProps=list(dop_by_id.values()),
            attrs=get_all_attrs(layer_el),
            linkedLayerIds=linked_ids,
        )

        if ni_sn:
            layer.attrs['NI_DIAGCOMM_SN'] = '\n'.join(sorted(ni_sn))
        if ni_ids:
            layer.attrs['NI_DIAGCOMM_ID'] = '\n'.join(sorted(ni_ids))

        return layer
    
    def _collect_links(self, layer_el: ET.Element) -> List[str]:
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
        for tag in ("BASE-VARIANT-REF", "PROTOCOL-REF", "FUNCTIONAL-GROUP-REF", "ECU-SHARED-DATA-REF"):
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
        seen: Set[str] = set()
        uniq: List[str] = []
        for lid in links:
            if lid not in seen:
                uniq.append(lid)
                seen.add(lid)
        return uniq

    # MAIN CONTAINER PARSER
    def parse_container(self, root: ET.Element) -> OdxContainer:
        container_el = self._ensure_container(root)
        cont = OdxContainer()
        protos = findall_descendants(container_el, "PROTOCOL")
        fgroups = findall_descendants(container_el, "FUNCTIONAL-GROUP")
        bvars = findall_descendants(container_el, "BASE-VARIANT")
        evars = findall_descendants(container_el, "ECU-VARIANT")
        shared = findall_descendants(container_el, "ECU-SHARED-DATA")
        logger.info("[ODXParser] Found layers: PROTOCOL=%d, FUNCTIONAL-GROUP=%d, BASE-VARIANT=%d, ECU-VARIANT=%d, ECU-SHARED-DATA=%d", len(protos), len(fgroups), len(bvars), len(evars), len(shared))
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

    # Public ODX file parse entrypoint
    def parse_odx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        root = self.parse_xml_bytes(content)
        return filename, self.parse_container(root)

    def parse_odx_file(self, filename: str, content: str) -> Tuple[str, OdxContainer]:
        return self.parse_odx_bytes(filename, content.encode("utf-8", errors="ignore"))

    # UNIT / COMPU / DTC
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

    def _parse_compu_method(self, compu_el: ET.Element) -> OdxCompuMethod:

        # Parse COMPU-METHOD with richer category handling (TEXTTABLE, LINEAR/IDENTICAL, BITMASK, RATIONAL)
        internal_to_phys = find_child(compu_el, "COMPU-INTERNAL-TO-PHYS")
        scales: List[OdxCompuScale] = []
        category = get_text_local(compu_el, "CATEGORY").upper() if get_text_local(compu_el, "CATEGORY") else ""

        if internal_to_phys is not None:
            for scale in get_elements(internal_to_phys, "COMPU-SCALE"):
                lower = get_text_local(scale, "LOWER-LIMIT")
                upper = get_text_local(scale, "UPPER-LIMIT")

                # Common sub-nodes that appear in multiple categories
                compuConst      = find_child(scale, "COMPU-CONST")
                compuRational   = find_child(scale, "COMPU-RATIONAL-COEFFS")
                textValueNode   = find_child(scale, "TEXT-VALUE")  # some dialects
                bitMaskNode     = find_child(scale, "BIT-MASK")     # BITMASK category

                # Gather rational coefficients (NUM/DEN) if present
                nums = [(n.text or "") for n in get_elements(compuRational, "NUM")] if compuRational is not None else []
                dens = [(d.text or "") for d in get_elements(compuRational, "DEN")] if compuRational is not None else []

                # TEXTTABLE: often expressed via COMPU-CONST with VT (text) and V (coded)
                # LINEAR / IDENTICAL: rational with simple A/B terms; general RATIONAL also maps here
                # BITMASK: BIT-MASK element indicates the active mask
                vt = get_text_local(compuConst, "VT") if compuConst is not None else (get_text_local(scale, "VT") if scale is not None else "")
                v  = get_text_local(compuConst, "V")  if compuConst is not None else (get_text_local(scale, "V")  if scale is not None else "")

                # For dialects using TEXT-VALUE as the textual representation
                if not vt and textValueNode is not None:
                    vt = (textValueNode.text or "").strip()

                # For BITMASK, capture mask bits into numerators list as a single value
                if bitMaskNode is not None:
                    mask_text = (bitMaskNode.text or "").strip()
                    if mask_text:
                        # Store mask in numerators for downstream consumers; you may adapt your model later
                        nums = [mask_text]

                scales.append(
                    OdxCompuScale(
                        lowerLimit=lower,
                        upperLimit=upper,
                        compuConstV=v,
                        compuConstVT=vt,
                        numerators=nums,
                        denominators=dens,
                    )
                )

        # Table rows (TEXTTABLE rows sometimes appear as TABLE-ROW under COMPU-METHOD)
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

    def flatten_layer_params(self, layer: OdxLayer) -> List[OdxParam]:
        out: List[OdxParam] = []
        for svc in layer.services:
            out.extend(self.flatten_service_params(svc))
        return out

    def _parse_not_inherited(self, layer_el: ET.Element) -> Tuple[Set[str], Set[str]]:
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

    def _dedup_children(self, params: List[OdxParam]) -> List[OdxParam]:
        seen = set()
        out: List[OdxParam] = []

        for p in params:
            key = (
                p.shortName,
                p.semantic,
                p.dopRefId or '',
                p.parentType,
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(p)

        return out


    def _dedup_services(self, services: List[OdxService]) -> List[OdxService]:
        seen: Set[str] = set()
        result: List[OdxService] = []
        for svc in services:
            key = svc.id or svc.shortName
            if not key:
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
            s = layer.attrs.get("NI_DIAGCOMM_SN", '')
            if s:
                sn = {x for x in s.split("\n") if x}
            i = layer.attrs.get("NI_DIAGCOMM_ID", '')
            if i:
                id = {x for x in i.split("\n") if x}
        return sn, id
# '''
#     def _resolve_links_for_layer(self, layer: OdxLayer, id_map: Dict[str, OdxLayer], visited: Set[str]) -> None:
#         if not layer.linkedLayerIds:
#             return
#         if layer.id in visited:
#             return
#         visited.add(layer.id)
#         ni_sn, ni_ids = self._get_not_inherited_sets(layer)
#         for ref_id in layer.linkedLayerIds:
#             ref_layer = id_map.get(ref_id)
#             if not ref_layer:
#                 continue
#             self._resolve_links_for_layer(ref_layer, id_map, visited)
#             for ref_id in ref_layer.linkedLayerIds:
#                 ref = id_map.get(ref_id)
#                 if not ref:
#                     continue
#                 self._resolve_links_for_layer(ref_layer, id_map, visited)
#             if ni_sn or ni_ids:
#                 filtered_services = []
#                 for svc in ref_layer.services:
#                     if (svc.shortName and svc.shortName in ni_sn) or (svc.id and svc.id in ni_ids):
#                         continue
#                     filtered_services.append(svc)
#                 layer.services.extend(filtered_services)
#             else:
#                 layer.services.extend(ref_layer.services)
#             layer.units.extend(ref_layer.units)
#             layer.compuMethods.extend(ref_layer.compuMethods)
#             layer.dataObjectProps.extend(ref_layer.dataObjectProps)
#             layer.dtcs.extend(ref_layer.dtcs)
#         layer.services = self._dedup_services(layer.services)
# '''
    # ---------------------------- PARSE PARAM ----------------------------
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

        attrs = get_all_attrs(param_el)
        shortName = get_text_local(param_el, 'SHORT-NAME')
        semantic  = (
            attrs.get('SEMANTIC')
            or attrs.get('semantic')
            or get_text_local(param_el, 'SEMANTIC')
            or ''
        )
        codedConst = find_child(param_el, 'CODED-CONST')
        physConst  = find_child(param_el, 'PHYS-CONST')
        diagCodedType = find_child(param_el, 'DIAG-CODED-TYPE')
        physType   = find_child(param_el, 'PHYSICAL-TYPE')

        shortName = get_text_local(param_el, 'SHORT-NAME')


        coded_value = extract_coded_value(codedConst) if codedConst is not None else ''
        if not coded_value:
            coded_value = extract_coded_value(param_el)

        # pid = f"{layerName}::{serviceShortName}::{parentType}::{shortName}::{uuid.uuid4().hex[:8]}"
        pid = self._make_param_id(
                layerName,
                serviceShortName,
                parentType,
                parentPath,
                shortName,
                semantic,
            )

        p = OdxParam(
            id=pid,
            shortName=shortName,
            longName=get_text_local(param_el, "LONG-NAME"),
            description=get_text_local(param_el, "DESC"),
            semantic=semantic,
            bytePosition=get_text_local(param_el, "BYTE-POSITION"),
            bitPosition=get_text_local(param_el, "BIT-POSITION"),
            bitLength=get_text_local(diagCodedType, "BIT-LENGTH") if diagCodedType else '',
            minLength=get_text_local(diagCodedType, "MIN-LENGTH") if diagCodedType else '',
            maxLength=get_text_local(diagCodedType, "MAX-LENGTH") if diagCodedType else '',
            baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE") if diagCodedType else '',
            physicalBaseType=get_attr(physType, "BASE-DATA-TYPE") if physType else '',
            codedConstValue=coded_value,
            physConstValue=get_text_local(physConst, 'V') if physConst else '',
            dopRefId=get_attr(find_child(param_el, 'DOP-REF'), 'ID-REF') or '',
            dopSnRefName=get_text_local(find_child(param_el, 'DOP-SNREF'), 'SHORT-NAME') or '',
            parentType=parentType,
            parentName=parentPath,
            layerName=layerName,
            serviceShortName=serviceShortName,
            attrs=attrs,
        )

        # ---------------------------------------------------------
        # Resolve DOP + inherit missing attributes
        # ---------------------------------------------------------
        dop = None
        if p.dopRefId:
            dop = dop_by_id.get(p.dopRefId)
        if dop is None and p.dopSnRefName:
            dop = dop_by_sn.get(p.dopSnRefName)
        self._fill_from_dop_if_missing(p, dop, dop_meta_by_id)

        next_path = f"{parentPath}.{shortName}" if parentPath else shortName



        # =========================================================
        # 1) INLINE STRUCTURE under PARAM  (highest priority)
        # =========================================================
        inline_struct = find_child(param_el, 'STRUCTURE')
        if inline_struct is not None:
            params_block = find_child(inline_struct, 'PARAMS')
            struct_params = find_children(params_block, 'PARAM') if params_block is not None else find_children(inline_struct, 'PARAM')
            for child_el in struct_params:
                child = self._try_parse_param(
                    child_el, 'STRUCTURE', next_path, layerName, serviceShortName,
                    dop_by_id, dop_by_sn, dop_meta_by_id, struct_by_id, struct_by_sn, table_by_id
                )
                if child:
                    p.children.append(child)

        # =========================================================
        # 2) TABLE-KEY expansion
        # =========================================================
        table_ref = find_child(param_el, 'TABLE-REF')
        if table_ref is not None:
            tbl_id = get_attr(table_ref, 'ID-REF')
            tbl = table_by_id.get(tbl_id)
            if tbl:
                for row in tbl.get("rows", []):
                    row_short = row.get('shortName','') or f"ROW_{row.get('key','')}" 
                    row_param = OdxParam(
                        id=f"{pid}::{row_short}",
                        shortName=row_short,
                        semantic='TABLE-ROW',
                        parentType='TABLE-KEY',
                        parentName=next_path,
                        layerName=layerName,
                        serviceShortName=serviceShortName,
                        attrs={
                            'TABLE-SHORT-NAME': tbl.get('shortName', ''),
                            "TABLE-ROW-KEY": row.get('key', ''),
                        },
                    )

                    # 2️⃣ ADD discriminator PARAM (KEY-DOP)
                    key_dop_id = tbl.get('keyDopRefId')
                    if key_dop_id and key_dop_id in dop_by_id:
                        dop = dop_by_id[key_dop_id]

                        key_param = OdxParam(
                            id=f"{row_param.id}::KEY",
                            shortName="KEY",
                            semantic="TABLE-KEY",
                            parentType="TABLE-KEY",
                            parentName=row_param.parentName,
                            layerName=layerName,
                            serviceShortName=serviceShortName,
                            dopRefId=dop.id,
                            baseDataType=dop.baseDataType,
                            physicalBaseType=dop.physicalBaseDataType,
                            bitLength=dop.bitLength,
                            attrs={"TABLE-KEY": True},
                        )

                        row_param.children.insert(0, key_param)  # discriminator first

                    row_path = f"{next_path}.{row_short}"
                    for child_el in row.get('structParams', []) or []:
                        child = self._try_parse_param(
                            child_el,
                            'STRUCTURE',
                            row_path,
                            layerName,
                            serviceShortName,
                            dop_by_id,
                            dop_by_sn,
                            dop_meta_by_id,
                            struct_by_id,
                            struct_by_sn,
                            table_by_id,
                        )
                        if child:
                            row_param.children.append(child)
                    p.children.append(row_param)

        
        # =========================================================
        # 3) DOP-owned STRUCTURE (CRITICAL for responses)
        # =========================================================
        if dop and getattr(dop, 'structureParams', None):
            for child_el in dop.structureParams:
                child = self._try_parse_param(
                    child_el,
                    'STRUCTURE',
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
                if child:
                    p.children.append(child)

        
        # =========================================================
        # 4) STRUCTURE via STRUCTURE-REF or DOP-REF
        # =========================================================
        struct_params = []

        if p.dopRefId and p.dopRefId in struct_by_id:
            struct_params = struct_by_id[p.dopRefId]
        elif p.dopSnRefName and p.dopSnRefName in struct_by_sn:
            struct_params = struct_by_sn[p.dopSnRefName]
        else:
            #5 Direct Structure-Ref
            struct_ref = find_child(param_el, 'STRUCTURE-REF')
            if struct_ref is not None:
                ref_id = get_attr(struct_ref, 'ID-REF')
                ref_sn = get_text_local(struct_ref, 'SHORT-NAME')
                struct_params = (
                    struct_by_id.get(ref_id)
                    or struct_by_sn.get(ref_sn)
                    or []
                )

        for child_el in struct_params:
            child = self._try_parse_param(
                child_el,
                'STRUCTURE',
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
            if child:
                p.children.append(child)

        # Dedup ONCE at the end
        if p.children:
            p.children = self._dedup_children(p.children)

        return p


    # # ---------------------------- MERGE CONTAINERS ----------------------------
    # def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
    #     db = OdxDatabase()
    #     for c in containers:
    #         db.ecuVariants.extend(c.ecuVariants)
    #         db.baseVariants.extend(c.baseVariants)
    #         db.protocols.extend(c.protocols)
    #         db.functionalGroups.extend(c.functionalGroups)
    #         db.ecuSharedData.extend(c.ecuSharedData)
    #     all_layers = (
    #         db.ecuVariants + db.baseVariants + db.protocols + db.functionalGroups + db.ecuSharedData
    #     )
    #     id_map: Dict[str, OdxLayer] = {lay.id: lay for lay in all_layers if lay.id}
    #     cache 
    #     for _ in range(2):
    #         for lay in all_layers:
    #             self._resolve_links_for_layer(lay, id_map, set())
    #     for layer in all_layers:
    #         for p in self.flatten_layer_params(layer):
    #             p.layerName = layer.shortName
    #             db.allParams.append(p)
    #         for u in layer.units:
    #             dd = asdict(u); dd['layerName'] = layer.shortName
    #             db.allUnits.append(dd)
    #         for cm in layer.compuMethods:
    #             dd = asdict(cm); dd['layerName'] = layer.shortName
    #             db.allCompuMethods.append(dd)
    #         for dop in layer.dataObjectProps:
    #             dd = asdict(dop); dd['layerName'] = layer.shortName
    #             dd.pop('structureParams', None)
    #             db.allDataObjects.append(dd)
    #         for dtc in layer.dtcs:
    #             dd = asdict(dtc); dd['layerName'] = layer.shortName
    #             db.allDTCs.append(dd)
    #     self._populate_presentation_fields(db)
    #     return db

    # def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
    #     db = OdxDatabase()
    #     for c in containers:
    #         db.ecuVariants.extend(c.ecuVariants)
    #         db.baseVariants.extend(c.baseVariants)
    #         db.protocols.extend(c.protocols)
    #         db.functionalGroups.extend(c.functionalGroups)
    #         db.ecuSharedData.extend(c.ecuSharedData)

    #     all_layers = (
    #         db.ecuVariants + db.baseVariants +
    #         db.protocols + db.functionalGroups +
    #         db.ecuSharedData
    #     )

    #     id_map = {l.id: l for l in all_layers if l.id}
    #     cache: Dict[str, Set[str]] = {}

    #     for _ in range(2):
    #         for l in all_layers:
    #             self._resolve_links_for_layer(l, id_map, cache)

    #     for layer in all_layers:
    #         for svc in layer.services:
    #             for p in svc.request.params if svc.request else []:
    #                 db.allParams.append(p)

    #     return db

    
    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
        db = OdxDatabase()

        # 1) Flatten layers from all containers
        for c in containers:
            db.ecuVariants.extend(c.ecuVariants)
            db.baseVariants.extend(c.baseVariants)
            db.protocols.extend(c.protocols)
            db.functionalGroups.extend(c.functionalGroups)
            db.ecuSharedData.extend(c.ecuSharedData)

        all_layers = (
            db.ecuVariants +
            db.baseVariants +
            db.protocols +
            db.functionalGroups +
            db.ecuSharedData
        )

        # 2) Build id map and resolve links using cache (bounded BFS)
        id_map: Dict[str, OdxLayer] = {lay.id: lay for lay in all_layers if lay.id}
        cache: Dict[str, Set[str]] = {}
        for _ in range(2):  # small fixed passes
            for lay in all_layers:
                self._resolve_links_for_layer(lay, id_map, cache)

        # 3) Aggregate params and other artifacts into db (with layerName)
        for layer in all_layers:
            # Params (request + pos + neg)
            for svc in getattr(layer, 'services', []) or []:
                if svc.request:
                    for p in svc.request.params or []:
                        p.layerName = layer.shortName
                        db.allParams.append(p)
                for rsp in svc.posResponses or []:
                    for p in rsp.params or []:
                        p.layerName = layer.shortName
                        db.allParams.append(p)
                for rsp in svc.negResponses or []:
                    for p in rsp.params or []:
                        p.layerName = layer.shortName
                        db.allParams.append(p)

            # Units
            for u in layer.units:
                dd = asdict(u); dd['layerName'] = layer.shortName
                db.allUnits.append(dd)

            # Compu Methods
            for cm in layer.compuMethods:
                dd = asdict(cm); dd['layerName'] = layer.shortName
                db.allCompuMethods.append(dd)

            # Data Object Props
            for dop in layer.dataObjectProps:
                dd = asdict(dop); dd['layerName'] = layer.shortName
                dd.pop('structureParams', None)  # avoid large nested lists
                db.allDataObjects.append(dd)

            # DTCs
            for dtc in layer.dtcs:
                dd = asdict(dtc); dd['layerName'] = layer.shortName
                db.allDTCs.append(dd)

        # 4) Presentation enrichment (SID / DID / infoText / display values)
        self._populate_presentation_fields(db)

        return db

    
    # ---------------------------- PRESENTATION ENRICHMENT ----------------------------
    def _populate_presentation_fields(self, db: OdxDatabase) -> None:
        layers = (getattr(db, 'ecuVariants', []) or []) \
               + (getattr(db, 'baseVariants', []) or []) \
               + (getattr(db, 'protocols', []) or []) \
               + (getattr(db, 'functionalGroups', []) or []) \
               + (getattr(db, 'ecuSharedData', []) or [])
        for layer in layers:
            for svc in getattr(layer, 'services', []) or []:
                raw_sid = svc.attrs.get('SID', '') or svc.attrs.get('sid', '')
                sid_val = self._fmt.parse_sid(raw_sid)
                if sid_val is None:
                    sid_val = self._derive_sid_from_request(svc)
                svc.sid = sid_val
                did_raw = self._extract_did_from_request(svc)
                svc.didNormalized = self._fmt.normalize_did(did_raw) or ''
                for msg in [svc.request] + list(svc.posResponses or []) + list(svc.negResponses or []):
                    if not msg:
                        continue
                    for p in msg.params or []:
                        raw_const = p.physConstValue or p.codedConstValue
                        p.displayValue = self._fmt.format_param_display(raw_const) or ''
                        p.isSelectableFor22 = self._fmt.is_param_selectable_for_22(svc.sid, p.parentType, p.semantic)
                total_params = self._count_params_in_service(svc)
                bits = ["Params: %d" % total_params]
                if svc.didNormalized:
                    bits.append("DID: %s" % svc.didNormalized)
                svc.infoText = " • ".join(bits)

    def _count_params_in_service(self, svc: OdxService) -> int:
        n = 0
        if svc.request:
            n += len(svc.request.params or [])
        for r in (svc.posResponses or []):
            n += len(r.params or [])
        for r in (svc.negResponses or []):
            n += len(r.params or [])
        return n

    def _derive_sid_from_request(self, svc: OdxService) -> Optional[int]:
        rq = getattr(svc, 'request', None)
        if not rq:
            return None
        sid_keys = {
            'SERVICE-ID', 'SERVICE-ID-RQ', 'SERVICE-ID-RSP', 'SERVICE-ID-RES',
            'POS-SERVICE-ID', 'NEG-SERVICE-ID'
        }
        for p in rq.params or []:
            sem = (p.semantic or '').upper()
            sn  = (p.shortName or '').upper()
            if (sem in sid_keys) or ('SERVICE-ID' in sem) or (sn == 'SID') or ('SID' in sn):
                raw = p.physConstValue or p.codedConstValue
                return self._fmt.parse_sid(raw)
        return None

    def _extract_did_from_request(self, svc: OdxService) -> Optional[str | int]:
        rq = getattr(svc, 'request', None)
        if not rq:
            return None
        did_keys = {'DATA-ID', 'DATA-IDENTIFIER', 'RECORD-DATA-IDENTIFIER', 'IDENTIFIER'}
        for p in rq.params or []:
            sem = (p.semantic or '').upper()
            sn  = (p.shortName or '').upper()
            if (sem in did_keys) or ('IDENTIFIER' in sem) or ('DID' in sn):
                return p.physConstValue or p.codedConstValue
        return None
    
    def _extend_unique(self, target: List, source: List, key_fn: Callable) -> None:
        seen = {key_fn(x) for x in target if key_fn(x)}
        for x in source:
            k = key_fn(x)
            if k and k not in seen:
                target.append(x)
                seen.add(k)
    
    def _extend_unique_services(self, layer: OdxLayer, services: List[OdxService]) -> None:
        self._extend_unique(layer.services, services,
                            lambda s: s.id or s.shortName)

    def _resolve_links_for_layer(
        self,
        layer: OdxLayer,
        id_map: Dict[str, OdxLayer],
        cache: Dict[str, Set[str]],
    ) -> None:
        if not layer.linkedLayerIds:
            return

        lid = layer.id
        if lid not in cache:
            visited = set()
            stack = list(layer.linkedLayerIds)
            while stack:
                rid = stack.pop()
                if rid in visited:
                    continue
                visited.add(rid)
                ref = id_map.get(rid)
                if ref:
                    stack.extend(ref.linkedLayerIds or [])
            cache[lid] = visited

        for rid in cache[lid]:
            ref = id_map.get(rid)
            if not ref:
                continue

            ni_sn, ni_ids = self._get_not_inherited_sets(layer)

            src_services = [
                svc for svc in ref.services
                if not (
                    (svc.shortName and svc.shortName in ni_sn) or
                    (svc.id and svc.id in ni_ids)
                )
            ]       

            self._extend_unique_services(layer, src_services)
            self._extend_unique(layer.units, ref.units, lambda x: x.id or x.shortName)
            self._extend_unique(layer.compuMethods, ref.compuMethods, lambda x: x.id or x.shortName)
            self._extend_unique(layer.dataObjectProps, ref.dataObjectProps, lambda x: x.id or x.shortName)
            self._extend_unique(layer.dtcs, ref.dtcs, lambda x: x.id or x.troubleCode)



# # ===========================
# # Patch: bounded link-resolution + dedup (prevents MemoryError)
# # ===========================

# from dataclasses import asdict
# from typing import Dict, Set, List, Callable
# import logging

# logger = logging.getLogger(__name__)

# # Unique extenders
# def _extend_unique(self, target_list: List, source_list: List, key_fn: Callable) -> None:
#     ''"Append only items with new keys; keeps order and prevents duplicates.''"
#     seen = set()
#     for x in target_list:
#         try:
#             k = key_fn(x)
#         except Exception:
#             k = None
#         if k:
#             seen.add(k)

#     for x in (source_list or []):
#         try:
#             k = key_fn(x)
#         except Exception:
#             k = None
#         if k and k not in seen:
#             target_list.append(x)
#             seen.add(k)

# def _extend_unique_services(self, layer, new_services):
#     def svc_key(s):
#         return (getattr(s, 'id', None) or getattr(s, 'shortName', None) or '').strip()
#     self._extend_unique(layer.services, new_services or [], svc_key)


# def _resolve_links_for_layer(self, layer: OdxLayer, id_map: Dict[str, OdxLayer], reachable_cache: Dict[str, Set[str]]) -> None:
#     if not layer or not getattr(layer, 'linkedLayerIds', None):
#         return

#     lid = getattr(layer, 'id', None) or getattr(layer, 'shortName', None) or ''
#     if not lid:
#         return

#     reachable = reachable_cache.get(lid)
#     if reachable is None:
#         reachable = set()
#         queue = list(layer.linkedLayerIds or [])
#         while queue:
#             rid = queue.pop()
#             if not rid or rid in reachable:
#                 continue
#             reachable.add(rid)
#             ref = id_map.get(rid)
#             if ref and ref.linkedLayerIds:
#                 queue.extend(ref.linkedLayerIds)
#         reachable_cache[lid] = reachable

#     ni_sn, ni_ids = self._get_not_inherited_sets(layer)
#     for rid in reachable:
#         ref_layer = id_map.get(rid)
#         if not ref_layer:
#             continue

#         src_services = [
#             svc for svc in ref_layer.services or []
#             if not (getattr(svc, 'shortName', None) and getattr(svc, 'shortName') in ni_sn) or
#                (getattr(svc, 'id', None) and getattr(svc, 'id') in ni_ids)
#         ]

#         self._extend_unique_services(layer, src_services)
#         self._extend_unique(layer.units, ref_layer.units, lambda x: (getattr(x, 'id', None) or getattr(x, 'shortName', None) or '').strip())
#         self._extend_unique(layer.compuMethods, ref_layer.compuMethods, lambda x: (getattr(x, 'id', None) or getattr(x, 'shortName', None) or '').strip())
#         self._extend_unique(layer.dataObjectProps, ref_layer.dataObjectProps, lambda x: (getattr(x, 'id', None) or getattr(x, 'shortName', None) or '').strip())
#         self._extend_unique(layer.dtcs, ref_layer.dtcs, lambda x: (getattr(x, 'id', None) or getattr(x, 'troubleCode', None) or '').strip())

# def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
#         db = OdxDatabase()
#         for c in containers:
#             db.ecuVariants.extend(c.ecuVariants)
#             db.baseVariants.extend(c.baseVariants)
#             db.protocols.extend(c.protocols)
#             db.functionalGroups.extend(c.functionalGroups)
#             db.ecuSharedData.extend(c.ecuSharedData)
#         all_layers = (
#             db.ecuVariants + db.baseVariants + db.protocols + db.functionalGroups + db.ecuSharedData
#         )
#         id_map: Dict[str, OdxLayer] = {lay.id: lay for lay in all_layers if lay.id}
#         for _ in range(2):
#             for lay in all_layers:
#                 self._resolve_links_for_layer(lay, id_map, set())
#         for layer in all_layers:
#             for p in self.flatten_layer_params(layer):
#                 p.layerName = layer.shortName
#                 db.allParams.append(p)
#             for u in layer.units:
#                 dd = asdict(u); dd['layerName'] = layer.shortName
#                 db.allUnits.append(dd)
#             for cm in layer.compuMethods:
#                 dd = asdict(cm); dd['layerName'] = layer.shortName
#                 db.allCompuMethods.append(dd)
#             for dop in layer.dataObjectProps:
#                 dd = asdict(dop); dd['layerName'] = layer.shortName
#                 dd.pop('structureParams', None)
#                 db.allDataObjects.append(dd)
#             for dtc in layer.dtcs:
#                 dd = asdict(dtc); dd['layerName'] = layer.shortName
#                 db.allDTCs.append(dd)
#         self._populate_presentation_fields(db)
#         return db

# # Apply overrides to class defined above
# ODXParser._extend_unique = _extend_unique
# ODXParser.merge_containers = merge_containers

#ODXParser._log_and_prefix = _log_and_prefix
