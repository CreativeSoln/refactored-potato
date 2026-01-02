from __future__ import annotations

import uuid
import logging
import hashlib
import xml.etree.ElementTree as ET
from dataclasses import asdict
from typing import List, Dict, Tuple, Optional, Set

# ----------------------------------------------------------------------------
# Logging & deterministic ID helper
# ----------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Deterministic short hash for building stable IDs
def _stable_id(*parts: str) -> str:
    h = hashlib.sha1(('|'.join(parts)).encode('utf-8')).hexdigest()[:12]
    return h



# =============================================================================
# Models
# =============================================================================
# These are assumed to be pure dataclasses with no logic.
# They represent the parsed ODX domain objects.

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
# XML Helper Utilities
# =============================================================================

def local_name(tag: str) -> str:
    """
    Return XML tag name without namespace.

    Example:
        '{http://x}PARAM' -> 'PARAM'
    """
    return tag.split("}", 1)[1] if "}" in tag else tag


def get_all_attrs(el: Optional[ET.Element]) -> Dict[str, str]:
    """Return all attributes of an element safely."""
    return {} if el is None else dict(el.attrib)


def get_attr(el: Optional[ET.Element], name: str, default: str = "") -> str:
    """Safely get attribute value with default."""
    if el is None:
        return default
    return el.attrib.get(name, default)


def get_text_local(el: Optional[ET.Element], name: str) -> str:
    """
    Return text content of the first direct child with given local tag name.
    """
    if el is None:
        return ""
    for c in el:
        if local_name(c.tag) == name:
            return "".join(c.itertext()).strip()
    return ""


def get_elements(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    """Return all direct children with given tag name."""
    if el is None:
        return []
    return [c for c in el if local_name(c.tag) == name]


def find_child(el: Optional[ET.Element], name: str) -> Optional[ET.Element]:
    """Return first direct child with given tag name."""
    if el is None:
        return None
    for c in el:
        if local_name(c.tag) == name:
            return c
    return None


def find_children(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    """Return all direct children with given tag name."""
    if el is None:
        return []
    return [c for c in el if local_name(c.tag) == name]


def findall_descendants(el: Optional[ET.Element], name: str) -> List[ET.Element]:
    """
    Return all descendant nodes (recursive) with given tag name.
    """
    if el is None:
        return []
    return [n for n in el.iter() if local_name(n.tag) == name]


def get_attr_ci(el: Optional[ET.Element], *names: str) -> str:
    """
    Case-insensitive attribute lookup.
    Returns first matching attribute value.
    """
    if el is None or not el.attrib:
        return ""

    low = {k.lower(): v for k, v in el.attrib.items()}
    for n in names:
        v = low.get(n.lower())
        if v:
            return v
    return ""



# =============================================================================
# STRUCTURE Harvesting
# =============================================================================

def harvest_structures(
    layer_el: ET.Element
) -> Tuple[Dict[str, List[ET.Element]], Dict[str, List[ET.Element]]]:
    """
    Collect STRUCTURE definitions within a diagnostic layer.

    Returns:
        by_id : STRUCTURE-ID -> list of PARAM elements
        by_sn : STRUCTURE-SHORT-NAME -> list of PARAM elements
    """
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
        params = (
            find_children(params_block, "PARAM")
            if params_block is not None
            else findall_descendants(st, "PARAM")
        )

        if sid:
            by_id[sid] = params
        if ssn:
            by_sn[ssn] = params

    return by_id, by_sn


# =============================================================================
# ODX Parser
# =============================================================================

class ODXParser:
    """
    Main ODX parser.

    This class converts raw ODX XML into structured Python models:
    - Layers
    - Services
    - Messages
    - Parameters
    - DOPs, Units, Compu Methods, DTCs
    """

    # -------------------------------------------------------------------------
    # XML entry points
    # -------------------------------------------------------------------------

    def parse_xml_bytes(self, content: bytes) -> ET.Element:
        """
        Parse raw bytes into XML element.
        Strips BOM / garbage before first '<'.
        """
        return ET.fromstring(content[content.find(b"<"):])

    def parse_xml(self, content: str) -> ET.Element:
        """Parse XML from string."""
        return self.parse_xml_bytes(content.encode("utf-8", errors="ignore"))

    # -------------------------------------------------------------------------
    # Public ODX parse APIs
    # -------------------------------------------------------------------------

    def parse_odx_bytes(
        self,
        filename: str,
        content: bytes
    ) -> Tuple[str, OdxContainer]:
        """
        Parse an ODX file from bytes.
        """
        root = self.parse_xml_bytes(content)
        return filename, self.parse_container(root)

    def parse_odx_file(
        self,
        filename: str,
        content: str
    ) -> Tuple[str, OdxContainer]:
        """
        Parse an ODX file from string content.
        """
        return self.parse_odx_bytes(
            filename,
            content.encode("utf-8", errors="ignore")
        )

    # -------------------------------------------------------------------------
    # Safe PARAM wrapper
    # -------------------------------------------------------------------------

    def _try_parse_param(self, *args, **kwargs) -> Optional[OdxParam]:
        """
        Wrapper around parse_param().
        Ensures a broken PARAM does not abort parsing.
        """
        try:
            return self.parse_param(*args, **kwargs)
        except Exception as ex:
            logger.warning("Skipping PARAM: %s", ex, exc_info=True)
            return None

    # -------------------------------------------------------------------------
    # DOP metadata inheritance
    # -------------------------------------------------------------------------

    def _fill_from_dop_if_missing(
        self,
        p: OdxParam,
        dop: Optional[OdxDataObjectProp],
        dop_meta_by_id: Dict[str, Dict[str, str]],
    ) -> None:
        """
        Inherit missing properties from referenced DOP.
        """
        if not dop:
            return

        meta = dop_meta_by_id.get(dop.id, {})

        if not p.baseDataType:
            p.baseDataType = dop.baseDataType
        if not p.physicalBaseType:
            p.physicalBaseType = dop.physicalBaseDataType
        if not p.bitLength:
            p.bitLength = dop.bitLength
        if not p.minLength:
            p.minLength = meta.get("minLength", "")
        if not p.maxLength:
            p.maxLength = meta.get("maxLength", "")

    # -------------------------------------------------------------------------
    # PARAM parser (core logic)
    # -------------------------------------------------------------------------

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
        """
        Parse a single <PARAM> element into OdxParam.

        Responsibilities:
        - Extract scalar metadata
        - Resolve DOP references
        - Expand STRUCTURE parameters
        - Expand TABLE-KEY parameters
        - Build recursive parameter hierarchy
        """

        attrs = get_all_attrs(param_el)

        codedConst = find_child(param_el, "CODED-CONST")
        physConst = find_child(param_el, "PHYS-CONST")
        dopRef = find_child(param_el, "DOP-REF")
        dopSnRef = find_child(param_el, "DOP-SNREF")
        compuRef = find_child(param_el, "COMPU-METHOD-REF")
        diagCodedType = find_child(param_el, "DIAG-CODED-TYPE")
        physType = find_child(param_el, "PHYSICAL-TYPE")

        shortName = get_text_local(param_el, "SHORT-NAME")
        semantic = attrs.get("SEMANTIC") or get_text_local(param_el, "SEMANTIC") or ""

        pid = f"{layerName}::{serviceShortName}::{parentType}::{shortName}::{_stable_id(parentPath or "", shortName or "")}"

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
            codedConstValue=extract_coded_value(codedConst) or extract_coded_value(param_el),
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

        # ---------------- Resolve DOP ----------------
        dop = (
            dop_by_id.get(p.dopRefId)
            if p.dopRefId
            else dop_by_sn.get(p.dopSnRefName)
        )

        self._fill_from_dop_if_missing(p, dop, dop_meta_by_id)

        next_path = f"{parentPath}.{shortName}" if parentPath else shortName

        # ---------------- STRUCTURE expansion ----------------
        struct_params: List[ET.Element] = []

        if dop and dop.structureParams:
            struct_params = dop.structureParams
        elif p.dopRefId in struct_by_id:
            struct_params = struct_by_id[p.dopRefId]
        elif p.dopSnRefName in struct_by_sn:
            struct_params = struct_by_sn[p.dopSnRefName]
        else:
            sref = find_child(param_el, "STRUCTURE-REF")
            if sref:
                struct_params = (
                    struct_by_id.get(get_attr(sref, "ID-REF"))
                    or struct_by_sn.get(get_text_local(sref, "SHORT-NAME"))
                    or []
                )

        for ch in struct_params:
            child = self._try_parse_param(
                ch,
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
            if child:
                p.children.append(child)

        # ---------------- TABLE-KEY expansion ----------------
        table_ref = find_child(param_el, "TABLE-REF")
        if table_ref:
            tbl = table_by_id.get(get_attr(table_ref, "ID-REF"))
            if tbl:
                for row in tbl.get("rows", []):
                    row_short = f"{row.get('shortName','Row')}_{uuid.uuid4().hex[:9]}"
                    row_param = OdxParam(
                        id=f"{pid}::{row_short}",
                        shortName=row_short,
                        longName=row.get("shortName", ""),
                        semantic="TABLE-ROW",
                        parentType="TABLE-KEY",
                        parentName=next_path,
                        layerName=layerName,
                        serviceShortName=serviceShortName,
                        attrs={
                            "TABLE-SHORT-NAME": tbl.get("shortName", ""),
                            "TABLE-ROW-KEY": row.get("key", ""),
                        },
                    )

                    for ch in row.get("structParams", []):
                        child = self._try_parse_param(
                            ch,
                            "STRUCTURE",
                            f"{next_path}.{row_short}",
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

        return p
    
        # -------------------------------------------------------------------------
    # Ensure DIAG-LAYER-CONTAINER root
    # -------------------------------------------------------------------------

    def _ensure_container(self, root: ET.Element) -> ET.Element:
        """
        Normalize XML root to DIAG-LAYER-CONTAINER.

        Some ODX files wrap layers directly without an explicit
        DIAG-LAYER-CONTAINER element. This method guarantees a
        consistent root for downstream parsing.
        """
        if local_name(root.tag) == "DIAG-LAYER-CONTAINER":
            return root

        dlc = find_child(root, "DIAG-LAYER-CONTAINER")
        if dlc is not None:
            return dlc

        matches = findall_descendants(root, "DIAG-LAYER-CONTAINER")
        if matches:
            return matches[0]

        return root

    # -------------------------------------------------------------------------
    # Parse a single diagnostic layer
    # -------------------------------------------------------------------------

    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:
        """
        Parse a single diagnostic layer (Protocol, Base Variant, ECU Variant, etc.).

        This method:
        - Collects STRUCTURE, DOP, TABLE definitions
        - Parses REQUEST / POS-RESPONSE / NEG-RESPONSE messages
        - Builds DIAG-SERVICE objects
        - Applies NOT-INHERITED-DIAG-COMMS metadata
        """
        layer_short = get_text_local(layer_el, "SHORT-NAME")

        # ---- STRUCTURES
        struct_by_id, struct_by_sn = harvest_structures(layer_el)

        # ---- DOPs
        dop_by_id: Dict[str, OdxDataObjectProp] = {}
        dop_by_sn: Dict[str, OdxDataObjectProp] = {}
        dop_meta_by_id: Dict[str, Dict[str, str]] = {}

        for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
            dd, meta = self._parse_dop_with_struct_map(d, struct_by_id, struct_by_sn)
            dop_by_id[dd.id] = dd
            dop_meta_by_id[dd.id] = meta
            if dd.shortName:
                dop_by_sn[dd.shortName] = dd

        # ---- TABLES (for TABLE-KEY expansion)
        table_by_id: Dict[str, Dict] = {}

        for t in findall_descendants(layer_el, "TABLE"):
            tid = get_attr(t, "ID")
            tsn = get_text_local(t, "SHORT-NAME")

            rows = []
            for tr in findall_descendants(t, "TABLE-ROW"):
                struct_ref = find_child(tr, "STRUCTURE-REF")
                struct_params: List[ET.Element] = []

                if struct_ref is not None:
                    ref_id = get_attr(struct_ref, "ID-REF")
                    ref_sn = get_text_local(struct_ref, "SHORT-NAME")
                    struct_params = (
                        struct_by_id.get(ref_id)
                        or struct_by_sn.get(ref_sn)
                        or []
                    )

                rows.append({
                    "id": get_attr(tr, "ID"),
                    "shortName": get_text_local(tr, "SHORT-NAME"),
                    "key": get_text_local(tr, "KEY"),
                    "structParams": struct_params,
                })

            if tid:
                table_by_id[tid] = {
                    "shortName": tsn,
                    "rows": rows,
                }

        # ---- Units, Compu Methods, DTCs
        units = [self._parse_unit(u) for u in findall_descendants(layer_el, "UNIT")]
        compu_methods = [self._parse_compu_method(c) for c in findall_descendants(layer_el, "COMPU-METHOD")]
        dtcs = [self._parse_dtc(d) for d in findall_descendants(layer_el, "DTC")]

        # ---- Message maps
        request_map: Dict[str, OdxMessage] = {}
        pos_resp_map: Dict[str, OdxMessage] = {}
        neg_resp_map: Dict[str, OdxMessage] = {}

        # ---- Standalone REQUEST
        for req in findall_descendants(layer_el, "REQUEST"):
            rid = get_attr(req, "ID")
            rshort = get_text_local(req, "SHORT-NAME")
            root_path = rshort or ""

            params = [
                p for p in (
                    self._try_parse_param(
                        pel, "REQUEST", root_path, layer_short, "",
                        dop_by_id, dop_by_sn, dop_meta_by_id,
                        struct_by_id, struct_by_sn, table_by_id
                    )
                    for pel in findall_descendants(req, "PARAM")
                )
                if p
            ]

            request_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(req, "LONG-NAME"),
                params=params,
            )

        # ---- POS / NEG responses (standalone)
        for tag, target in (
            ("POS-RESPONSE", pos_resp_map),
            ("NEG-RESPONSE", neg_resp_map),
        ):
            for res in findall_descendants(layer_el, tag):
                rid = get_attr(res, "ID")
                rshort = get_text_local(res, "SHORT-NAME")
                root_path = rshort or ""

                params = [
                    p for p in (
                        self._try_parse_param(
                            pel, tag.replace("-", "_"), root_path, layer_short, "",
                            dop_by_id, dop_by_sn, dop_meta_by_id,
                            struct_by_id, struct_by_sn, table_by_id
                        )
                        for pel in findall_descendants(res, "PARAM")
                    )
                    if p
                ]

                target[rid] = OdxMessage(
                    id=rid,
                    shortName=rshort,
                    longName=get_text_local(res, "LONG-NAME"),
                    params=params,
                )

        # ---- Services
        services: List[OdxService] = []

        for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
            svc_attrs = get_all_attrs(svc_el)
            svc_short = get_text_local(svc_el, "SHORT-NAME")

            req_ref = find_child(svc_el, "REQUEST-REF")
            request = request_map.get(get_attr(req_ref, "ID-REF")) if req_ref else None

            pos_responses = [
                pos_resp_map.get(get_attr(r, "ID-REF"))
                for r in find_children(svc_el, "POS-RESPONSE-REF")
                if get_attr(r, "ID-REF") in pos_resp_map
            ]

            neg_responses = [
                neg_resp_map.get(get_attr(r, "ID-REF"))
                for r in find_children(svc_el, "NEG-RESPONSE-REF")
                if get_attr(r, "ID-REF") in neg_resp_map
            ]

            services.append(
                OdxService(
                    id=svc_attrs.get("ID", ""),
                    shortName=svc_short,
                    longName=get_text_local(svc_el, "LONG-NAME"),
                    description=get_text_local(svc_el, "DESC"),
                    semantic=svc_attrs.get("SEMANTIC", ""),
                    addressing=svc_attrs.get("ADDRESSING", ""),
                    request=request,
                    posResponses=[r for r in pos_responses if r],
                    negResponses=[r for r in neg_responses if r],
                    attrs=svc_attrs,
                )
            )

        
        # ---- Apply service annotations and path prefixing
        for svc in services:
            if svc.request and (svc.request.params or []):
                self._annotate_service_name(svc.request.params, svc.shortName or "")
                self._prefix_path(svc.request.params, svc.shortName or "")
            for r in (svc.posResponses or []) + (svc.negResponses or []):
                if r and (r.params or []):
                    self._annotate_service_name(r.params, svc.shortName or "")
                    self._prefix_path(r.params, svc.shortName or "")

        # ---- NOT-INHERITED-DIAG-COMMS via XML (populate attrs for downstream filtering)
        ni_sn_xml, ni_ids_xml = parse_not_inherited_from_xml(layer_el)
        layer_attrs = get_all_attrs(layer_el)
        if ni_sn_xml and not layer_attrs.get("NI_DIAGCOMM_SN"):
            layer_attrs["NI_DIAGCOMM_SN"] = "\n".join(sorted(ni_sn_xml))
            layer_attrs["NI_DIAGCOMM_ID"] = "\n".join(sorted(ni_ids_xml))

        return OdxLayer(
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
            attrs=layer_attrs,
            linkedLayerIds=self._collect_links(layer_el),
        )

    # -------------------------------------------------------------------------
    # Collect inter-layer links
    # -------------------------------------------------------------------------

    def _collect_links(self, layer_el: ET.Element) -> List[str]:
        """
        Collect referenced layer IDs used for inheritance.
        """
        links: List[str] = []

        links_el = find_child(layer_el, "DIAG-LAYER-LINKS")
        if links_el:
            for lnk in find_children(links_el, "DIAG-LAYER-LINK"):
                for c in lnk:
                    if local_name(c.tag).endswith("-REF"):
                        rid = get_attr(c, "ID-REF")
                        if rid:
                            links.append(rid)

        # Deduplicate while preserving order
        seen = set()
        return [x for x in links if not (x in seen or seen.add(x))]

    # -------------------------------------------------------------------------
    # Parse container
    # -------------------------------------------------------------------------

    def parse_container(self, root: ET.Element) -> OdxContainer:
        """
        Parse DIAG-LAYER-CONTAINER into an OdxContainer.
        """
        container_el = self._ensure_container(root)
        cont = OdxContainer()

        for tag, dest, typ in (
            ("PROTOCOL", cont.protocols, "PROTOCOL"),
            ("FUNCTIONAL-GROUP", cont.functionalGroups, "FUNCTIONAL-GROUP"),
            ("BASE-VARIANT", cont.baseVariants, "BASE-VARIANT"),
            ("ECU-VARIANT", cont.ecuVariants, "ECU-VARIANT"),
            ("ECU-SHARED-DATA", cont.ecuSharedData, "ECU-SHARED-DATA"),
        ):
            for el in findall_descendants(container_el, tag):
                dest.append(self._parse_layer(el, typ))

        return cont

    # -------------------------------------------------------------------------
    # Merge containers into database
    # -------------------------------------------------------------------------

    def merge_containers(self, containers: List[OdxContainer]) -> OdxDatabase:
        """
        Merge multiple containers into a flattened OdxDatabase.
        """
        db = OdxDatabase()

        for c in containers:
            db.ecuVariants.extend(c.ecuVariants)
            db.baseVariants.extend(c.baseVariants)
            db.protocols.extend(c.protocols)
            db.functionalGroups.extend(c.functionalGroups)
            db.ecuSharedData.extend(c.ecuSharedData)

        for layer in (
            db.ecuVariants
            + db.baseVariants
            + db.protocols
            + db.functionalGroups
            + db.ecuSharedData
        ):
            for svc in layer.services:
                if svc.request:
                    db.allParams.extend(svc.request.params)
                for r in svc.posResponses + svc.negResponses:
                    db.allParams.extend(r.params)

            for u in layer.units:
                d = asdict(u); d["layerName"] = layer.shortName
                db.allUnits.append(d)

            for cm in layer.compuMethods:
                d = asdict(cm); d["layerName"] = layer.shortName
                db.allCompuMethods.append(d)

            for dop in layer.dataObjectProps:
                d = asdict(dop); d["layerName"] = layer.shortName
                d.pop("structureParams", None)
                db.allDataObjects.append(d)

            for dtc in layer.dtcs:
                d = asdict(dtc); d["layerName"] = layer.shortName
                db.allDTCs.append(d)

        return db
    # -------------------------------------------------------------------------

        # -------------------------------------------------------------------------
    # Annotate service name on all parameters
    # -------------------------------------------------------------------------

    def _annotate_service_name(
        self,
        params: List[OdxParam],
        svc_short: str
    ) -> None:
        """
        Set serviceShortName on all parameters recursively.

        This ensures every parameter (including STRUCTURE / TABLE children)
        knows which DIAG-SERVICE it belongs to.
        """
        if not params:
            return

        stack = list(params)

        while stack:
            p = stack.pop()
            p.serviceShortName = svc_short

            for c in getattr(p, "children", []) or []:
                stack.append(c)

    # -------------------------------------------------------------------------
    # Prefix parameter path with service context
    # -------------------------------------------------------------------------

    def _prefix_path(
        self,
        params: List[OdxParam],
        prefix: str
    ) -> None:
        """
        Prefix parentName (dot path) of parameters with a service-specific prefix.

        Used when REQUEST / RESPONSE definitions are shared across services.
        """
        if not params or not prefix:
            return

        stack = list(params)

        while stack:
            p = stack.pop()
            base = p.parentName or ""
            p.parentName = f"{prefix}.{base}" if base else prefix

            for c in getattr(p, "children", []) or []:
                stack.append(c)

    # =========================================================================
    # DOP (Data Object Property) parsing helpers
    # =========================================================================

    def _parse_dop_with_struct_map(
        self,
        dop_el: ET.Element,
        struct_by_id: Dict[str, List[ET.Element]],
        struct_by_sn: Dict[str, List[ET.Element]],
    ) -> Tuple[OdxDataObjectProp, Dict[str, str]]:
        """
        Parse a DATA-OBJECT-PROP and resolve any STRUCTURE it owns or references.

        Returns:
            - OdxDataObjectProp instance
            - metadata dictionary (minLength / maxLength)
        """

        diagCodedType = find_child(dop_el, "DIAG-CODED-TYPE")
        physType = find_child(dop_el, "PHYSICAL-TYPE")
        unitRef = find_child(dop_el, "UNIT-REF")
        compuMethod = find_child(dop_el, "COMPU-METHOD")

        structure = find_child(dop_el, "STRUCTURE")
        structure_ref = find_child(dop_el, "STRUCTURE-REF")

        struct_params: List[ET.Element] = []

        # ---- Inline STRUCTURE
        if structure is not None:
            params_block = find_child(structure, "PARAMS")
            if params_block is not None:
                struct_params = find_children(params_block, "PARAM")
            else:
                struct_params = find_children(structure, "PARAM")
                if not struct_params:
                    struct_params = findall_descendants(structure, "PARAM")

        # ---- STRUCTURE-REF
        if not struct_params and structure_ref is not None:
            ref_id = get_attr(structure_ref, "ID-REF")
            ref_sn = get_text_local(structure_ref, "SHORT-NAME")

            if ref_id and ref_id in struct_by_id:
                struct_params = struct_by_id[ref_id]
            elif ref_sn and ref_sn in struct_by_sn:
                struct_params = struct_by_sn[ref_sn]

        dop = OdxDataObjectProp(
            id=get_attr(dop_el, "ID"),
            shortName=get_text_local(dop_el, "SHORT-NAME"),
            longName=get_text_local(dop_el, "LONG-NAME"),
            description=get_text_local(dop_el, "DESC"),
            baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE") if diagCodedType else "",
            bitLength=get_text_local(diagCodedType, "BIT-LENGTH") if diagCodedType else "",
            physicalBaseDataType=get_attr(physType, "BASE-DATA-TYPE") if physType else "",
            unitRefId=get_attr(unitRef, "ID-REF") if unitRef else "",
            compuCategory=get_text_local(compuMethod, "CATEGORY") if compuMethod else "",
            structureParams=struct_params,
        )

        meta = {
            "minLength": get_text_local(diagCodedType, "MIN-LENGTH") if diagCodedType else "",
            "maxLength": get_text_local(diagCodedType, "MAX-LENGTH") if diagCodedType else "",
        }

        return dop, meta

    # =========================================================================
    # UNIT parser
    # =========================================================================

    def _parse_unit(self, unit_el: ET.Element) -> OdxUnit:
        """
        Parse UNIT definition.
        """
        return OdxUnit(
            id=get_attr(unit_el, "ID"),
            shortName=get_text_local(unit_el, "SHORT-NAME"),
            longName=get_text_local(unit_el, "LONG-NAME"),
            displayName=get_text_local(unit_el, "DISPLAY-NAME"),
            factorSiToUnit=get_text_local(unit_el, "FACTOR-SI-TO-UNIT"),
            offsetSiToUnit=get_text_local(unit_el, "OFFSET-SI-TO-UNIT"),
            physicalDimensionRef=get_attr(
                find_child(unit_el, "PHYSICAL-DIMENSION-REF"),
                "ID-REF"
            ),
        )

    # =========================================================================
    # COMPU-METHOD parser
    # =========================================================================

    def _parse_compu_method(self, compu_el: ET.Element) -> OdxCompuMethod:
        """
        Parse COMPU-METHOD including:
        - Rational coefficients
        - Constant mappings
        - TEXTTABLE rows
        """

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
                        compuConstV=get_text_local(compuConst, "V") if compuConst else "",
                        compuConstVT=get_text_local(compuConst, "VT") if compuConst else "",
                        numerators=[n.text or "" for n in get_elements(compuRational, "NUM")] if compuRational else [],
                        denominators=[d.text or "" for d in get_elements(compuRational, "DEN")] if compuRational else [],
                    )
                )

        # ---- TEXTTABLE support
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
            tableRows=table_rows,
        )

    # =========================================================================
    # DTC parser
    # =========================================================================

    def _parse_dtc(self, dtc_el: ET.Element) -> OdxDTC:
        """
        Parse Diagnostic Trouble Code (DTC).
        """
        return OdxDTC(
            id=get_attr(dtc_el, "ID"),
            shortName=get_text_local(dtc_el, "SHORT-NAME"),
            longName=get_text_local(dtc_el, "LONG-NAME"),
            description=get_text_local(dtc_el, "DESC"),
            troubleCode=get_text_local(dtc_el, "TROUBLE-CODE"),
            displayTroubleCode=get_text_local(dtc_el, "DISPLAY-TROUBLE-CODE"),
            level=get_text_local(dtc_el, "LEVEL"),
        )

def first_text(el: Optional[ET.Element], tag_names: List[str]) -> str:
    """
    Return the first non-empty text found for any of the given tag names,
    searching recursively through all descendants.

    This is required for robust ODX parsing because vendors may nest
    values differently (e.g., CODED-VALUE under multiple wrapper nodes).

    Args:
        el: Root XML element to search
        tag_names: List of tag names to look for (local names)

    Returns:
        First non-empty text value found, or empty string if none found
    """
    if el is None:
        return ""

    for tag in tag_names:
        for node in el.iter():
            if local_name(node.tag) == tag:
                txt = (node.text or "").strip()
                if txt:
                    return txt
    return ""


def extract_coded_value(scope: Optional[ET.Element]) -> str:
    """
    Robust extraction of coded value.

    Priority:
      1. <CODED-VALUE> (any depth)
      2. <V> (any depth)
      3. CODED-VALUE attribute (case-insensitive)
    """
    if scope is None:
        return ""

    cv = first_text(scope, ["CODED-VALUE"])
    if cv:
        return cv

    v = first_text(scope, ["V"])
    if v:
        return v

    return get_attr_ci(scope, "CODED-VALUE")

def _dedup_services(self, services: List[OdxService]) -> List[OdxService]:
        """
        Remove duplicate DIAG-SERVICE entries while preserving order.

        Deduplication strategy:
          1. Prefer service ID when available
          2. Fallback to service SHORT-NAME when ID is missing

        This is required after:
          - Inter-layer inheritance resolution
          - Multi-pass link expansion

        Args:
            services: List of OdxService objects (possibly with duplicates)

        Returns:
            New list with duplicates removed, original order preserved
        """

        seen: Set[str] = set()
        result: List[OdxService] = []

        for svc in services:
            # Prefer stable unique identifier
            key = svc.id or svc.shortName

            # If even fallback key is missing, keep service to avoid data loss
            if not key:
                result.append(svc)
                continue

            if key in seen:
                continue

            seen.add(key)
            result.append(svc)

        return result

def _resolve_links_for_layer(
        self,
        layer: OdxLayer,
        id_map: Dict[str, OdxLayer],
        visited: Set[str]
    ) -> None:
        """
        Resolve inherited content from linked layers.

        - Recursively resolves referenced layers first
        - Prevents cycles via 'visited'
        - Applies NOT-INHERITED-DIAG-COMMS filtering
        - Extends services, units, compu methods, DOPs, and DTCs
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

            # Resolve referenced layer first
            self._resolve_links_for_layer(ref_layer, id_map, visited)

            # ---- Services (apply NOT-INHERITED filtering if present)
            if ni_sn or ni_ids:
                for svc in ref_layer.services:
                    if (svc.shortName and svc.shortName in ni_sn):
                        continue
                    if (svc.id and svc.id in ni_ids):
                        continue
                    layer.services.append(svc)
            else:
                layer.services.extend(ref_layer.services)

            # ---- Other layer content (always inherited)
            layer.units.extend(ref_layer.units)
            layer.compuMethods.extend(ref_layer.compuMethods)
            layer.dataObjectProps.extend(ref_layer.dataObjectProps)
            layer.dtcs.extend(ref_layer.dtcs)

        # Deduplicate services after inheritance
        layer.services = self._dedup_services(layer.services)



def parse_not_inherited_from_xml(layer_el: ET.Element) -> Tuple[Set[str], Set[str]]:
    """
    Extract NOT-INHERITED-DIAG-COMMS from PARENT-REFS in the layer XML.
    Returns (excluded_service_short_names, excluded_service_ids).
    """
    excluded_sn: Set[str] = set()
    excluded_ids: Set[str] = set()

    prefs = find_child(layer_el, "PARENT-REFS")
    if not prefs:
        return excluded_sn, excluded_ids

    for pref in find_children(prefs, "PARENT-REF"):
        not_inh = find_child(pref, "NOT-INHERITED-DIAG-COMMS")
        if not_inh is None:
            continue
        for nic in find_children(not_inh, "NOT-INHERITED-DIAG-COMM"):
            snref = find_child(nic, "DIAG-COMM-SNREF")
            if snref is not None:
                sn = get_attr(snref, "SHORT-NAME") or get_text_local(snref, "SHORT-NAME")
                if sn:
                    excluded_sn.add(sn)
            idref = find_child(nic, "DIAG-COMM-REF")
            if idref is not None:
                rid = get_attr(idref, "ID-REF")
                if rid:
                    excluded_ids.add(rid)
    return excluded_sn, excluded_ids
def _get_not_inherited_sets(
        self,
        layer: OdxLayer
    ) -> Tuple[Set[str], Set[str]]:
        """
        Return NOT-INHERITED service identifiers for a layer.

        The layer attrs may contain:
          - NI_DIAGCOMM_SN : pipe-separated service SHORT-NAMEs
          - NI_DIAGCOMM_ID : pipe-separated service IDs

        Returns:
            (excluded_short_names, excluded_ids)
        """
        excluded_sn: Set[str] = set()
        excluded_ids: Set[str] = set()

        if not layer.attrs:
            return excluded_sn, excluded_ids

        sn_val = layer.attrs.get("NI_DIAGCOMM_SN", "")
        if sn_val:
            excluded_sn = {x for x in sn_val.split("|") if x}

        id_val = layer.attrs.get("NI_DIAGCOMM_ID", "")
        if id_val:
            excluded_ids = {x for x in id_val.split("|") if x}

        return excluded_sn, excluded_ids

def flatten_service_params(
        self,
        service: OdxService
    ) -> List[OdxParam]:
        """
        Return all root-level parameters for a service, including:
          - REQUEST params
          - POS-RESPONSE params
          - NEG-RESPONSE params

        Child parameters (STRUCTURE / TABLE) remain nested.
        """
        out: List[OdxParam] = []

        if service.request:
            out.extend(service.request.params or [])

        for resp in service.posResponses or []:
            out.extend(resp.params or [])

        for resp in service.negResponses or []:
            out.extend(resp.params or [])

        return out

def flatten_layer_params(
        self,
        layer: OdxLayer
    ) -> List[OdxParam]:
        """
        Return all parameters belonging to a layer by flattening
        parameters across all services.
        """
        out: List[OdxParam] = []

        for svc in layer.services or []:
            out.extend(self.flatten_service_params(svc))

        return out

def flatten_param_tree(
        self,
        params: List[OdxParam]
    ) -> List[OdxParam]:
        """
        Recursively flatten a parameter tree.

        Input:
            - A list of root OdxParam objects

        Output:
            - A flat list containing:
                * root parameters
                * all nested STRUCTURE parameters
                * all TABLE-ROW parameters
                * all nested children under TABLE-ROW

        Characteristics:
            - Depth-first traversal
            - Order-preserving
            - No mutation of the original tree
            - No deduplication (intentional)
        """

        flat: List[OdxParam] = []

        def visit(p: OdxParam) -> None:
            flat.append(p)
            for c in getattr(p, "children", []) or []:
                visit(c)

        for p in params or []:
            visit(p)

        return flat

def flatten_param_tree_iterative(
        self,
        params: List[OdxParam]
    ) -> List[OdxParam]:
        """
        Iterative (non-recursive) deep flatten of parameter tree.

        Advantages:
          - No recursion depth risk
          - Faster for large structures
          - Order-preserving (DFS)

        Returns:
            Flat list of all parameters
        """

        flat: List[OdxParam] = []
        stack: List[OdxParam] = []

        # Push in reverse so original order is preserved
        for p in reversed(params or []):
            stack.append(p)

        while stack:
            p = stack.pop()
            flat.append(p)

            children = getattr(p, "children", []) or []
            for c in reversed(children):
                stack.append(c)

        return flat

def validate_params(
        self,
        params: List[OdxParam]
    ) -> Dict[str, List[str]]:
        """
        Validate parameters for common structural issues.

        Checks performed:
          1. Duplicate parentName paths
          2. Missing bitLength on coded parameters
          3. Missing baseDataType on coded parameters

        Returns:
            Dict with keys:
              - 'duplicate_paths'
              - 'missing_bit_length'
              - 'missing_base_type'
        """

        issues = {
            "duplicate_paths": [],
            "missing_bit_length": [],
            "missing_base_type": [],
        }

        seen_paths: Set[str] = set()

        for p in params:
            path = p.parentName or p.shortName

            # ---- Duplicate path detection
            if path in seen_paths:
                issues["duplicate_paths"].append(path)
            else:
                seen_paths.add(path)

            # ---- Missing bit length (coded parameters)
            if p.baseDataType and not p.bitLength:
                issues["missing_bit_length"].append(path)

            # ---- Missing base data type
            if not p.baseDataType:
                issues["missing_base_type"].append(path)

        return issues

def flatten_structure_params(
        self,
        params: List[OdxParam]
    ) -> List[OdxParam]:
        """
        Flatten only STRUCTURE parameters and their descendants.

        Root parameters are included only if they have children.
        TABLE-ROW parameters are excluded.
        """

        flat: List[OdxParam] = []
        stack: List[OdxParam] = list(params or [])

        while stack:
            p = stack.pop()
            children = getattr(p, "children", []) or []

            if children and p.parentType == "STRUCTURE":
                flat.append(p)

            for c in reversed(children):
                stack.append(c)

        return flat


def flatten_table_params(
        self,
        params: List[OdxParam]
    ) -> List[OdxParam]:
        """
        Flatten only TABLE-ROW parameters and their descendants.
        """

        flat: List[OdxParam] = []
        stack: List[OdxParam] = list(params or [])

        while stack:
            p = stack.pop()

            if p.parentType == "TABLE-KEY":
                flat.append(p)

            for c in reversed(getattr(p, "children", []) or []):
                stack.append(c)

        return flat
