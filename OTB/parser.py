from __future__ import annotations
import uuid
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

# ---------------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------------

class ODXParser:

    # ================================================
    # XML root parser
    # ================================================
    def parse_xml(self, content: str) -> ET.Element:
        return ET.fromstring(content)

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

        raise ValueError("No DIAG-LAYER-CONTAINER root found")

    def _parse_layer(self, layer_el: ET.Element, layerType: str) -> OdxLayer:

        # ---------------------------------------------------------
        # Build REQUEST map
        # ---------------------------------------------------------
        request_map: Dict[str, OdxMessage] = {}
        pos_resp_map: Dict[str, OdxMessage] = {}
        neg_resp_map: Dict[str, OdxMessage] = {}
        dop_map: Dict[str, OdxDataObjectProp] = {}

        for d in findall_descendants(layer_el, "DATA-OBJECT-PROP"):
            dd = self._parse_dop(d)
            dop_map[dd.id] = dd

        units: List[OdxUnit] = [self._parse_unit(u) for u in findall_descendants(layer_el, "UNIT")]
        compu_methods: List[OdxCompuMethod] = [self._parse_compu_method(c)
            for c in findall_descendants(layer_el, "COMPU-METHOD")]
        dtcs: List[OdxDTC] = [self._parse_dtc(dtc) for dtc in findall_descendants(layer_el, "DTC")]

        for req in findall_descendants(layer_el, "REQUEST"):
            rid = get_attr(req, "ID")
            rshort = get_text_local(req, "SHORT-NAME")
            rparams = [
                self._parse_param(p, "REQUEST", rshort, "", "", {})
                for p in findall_descendants(req, "PARAM")
            ]
            request_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(req, "LONG-NAME"),
                params=rparams
            )

        # ---------------------------------------------------------
        # Build POS-RESPONSE map
        # ---------------------------------------------------------
        pos_resp_map: Dict[str, OdxMessage] = {}
        for res in findall_descendants(layer_el, "POS-RESPONSE"):
            rid = get_attr(res, "ID")
            rshort = get_text_local(res, "SHORT-NAME")
            rparams = [
                self._parse_param(p, "POS_RESPONSE", rshort, "", "", {})
                for p in findall_descendants(res, "PARAM")
            ]
            pos_resp_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(res, "LONG-NAME"),
                params=rparams
            )

        # ---------------------------------------------------------
        # Build NEG-RESPONSE map
        # ---------------------------------------------------------
        neg_resp_map: Dict[str, OdxMessage] = {}
        for res in findall_descendants(layer_el, "NEG-RESPONSE"):
            rid = get_attr(res, "ID")
            rshort = get_text_local(res, "SHORT-NAME")
            rparams = [
                self._parse_param(p, "NEG_RESPONSE", rshort, "", "", {})
                for p in findall_descendants(res, "PARAM")
            ]
            neg_resp_map[rid] = OdxMessage(
                id=rid,
                shortName=rshort,
                longName=get_text_local(res, "LONG-NAME"),
                params=rparams
            )

        # =================================================================
        # SERVICES — reference resolution + inline fallback
        # =================================================================
        services: List[OdxService] = []

        for svc_el in findall_descendants(layer_el, "DIAG-SERVICE"):
            svc_attrs = get_all_attrs(svc_el)
            svc_short = get_text_local(svc_el, "SHORT-NAME")

            request_ref = find_child(svc_el, "REQUEST-REF")
            request_ref_id = get_attr(request_ref, "ID-REF") if request_ref is not None else ""

            pos_ref_ids = [get_attr(r, "ID-REF") for r in get_elements(svc_el, "POS-RESPONSE-REF")]
            neg_ref_ids = [get_attr(r, "ID-REF") for r in get_elements(svc_el, "NEG-RESPONSE-REF")]

            inline_req = find_child(svc_el, "REQUEST")
            inline_pos = find_children(svc_el, "POS-RESPONSE")
            inline_neg = find_children(svc_el, "NEG-RESPONSE")

            # -----------------------------------------------------
            # REQUEST
            # -----------------------------------------------------
            request = None
            if request_ref_id and request_ref_id in request_map:
                request = request_map[request_ref_id]

            elif inline_req is not None:
                rshort = get_text_local(inline_req, "SHORT-NAME") or svc_short + "_req"
                rparams = [
                    self._parse_param(p, "REQUEST", rshort, "", "", {})
                    for p in findall_descendants(inline_req, "PARAM")
                ]
                request = OdxMessage(
                    id=get_attr(inline_req, "ID"),
                    shortName=rshort,
                    longName=get_text_local(inline_req, "LONG-NAME"),
                    params=rparams
                )

            # -----------------------------------------------------
            # POSITIVE RESPONSES
            # -----------------------------------------------------
            pos_responses: List[OdxMessage] = []

            for rid in pos_ref_ids:
                rr = pos_resp_map.get(rid)
                if rr:
                    pos_responses.append(rr)

            if inline_pos:
                for el in inline_pos:
                    rshort = get_text_local(el, "SHORT-NAME") or svc_short + "_pos"
                    rparams = [
                        self._parse_param(p, "POS_RESPONSE", rshort, "", "", {})
                        for p in findall_descendants(el, "PARAM")
                    ]
                    pos_responses.append(
                        OdxMessage(
                            id=get_attr(el, "ID"),
                            shortName=rshort,
                            longName=get_text_local(el, "LONG-NAME"),
                            params=rparams
                        )
                    )

            # -----------------------------------------------------
            # NEGATIVE RESPONSES
            # -----------------------------------------------------
            neg_responses: List[OdxMessage] = []

            for rid in neg_ref_ids:
                rr = neg_resp_map.get(rid)
                if rr:
                    neg_responses.append(rr)

            if inline_neg:
                for el in inline_neg:
                    rshort = get_text_local(el, "SHORT-NAME") or svc_short + "_neg"
                    rparams = [
                        self._parse_param(p, "NEG_RESPONSE", rshort, "", "", {})
                        for p in findall_descendants(el, "PARAM")
                    ]
                    neg_responses.append(
                        OdxMessage(
                            id=get_attr(el, "ID"),
                            shortName=rshort,
                            longName=get_text_local(el, "LONG-NAME"),
                            params=rparams
                        )
                    )

            # -----------------------------------------------------
            # Annotate params with serviceShortName
            # -----------------------------------------------------
            if request:
                request.params = [p.__class__(**{**asdict(p), "serviceShortName": svc_short}) for p in request.params]

            for rr in pos_responses:
                rr.params = [p.__class__(**{**asdict(p), "serviceShortName": svc_short}) for p in rr.params]

            for rr in neg_responses:
                rr.params = [p.__class__(**{**asdict(p), "serviceShortName": svc_short}) for p in rr.params]

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

        # =================================================================
        # FINALIZE LAYER OBJECT (matches screenshot)
        # =================================================================
        parent_ref = find_child(layer_el, "PARENT-REF")
        linked_ids = self._collect_links(layer_el)

        layer = OdxLayer(
            layerType=layerType,
            id=get_attr(layer_el, "ID", ""),
            shortName=get_text_local(layer_el, "SHORT-NAME"),
            longName=get_text_local(layer_el, "LONG-NAME"),
            description=get_text_local(layer_el, "DESC"),
            parentId=get_attr(parent_ref, "ID-REF") if parent_ref is not None else "",
            rxId=get_text_local(layer_el, "RECEIVE-ID"),
            txId=get_text_local(layer_el, "TRANSMIT-ID"),
            services=services,
            units=units,
            compuMethods=compu_methods,
            dataObjectProps=list(dop_map.values()),
            dtcs=dtcs,
            attrs=get_all_attrs(layer_el),
            linkedLayerIds=linked_ids,
        )

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
    def parse_odx_file(self, filename: str, content: str) -> Tuple[str, OdxContainer]:
        root = self.parse_xml(content)
        return filename, self.parse_container(root)

    # --------------------------------------------------------------------
    # PARAM PARSER  (Confirmed from screenshot)
    # --------------------------------------------------------------------
    def _parse_param(
        self,
        param_el: ET.Element,
        parentType: str,
        parentName: str,
        layerName: str,
        serviceShortName: str,
        dop_map: Dict[str, OdxDataObjectProp],
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
        pid = f"{layerName}::{serviceShortName}::{parentType}::{shortName}::{uuid.uuid4().hex[:9]}"

        param = OdxParam(
            id=pid,
            shortName=shortName,
            longName=get_text_local(param_el, "LONG-NAME"),
            description=get_text_local(param_el, "DESC"),
            semantic=attrs.get("SEMANTIC", ""),
            bytePosition=get_text_local(param_el, "BYTE-POSITION"),
            bitPosition=get_text_local(param_el, "BIT-POSITION"),
            bitLength=get_text_local(diagCodedType, "BIT-LENGTH") if diagCodedType is not None else "",
            minLength=get_text_local(diagCodedType, "MIN-LENGTH") if diagCodedType is not None else "",
            maxLength=get_text_local(diagCodedType, "MAX-LENGTH") if diagCodedType is not None else "",
            baseDataType=get_attr(diagCodedType, "BASE-DATA-TYPE") if diagCodedType is not None else "",
            physicalBaseType=get_attr(physType, "BASE-DATA-TYPE") if physType is not None else "",
            isHighLowByteOrder=get_attr(diagCodedType, "IS-HIGHLOW-BYTE-ORDER") if diagCodedType is not None else "",
            codedConstValue=(
                (get_text_local(codedConst, "CODED-VALUE")
                or get_text_local(codedConst, "V")
                or get_attr(codedConst, "CODED-VALUE"))
                if codedConst is not None else ""
            ),
            physConstValue=get_text_local(physConst, "V") if physConst is not None else "",
            dopRefId=get_attr(dopRef, "ID-REF") if dopRef is not None else "",
            dopSnRefName=get_attr(dopSnRef, "SHORT-NAME") if dopSnRef is not None else "",
            compuMethodRefId=get_attr(compuRef, "ID-REF") if compuRef is not None else "",
            parentType=parentType,
            parentName=parentName,
            layerName=layerName,
            serviceShortName=serviceShortName,
            attrs=attrs,
        )

        # ---------- DOP STRUCTURE CHILDREN ----------
        dop_id = attrs.get("DOP-REF") or (get_attr(dopRef, "ID-REF") if dopRef else "")
        if dop_id:
            dop = dop_map.get(dop_id)
            if dop and dop.structureParams:
                for child_el in dop.structureParams:
                    param.children.append(
                        self._parse_param(child_el, "STRUCTURE", shortName,
                                          layerName, serviceShortName, dop_map)
                    )

        return param


    # =====================================================================================
    # UNIT PARSER  (Confirmed screenshot)
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
    # COMPU-METHOD PARSER  (Confirmed Screenshot)
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

        for ref_id in layer.linkedLayerIds:
            ref_layer = id_map.get(ref_id)
            if not ref_layer:
                continue

            # Recursively resolve the referenced layer first
            self._resolve_links_for_layer(ref_layer, id_map, visited)

            # ---- Merge content ----
            layer.services.extend(ref_layer.services)
            layer.units.extend(ref_layer.units)
            layer.compuMethods.extend(ref_layer.compuMethods)
            layer.dataObjectProps.extend(ref_layer.dataObjectProps)
            layer.dtcs.extend(ref_layer.dtcs)

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

        # ---- Resolve LINKS
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
