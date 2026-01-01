# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import zipfile
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional, Any

from models import (
    OdxContainer, OdxDatabase, OdxLayer, OdxService,
    OdxMessage, OdxParam, OdxUnit, OdxCompuMethod,
    OdxCompuScale, OdxTableRow, OdxDataObjectProp, OdxDTC
)


# ---------------- Namespace Helper ----------------
def tag(e: ET.Element) -> str:
    """remove namespace"""
    return e.tag[e.tag.rfind("}") + 1:]


# ---------------- Main Parser ----------------
class ODXParser:

    def parse_file(self, path: str) -> Optional[OdxContainer]:
        if path.lower().endswith(".pdx") or path.lower().endswith(".zip"):
            return self._parse_pdx(path)

        return self._parse_xml(path)


    def _parse_pdx(self, path: str) -> Optional[OdxContainer]:
        try:
            with zipfile.ZipFile(path, "r") as z:
                xml_files = [f for f in z.namelist() if f.lower().endswith(".xml")]

                containers = []
                for xf in xml_files:
                    with z.open(xf) as f:
                        raw = f.read()
                        containers.append(self._parse_xml_raw(raw))
                return merge_containers(containers)

        except Exception as ex:
            print("PDX Parse Failed:", ex)
            return None


    def _parse_xml(self, path: str) -> Optional[OdxContainer]:
        try:
            tree = ET.parse(path)
            root = tree.getroot()
            return self._parse_container(root)

        except Exception as ex:
            print("XML Parse Failed:", ex)
            return None


    def _parse_xml_raw(self, raw: bytes) -> Optional[OdxContainer]:
        try:
            text = raw.decode("utf-8", errors="ignore")
            root = ET.fromstring(text)
            return self._parse_container(root)
        except Exception as ex:
            print("Raw XML Parse Failed:", ex)
            return None


    # ---------------- Container Parse ----------------
    def _parse_container(self, root: ET.Element) -> Optional[OdxContainer]:
        cont = OdxContainer()

        for child in root:
            name = tag(child)

            if name == "PROTOCOLS":
                cont.protocols = self._parse_layers(child)

            elif name == "FUNCTIONAL-GROUPS":
                cont.functionalGroups = self._parse_layers(child)

            elif name == "BASE-VARIANTS":
                cont.baseVariants = self._parse_layers(child)

            elif name == "ECU-VARIANTS":
                cont.ecuVariants = self._parse_layers(child)

            elif name == "ECU-SHARED-DATA":
                cont.ecuSharedData = self._parse_layers(child)

        return cont


    # ---------------- Layers ----------------
    def _parse_layers(self, node: ET.Element) -> List[OdxLayer]:
        layers = []

        for lay in node:
            if tag(lay) not in ("BASE-VARIANT", "ECU-VARIANT", "PROTOCOL", "FUNCTIONAL-GROUP"):
                continue

            L = OdxLayer()
            L.layerType = tag(lay)
            L.id = lay.attrib.get("ID", "")
            L.shortName = (lay.findtext(".//SHORT-NAME") or "")
            L.longName = (lay.findtext(".//LONG-NAME") or "")

            # SERVICES
            svc_node = lay.find(".//DIAG-SERVICES")
            if svc_node is not None:
                L.services = self._parse_services(svc_node)

            # UNITS
            units_node = lay.find(".//UNITS")
            if units_node is not None:
                L.units = [self._parse_unit(u) for u in units_node.findall(".//UNIT")]

            # COMPU-METHODS
            cm_node = lay.find(".//COMPU-METHODS")
            if cm_node is not None:
                L.compuMethods = [self._parse_compu(m) for m in cm_node.findall(".//COMPU-METHOD")]

            # DATA OBJECT PROPS
            dop_node = lay.find(".//DATA-OBJECT-PROPS")
            if dop_node is not None:
                L.dataObjectProps = [
                    self._parse_dop(d) for d in dop_node.findall(".//DATA-OBJECT-PROP")
                ]

            layers.append(L)

        return layers


    # ---------------- Services ----------------
    def _parse_services(self, node: ET.Element) -> List[OdxService]:
        result = []

        for s in node.findall(".//DIAG-SERVICE"):
            svc = OdxService()
            svc.id = s.attrib.get("ID", "")
            svc.shortName = s.findtext("SHORT-NAME", "")
            svc.longName = s.findtext("LONG-NAME", "")
            svc.semantic = s.findtext("SEMANTIC", "")

            # REQUEST
            req = s.find(".//REQUEST")
            if req is not None:
                svc.request = self._parse_message(req)

            # POS RESP
            for r in s.findall(".//POS-RESPONSE"):
                svc.posResponses.append(self._parse_message(r))

            # NEG RESP
            for r in s.findall(".//NEG-RESPONSE"):
                svc.negResponses.append(self._parse_message(r))

            result.append(svc)

        return result


    # ---------------- Message ----------------
    def _parse_message(self, node: ET.Element) -> OdxMessage:
        msg = OdxMessage()
        msg.id = node.attrib.get("ID", "")
        msg.shortName = node.findtext("SHORT-NAME", "")

        params = node.find(".//PARAMS")
        if params is not None:
            msg.params = [self._parse_param(p) for p in params.findall(".//PARAM")]

        return msg


    # ---------------- Parameter ----------------
    def _parse_param(self, node: ET.Element) -> OdxParam:
        p = OdxParam()
        p.id = node.attrib.get("ID", "")
        p.shortName = node.findtext("SHORT-NAME", "")
        p.semantic = node.findtext("SEMANTIC", "")

        # child params (structures)
        for c in node.findall(".//SUB-PARAM"):
            p.children.append(self._parse_param(c))

        return p


    # ---------------- Units ----------------
    def _parse_unit(self, node: ET.Element) -> OdxUnit:
        u = OdxUnit()
        u.id = node.attrib.get("ID", "")
        u.shortName = node.findtext("SHORT-NAME", "")
        return u


    # ---------------- Compu Method ----------------
    def _parse_compu(self, node: ET.Element) -> OdxCompuMethod:
        c = OdxCompuMethod()
        c.id = node.attrib.get("ID", "")
        c.shortName = node.findtext("SHORT-NAME", "")
        return c


    def _parse_dop(self, node: ET.Element) -> OdxDataObjectProp:
        d = OdxDataObjectProp()
        d.id = node.attrib.get("ID", "")
        d.shortName = node.findtext("SHORT-NAME", "")
        return d



# ================== Merge Containers ===================
def merge_containers(containers: List[OdxContainer]) -> Optional[OdxDatabase]:
    if not containers:
        return None

    db = OdxDatabase()

    for c in containers:
        db.ecuVariants += c.ecuVariants
        db.baseVariants += c.baseVariants
        db.protocols += c.protocols
        db.functionalGroups += c.functionalGroups
        db.ecuSharedData += c.ecuSharedData

    return db



# ================== Helper ===================
def flatten_service_params(msg: Dict) -> List[Dict]:
    """Used in Tree UI param counting"""
    out = []

    def walk(params):
        for p in params:
            out.append(p)
            walk(p.get("children", []))

    walk(msg.get("params", []))
    return out
