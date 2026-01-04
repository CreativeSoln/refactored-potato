from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Tuple, List

from models import (
    OdxParam,
    OdxMessage,
    OdxService,
    OdxLayer,
    OdxContainer,
)


# =========================================================
# Parser
# =========================================================

class ODXParser:
    """
    Conservative ODX parser.
    This file only guarantees structural correctness for the UI.
    No semantic decoding is attempted here.
    """

    # -----------------------------------------------------
    # Public API (UI depends on this)
    # -----------------------------------------------------

    def parse_odx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        root = self._parse_xml_bytes(content)
        container = self._parse_container(root)
        return filename, container

    # -----------------------------------------------------
    # XML helpers
    # -----------------------------------------------------

    def _parse_xml_bytes(self, content: bytes) -> ET.Element:
        try:
            return ET.fromstring(content)
        except ET.ParseError:
            # tolerate BOM / garbage before XML
            text = content.decode("utf-8", errors="ignore")
            start = text.find("<")
            if start >= 0:
                return ET.fromstring(text[start:].encode("utf-8"))
            raise

    def _text(self, el: ET.Element, tag: str) -> str:
        t = el.findtext(tag)
        return t.strip() if t else ""

    # -----------------------------------------------------
    # Container
    # -----------------------------------------------------

    def _parse_container(self, root: ET.Element) -> OdxContainer:
        cont = OdxContainer()

        for ev in root.iter("ECU-VARIANT"):
            cont.ecuVariants.append(self._parse_layer(ev, "ECU-VARIANT"))

        for bv in root.iter("BASE-VARIANT"):
            cont.baseVariants.append(self._parse_layer(bv, "BASE-VARIANT"))

        return cont

    # -----------------------------------------------------
    # Layer
    # -----------------------------------------------------

    def _parse_layer(self, layer_el: ET.Element, layer_type: str) -> OdxLayer:
        layer = OdxLayer(
            layerType=layer_type,
            id=layer_el.get("ID", ""),
            shortName=self._text(layer_el, "SHORT-NAME"),
            longName=self._text(layer_el, "LONG-NAME"),
            description=self._text(layer_el, "DESC"),
        )

        for svc_el in layer_el.iter("DIAG-SERVICE"):
            layer.services.append(self._parse_service(svc_el))

        return layer

    # -----------------------------------------------------
    # Service
    # -----------------------------------------------------

    def _parse_service(self, svc_el: ET.Element) -> OdxService:
        svc = OdxService(
            id=svc_el.get("ID", ""),
            shortName=self._text(svc_el, "SHORT-NAME"),
            longName=self._text(svc_el, "LONG-NAME"),
            description=self._text(svc_el, "DESC"),
            semantic=svc_el.get("SEMANTIC", ""),
        )

        # REQUEST
        req_el = svc_el.find("REQUEST")
        if req_el is not None:
            svc.request = self._parse_message(req_el)

        # POS RESPONSES
        for pr in svc_el.findall("POS-RESPONSE"):
            svc.posResponses.append(self._parse_message(pr))

        # NEG RESPONSES
        for nr in svc_el.findall("NEG-RESPONSE"):
            svc.negResponses.append(self._parse_message(nr))

        return svc

    # -----------------------------------------------------
    # Message
    # -----------------------------------------------------

    def _parse_message(self, msg_el: ET.Element) -> OdxMessage:
        msg = OdxMessage(
            id=msg_el.get("ID", ""),
            shortName=self._text(msg_el, "SHORT-NAME"),
            longName=self._text(msg_el, "LONG-NAME"),
        )

        for p_el in msg_el.findall(".//PARAM"):
            msg.params.append(self._parse_param(p_el))

        return msg

    # -----------------------------------------------------
    # Param
    # -----------------------------------------------------

    def _parse_param(self, p_el: ET.Element) -> OdxParam:
        p = OdxParam(
            shortName=self._text(p_el, "SHORT-NAME"),
            longName=self._text(p_el, "LONG-NAME"),
            description=self._text(p_el, "DESC"),
            semantic=self._text(p_el, "SEMANTIC"),
            bytePosition=self._text(p_el, "BYTE-POSITION"),
            bitLength=self._text(p_el, "BIT-LENGTH"),
        )

        # Constants (safe, optional)
        coded = p_el.find("CODED-CONST")
        if coded is not None:
            p.codedConstValue = self._text(coded, "CODED-VALUE")

        phys = p_el.find("PHYS-CONST")
        if phys is not None:
            p.physConstValue = self._text(phys, "V")

        # Children (STRUCTURE support placeholder)
        p.children = []

        return p
