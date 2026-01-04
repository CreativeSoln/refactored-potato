from __future__ import annotations

import xml.etree.ElementTree as ET
import re
from typing import Tuple

from models import (
    OdxContainer,
    OdxLayer,
    OdxService,
    OdxMessage,
    OdxParam,
)

# ---------------------------------------------------------
# Low-level XML recovery helpers (AS IN YOUR SCREENSHOT)
# ---------------------------------------------------------

def _slice_from_first_lt(raw: bytes) -> bytes:
    i = raw.find(b"<")
    return raw if i <= 0 else raw[i:]


def _try_parse_bytes(raw: bytes) -> ET.Element:
    raw1 = _slice_from_first_lt(raw)

    try:
        return ET.fromstring(raw1)
    except ET.ParseError:
        pass

    for enc in ("utf-8", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1"):
        try:
            text = raw1.decode(enc, errors="strict")
        except UnicodeDecodeError:
            continue

        if "<" in text and ">" in text:
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


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1]


# ---------------------------------------------------------
# Parser
# ---------------------------------------------------------

class ODXParser:
    """
    Parser is BYTES-only.
    ZIP / PDX handling is done by UI (as in your loader).
    """

    # -------- entry points --------

    def parse_xml_bytes(self, raw: bytes) -> ET.Element:
        return _try_parse_bytes(raw)

    def parse_odx_bytes(self, filename: str, raw: bytes) -> Tuple[str, OdxContainer]:
        root = self.parse_xml_bytes(raw)
        return filename, self.parse_container(root)

    # -------- container --------

    def parse_container(self, root: ET.Element) -> OdxContainer:
        cont = OdxContainer()

        # ODX root → DIAG-LAYER-CONTAINER may be nested
        if _local(root.tag) != "DIAG-LAYER-CONTAINER":
            found = root.find(".//DIAG-LAYER-CONTAINER")
            if found is not None:
                root = found

        for el in root.iter():
            tag = _local(el.tag)

            if tag == "ECU-VARIANT":
                cont.ecuVariants.append(self._parse_layer(el, "ECU-VARIANT"))
            elif tag == "BASE-VARIANT":
                cont.baseVariants.append(self._parse_layer(el, "BASE-VARIANT"))
            elif tag == "PROTOCOL":
                cont.protocols.append(self._parse_layer(el, "PROTOCOL"))
            elif tag == "FUNCTIONAL-GROUP":
                cont.functionalGroups.append(self._parse_layer(el, "FUNCTIONAL-GROUP"))
            elif tag == "ECU-SHARED-DATA":
                cont.ecuSharedData.append(self._parse_layer(el, "ECU-SHARED-DATA"))

        return cont

    # -------- layer --------

    def _parse_layer(self, el: ET.Element, layer_type: str) -> OdxLayer:
        layer = OdxLayer(
            layerType=layer_type,
            id=el.get("ID", ""),
            shortName=self._text(el, "SHORT-NAME"),
            longName=self._text(el, "LONG-NAME"),
            description=self._text(el, "DESC"),
        )

        for s in el.iter():
            if _local(s.tag) == "DIAG-SERVICE":
                layer.services.append(self._parse_service(s))

        return layer

    # -------- service --------

    def _parse_service(self, el: ET.Element) -> OdxService:
        svc = OdxService(
            id=el.get("ID", ""),
            shortName=self._text(el, "SHORT-NAME"),
            longName=self._text(el, "LONG-NAME"),
            description=self._text(el, "DESC"),
            semantic=el.get("SEMANTIC", ""),
        )

        for c in el:
            t = _local(c.tag)
            if t == "REQUEST":
                svc.request = self._parse_message(c)
            elif t == "POS-RESPONSE":
                svc.posResponses.append(self._parse_message(c))
            elif t == "NEG-RESPONSE":
                svc.negResponses.append(self._parse_message(c))

        return svc

    # -------- message --------

    def _parse_message(self, el: ET.Element) -> OdxMessage:
        msg = OdxMessage(
            id=el.get("ID", ""),
            shortName=self._text(el, "SHORT-NAME"),
            longName=self._text(el, "LONG-NAME"),
        )

        for p in el.iter():
            if _local(p.tag) == "PARAM":
                msg.params.append(self._parse_param(p))

        return msg

    # -------- param --------

    def _parse_param(self, el: ET.Element) -> OdxParam:
        p = OdxParam(
            shortName=self._text(el, "SHORT-NAME"),
            longName=self._text(el, "LONG-NAME"),
            description=self._text(el, "DESC"),
            semantic=self._text(el, "SEMANTIC"),
            bytePosition=self._text(el, "BYTE-POSITION"),
            bitLength=self._text(el, "BIT-LENGTH"),
        )

        for c in el:
            t = _local(c.tag)
            if t == "CODED-CONST":
                p.codedConstValue = self._text(c, "CODED-VALUE")
            elif t == "PHYS-CONST":
                p.physConstValue = self._text(c, "V")

        p.children = []
        return p

    # -------- util --------

    def _text(self, el: ET.Element, tag: str) -> str:
        for c in el:
            if _local(c.tag) == tag:
                return (c.text or "").strip()
        return ""
