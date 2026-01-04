from __future__ import annotations

import xml.etree.ElementTree as ET
import zipfile
import io
import logging
from typing import Tuple

from models import (
    OdxContainer,
    OdxLayer,
    OdxService,
    OdxMessage,
    OdxParam,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR)

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


# ---------------------------------------------------------
# Parser
# ---------------------------------------------------------

class ODXParser:

    # =====================================================
    # PUBLIC API (USED BY UI – DO NOT CHANGE)
    # =====================================================
    def parse_odx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        zip_sig = b"PK\x03\x04"

        # ---- Detect PDX (ZIP) anywhere in header ----
        if zip_sig in content[:1024]:
            fname, container = self._parse_pdx_bytes(filename, content)
        else:
            root = self._try_parse_bytes(content)
            root = self._ensure_container(root)
            container = self._parse_container(root)
            fname = filename

        # 🔴 ONE-LINE DEBUG COUNT (REQUESTED)
        logger.error(
            "[ODX-DEBUG] %s | ECU=%d BASE=%d PROTO=%d FG=%d",
            fname,
            len(container.ecuVariants),
            len(container.baseVariants),
            len(container.protocols),
            len(container.functionalGroups),
        )

        return fname, container

    # =====================================================
    # XML parsing helpers
    # =====================================================
    def _try_parse_bytes(self, raw: bytes) -> ET.Element:
        # Try raw
        try:
            return ET.fromstring(raw)
        except ET.ParseError:
            pass

        # Try decoding variants
        for enc in ("utf-8", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1"):
            try:
                text = raw.decode(enc, errors="strict")
            except UnicodeDecodeError:
                continue

            idx = text.find("<")
            if idx >= 0:
                try:
                    return ET.fromstring(text[idx:].encode("utf-8"))
                except ET.ParseError:
                    continue

        raise ET.ParseError("Unable to parse XML content")

    def _ensure_container(self, root: ET.Element) -> ET.Element:
        # ODX root may be <ODX>, container is nested
        if local_name(root.tag) == "DIAG-LAYER-CONTAINER":
            return root

        found = root.find(".//DIAG-LAYER-CONTAINER")
        if found is not None:
            return found

        return root

    def _text(self, el: ET.Element, tag: str) -> str:
        for c in el:
            if local_name(c.tag) == tag:
                return (c.text or "").strip()
        return ""

    # =====================================================
    # PDX (ZIP) handling
    # =====================================================
    def _parse_pdx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        container = OdxContainer()

        zip_sig = b"PK\x03\x04"
        idx = content.find(zip_sig)
        if idx < 0:
            logger.error("[ODX-DEBUG] ZIP signature not found in %s", filename)
            return filename, container

        zip_bytes = content[idx:]

        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            odx_files = [n for n in zf.namelist() if n.lower().endswith(".odx")]
            logger.error("[ODX-DEBUG] %s contains %d ODX files", filename, len(odx_files))

            for name in odx_files:
                try:
                    xml_bytes = zf.read(name)
                    root = self._try_parse_bytes(xml_bytes)
                    root = self._ensure_container(root)
                    sub = self._parse_container(root)

                    container.ecuVariants.extend(sub.ecuVariants)
                    container.baseVariants.extend(sub.baseVariants)
                    container.protocols.extend(sub.protocols)
                    container.functionalGroups.extend(sub.functionalGroups)
                    container.ecuSharedData.extend(sub.ecuSharedData)

                except Exception as ex:
                    logger.error("[ODX-DEBUG] Failed parsing %s: %s", name, ex)

        return filename, container

    # =====================================================
    # Container
    # =====================================================
    def _parse_container(self, root: ET.Element) -> OdxContainer:
        cont = OdxContainer()

        for el in root.iter():
            tag = local_name(el.tag)

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

    # =====================================================
    # Layer
    # =====================================================
    def _parse_layer(self, el: ET.Element, layer_type: str) -> OdxLayer:
        layer = OdxLayer(
            layerType=layer_type,
            id=el.get("ID", ""),
            shortName=self._text(el, "SHORT-NAME"),
            longName=self._text(el, "LONG-NAME"),
            description=self._text(el, "DESC"),
        )

        for s in el.iter():
            if local_name(s.tag) == "DIAG-SERVICE":
                layer.services.append(self._parse_service(s))

        return layer

    # =====================================================
    # Service
    # =====================================================
    def _parse_service(self, el: ET.Element) -> OdxService:
        svc = OdxService(
            id=el.get("ID", ""),
            shortName=self._text(el, "SHORT-NAME"),
            longName=self._text(el, "LONG-NAME"),
            description=self._text(el, "DESC"),
            semantic=el.get("SEMANTIC", ""),
        )

        for c in el:
            tag = local_name(c.tag)
            if tag == "REQUEST":
                svc.request = self._parse_message(c)
            elif tag == "POS-RESPONSE":
                svc.posResponses.append(self._parse_message(c))
            elif tag == "NEG-RESPONSE":
                svc.negResponses.append(self._parse_message(c))

        return svc

    # =====================================================
    # Message
    # =====================================================
    def _parse_message(self, el: ET.Element) -> OdxMessage:
        msg = OdxMessage(
            id=el.get("ID", ""),
            shortName=self._text(el, "SHORT-NAME"),
            longName=self._text(el, "LONG-NAME"),
        )

        for p in el.iter():
            if local_name(p.tag) == "PARAM":
                msg.params.append(self._parse_param(p))

        return msg

    # =====================================================
    # Param
    # =====================================================
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
            tag = local_name(c.tag)
            if tag == "CODED-CONST":
                p.codedConstValue = self._text(c, "CODED-VALUE")
            elif tag == "PHYS-CONST":
                p.physConstValue = self._text(c, "V")

        p.children = []
        return p
