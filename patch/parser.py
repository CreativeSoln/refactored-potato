from __future__ import annotations

import xml.etree.ElementTree as ET
import zipfile
import io
from typing import Tuple

from models import (
    OdxParam,
    OdxMessage,
    OdxService,
    OdxLayer,
    OdxContainer,
)

# =========================================================
# Helpers
# =========================================================

def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


# =========================================================
# Parser
# =========================================================

class ODXParser:
    """
    Minimal, UI-compatible ODX / PDX parser.
    Structure preserved. UI contract preserved.
    Debug prints added ONLY for diagnostics.
    """

    # -----------------------------------------------------
    # PUBLIC API
    # -----------------------------------------------------
    def parse_odx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        zip_sig = b"PK\x03\x04"

        if zip_sig in content[:1024]:
            print("[PARSER] Detected PDX (ZIP)")
            return self._parse_pdx_bytes(filename, content)

        print("[PARSER] Detected plain ODX (XML)")
        root = self._parse_xml_bytes(content)
        container = self._parse_container(root)

        print(
            f"[PARSER] XML load: "
            f"ECU={len(container.ecuVariants)}, "
            f"BASE={len(container.baseVariants)}, "
            f"PROTO={len(container.protocols)}"
        )

        return filename, container

    # -----------------------------------------------------
    # XML parsing
    # -----------------------------------------------------
    def _parse_xml_bytes(self, content: bytes) -> ET.Element:
        try:
            return ET.fromstring(content)
        except ET.ParseError:
            text = content.decode("utf-8", errors="ignore")
            idx = text.find("<")
            return ET.fromstring(text[idx:].encode("utf-8"))

    def _text(self, el: ET.Element, name: str) -> str:
        for c in el:
            if local_name(c.tag) == name:
                return (c.text or "").strip()
        return ""

    # -----------------------------------------------------
    # PDX handling
    # -----------------------------------------------------
    def _parse_pdx_bytes(self, filename: str, content: bytes) -> Tuple[str, OdxContainer]:
        container = OdxContainer()

        zip_sig = b"PK\x03\x04"
        idx = content.find(zip_sig)
        if idx < 0:
            print("[PARSER] ZIP signature not found — invalid PDX")
            return filename, container

        zip_bytes = content[idx:]

        odx_count = 0

        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".odx"):
                    continue

                odx_count += 1
                try:
                    xml_bytes = zf.read(name)
                    root = self._parse_xml_bytes(xml_bytes)
                    sub = self._parse_container(root)

                    container.ecuVariants.extend(sub.ecuVariants)
                    container.baseVariants.extend(sub.baseVariants)
                    container.protocols.extend(sub.protocols)
                    container.functionalGroups.extend(sub.functionalGroups)
                    container.ecuSharedData.extend(sub.ecuSharedData)

                except Exception as e:
                    print(f"[PARSER] Failed ODX: {name} ({e})")
                    continue

        print(
            f"[PARSER] PDX summary: "
            f"ODX files={odx_count}, "
            f"ECU={len(container.ecuVariants)}, "
            f"BASE={len(container.baseVariants)}, "
            f"PROTO={len(container.protocols)}, "
            f"FG={len(container.functionalGroups)}"
        )

        return filename, container

    # -----------------------------------------------------
    # Container
    # -----------------------------------------------------
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

        svc_count = 0

        for el in layer_el.iter():
            if local_name(el.tag) == "DIAG-SERVICE":
                layer.services.append(self._parse_service(el))
                svc_count += 1

        if svc_count:
            print(f"[PARSER] Layer {layer.shortName} → services={svc_count}")

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

        for el in svc_el:
            tag = local_name(el.tag)

            if tag == "REQUEST":
                svc.request = self._parse_message(el)

            elif tag == "POS-RESPONSE":
                svc.posResponses.append(self._parse_message(el))

            elif tag == "NEG-RESPONSE":
                svc.negResponses.append(self._parse_message(el))

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

        pcount = 0

        for el in msg_el.iter():
            if local_name(el.tag) == "PARAM":
                msg.params.append(self._parse_param(el))
                pcount += 1

        if pcount:
            print(f"[PARSER] Message {msg.shortName} → params={pcount}")

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

        for el in p_el:
            tag = local_name(el.tag)

            if tag == "CODED-CONST":
                p.codedConstValue = self._text(el, "CODED-VALUE")

            elif tag == "PHYS-CONST":
                p.physConstValue = self._text(el, "V")

        p.children = []
        return p
