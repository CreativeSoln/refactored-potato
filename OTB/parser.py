from __future__ import annotations
import zipfile
import xml.etree.ElementTree as ET
from typing import Tuple
from models import *


class ODXParser:

    # ------------------------------------
    def parse_odx_file(self, filename: str) -> Tuple[str, OdxContainer]:
        if filename.lower().endswith(".pdx"):
            with zipfile.ZipFile(filename, "r") as z:
                for f in z.namelist():
                    if f.lower().endswith(".odx-d") or f.lower().endswith(".odx-c"):
                        xml_text = z.read(f).decode("utf-8")
                        root = ET.fromstring(xml_text)
                        return f, self.parse_container(root)

        xml_text = open(filename, "r", encoding="utf-8").read()
        root = ET.fromstring(xml_text)
        return filename, self.parse_container(root)

    # ------------------------------------
    def parse_container(self, root: ET.Element) -> OdxContainer:
        container = OdxContainer()

        for layer in root.findall(".//DIAG-LAYER"):
            odx_layer = self._parse_layer(layer)
            ltype = layer.attrib.get("LAYER-TYPE", "")

            if ltype == "PROTOCOL":
                container.protocols.append(odx_layer)
            elif ltype == "BASE-VARIANT":
                container.baseVariants.append(odx_layer)
            elif ltype == "ECU-VARIANT":
                container.ecuVariants.append(odx_layer)
            elif ltype == "FUNCTIONAL-GROUP":
                container.functionalGroups.append(odx_layer)

        return container

    # ------------------------------------
    def _parse_layer(self, el: ET.Element) -> OdxLayer:
        layer = OdxLayer(
            layerType=el.attrib.get("LAYER-TYPE", ""),
            id=el.attrib.get("ID", ""),
            shortName=el.findtext("SHORT-NAME", ""),
            longName=el.findtext("LONG-NAME", ""),
            description=el.findtext("DESC", ""),
        )

        # Units
        for u in el.findall(".//UNIT"):
            layer.units.append(
                OdxUnit(
                    id=u.attrib.get("ID", ""),
                    shortName=u.findtext("SHORT-NAME", ""),
                )
            )

        # Services
        for svc in el.findall(".//DIAG-SERVICE"):
            layer.services.append(self._parse_service(svc))

        return layer

    # ------------------------------------
    def _parse_service(self, el: ET.Element) -> OdxService:
        svc = OdxService(
            id=el.attrib.get("ID", ""),
            shortName=el.findtext("SHORT-NAME", ""),
            semantic=el.attrib.get("SEMANTIC", "")
        )

        req = el.find("REQUEST")
        if req is not None:
            svc.request = self._parse_message(req)

        for p in el.findall("POS-RESPONSE-REFS/POS-RESPONSE-REF"):
            pass  # extend later

        return svc

    # ------------------------------------
    def _parse_message(self, el: ET.Element) -> OdxMessage:
        msg = OdxMessage(
            id=el.attrib.get("ID", ""),
            shortName=el.findtext("SHORT-NAME", "")
        )

        for p in el.findall(".//PARAM"):
            msg.params.append(
                OdxParam(
                    id=p.attrib.get("ID", ""),
                    shortName=p.findtext("SHORT-NAME", "")
                )
            )
        return msg
