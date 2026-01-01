# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import xml.etree.ElementTree as ET


# =========================================================
# PARAM
# =========================================================
@dataclass
class OdxParam:
    id: str
    shortName: str
    longName: str = ""
    description: str = ""
    semantic: str = ""

    bytePosition: str = ""
    bitPosition: str = ""
    bitLength: str = ""
    minLength: str = ""
    maxLength: str = ""

    baseDataType: str = ""
    physicalBaseType: str = ""
    isHighLowByteOrder: str = ""

    codedConstValue: str = ""
    physConstValue: str = ""

    dopRefId: str = ""
    dopSnRefName: str = ""
    compuMethodRefId: str = ""

    parentType: str = ""
    parentName: str = ""

    layerName: str = ""
    serviceShortName: str = ""

    attrs: Dict[str, str] = field(default_factory=dict)
    children: List["OdxParam"] = field(default_factory=list)


# =========================================================
# UNIT
# =========================================================
@dataclass
class OdxUnit:
    id: str
    shortName: str
    longName: str = ""
    displayName: str = ""
    factorSiToUnit: str = ""
    offsetSiToUnit: str = ""
    physicalDimensionRef: str = ""


# =========================================================
# COMPU METHOD / SCALE / TABLE
# =========================================================
@dataclass
class OdxTableRow:
    id: str
    shortName: str = ""
    longName: str = ""
    description: str = ""
    key: str = ""
    structureRefId: str = ""


@dataclass
class OdxCompuScale:
    lowerLimit: str = ""
    upperLimit: str = ""
    compuConstVt: str = ""
    compuConstVT: str = ""
    numerators: List[str] = field(default_factory=list)
    denominators: List[str] = field(default_factory=list)


@dataclass
class OdxCompuMethod:
    id: str
    shortName: str
    longName: str = ""
    category: str = ""
    scales: List[OdxCompuScale] = field(default_factory=list)
    tableRows: List[OdxTableRow] = field(default_factory=list)


# =========================================================
# DATA OBJECT PROP (DOP)
# =========================================================
@dataclass
class OdxDataObjectProp:
    id: str
    shortName: str
    longName: str = ""
    description: str = ""
    baseDataType: str = ""
    bitlength: str = ""
    physicalBaseDataType: str = ""
    unitRefId: str = ""
    compuCategory: str = ""
    structureParams: List[ET.Element] = field(default_factory=list)


# =========================================================
# MESSAGE / SERVICE / LAYER
# =========================================================
@dataclass
class OdxMessage:
    id: str
    shortName: str
    longName: str = ""
    params: List[OdxParam] = field(default_factory=list)


@dataclass
class OdxService:
    id: str
    shortName: str
    longName: str = ""
    description: str = ""
    semantic: str = ""
    addressing: str = ""

    request: Optional[OdxMessage] = None
    posResponses: List[OdxMessage] = field(default_factory=list)
    negResponses: List[OdxMessage] = field(default_factory=list)

    attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class OdxLayer:
    layerType: str
    id: str
    shortName: str
    longName: str = ""
    description: str = ""

    parentId: str = ""
    rxId: str = ""
    txId: str = ""

    services: List[OdxService] = field(default_factory=list)
    units: List[OdxUnit] = field(default_factory=list)
    compuMethods: List[OdxCompuMethod] = field(default_factory=list)
    dataObjectProps: List[OdxDataObjectProp] = field(default_factory=list)

    attrs: Dict[str, str] = field(default_factory=dict)
    linkedLayerIds: List[str] = field(default_factory=list)


# =========================================================
# CONTAINER + DATABASE
# =========================================================
@dataclass
class OdxContainer:
    protocols: List[OdxLayer] = field(default_factory=list)
    functionalGroups: List[OdxLayer] = field(default_factory=list)
    baseVariants: List[OdxLayer] = field(default_factory=list)
    ecuVariants: List[OdxLayer] = field(default_factory=list)
    ecuSharedData: List[OdxLayer] = field(default_factory=list)


@dataclass
class OdxDatabase:
    ecuVariants: List[OdxLayer] = field(default_factory=list)
    baseVariants: List[OdxLayer] = field(default_factory=list)
    protocols: List[OdxLayer] = field(default_factory=list)
    functionalGroups: List[OdxLayer] = field(default_factory=list)

    allDataObjects: List[Dict[str, Any]] = field(default_factory=list)
    ecuSharedData: List[OdxLayer] = field(default_factory=list)

    allDTCs: List[Dict[str, Any]] = field(default_factory=list)
    allParams: List[OdxParam] = field(default_factory=list)
    allUnits: List[Dict[str, Any]] = field(default_factory=list)
    allCompuMethods: List[Dict[str, Any]] = field(default_factory=list)
