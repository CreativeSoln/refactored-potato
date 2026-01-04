from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any


# =========================================================
# PARAM
# =========================================================

@dataclass
class OdxParam:
    # ---- Identity / naming ----
    id: str = ""
    shortName: str = ""
    longName: str = ""
    description: str = ""
    semantic: str = ""

    # ---- Positioning / size ----
    bytePosition: str = ""
    bitPosition: str = ""
    bitLength: str = ""
    minLength: str = ""
    maxLength: str = ""

    # ---- Types ----
    baseDataType: str = ""
    physicalBaseType: str = ""
    isHighLowByteOrder: str = ""

    # ---- Constants ----
    codedConstValue: str = ""
    physConstValue: str = ""

    # ---- NEW (UI-required fields) ----
    # These were missing earlier and caused failures
    value: str = ""           # decoded or raw value
    rawHex: str = ""          # raw hex bytes
    displayHex: str = ""      # formatted hex for UI
    requestDidHex: str = ""   # DID hex for request context

    # ---- References ----
    dopRefId: str = ""
    dopSnRefName: str = ""
    compuMethodRefId: str = ""

    # ---- Hierarchy / context ----
    parentType: str = ""
    parentName: str = ""
    layerName: str = ""
    serviceShortName: str = ""

    # ---- Children ----
    children: List["OdxParam"] = field(default_factory=list)

    # ---- Attributes ----
    attrs: Dict[str, Any] = field(default_factory=dict)


# =========================================================
# MESSAGE
# =========================================================

@dataclass
class OdxMessage:
    id: str = ""
    shortName: str = ""
    longName: str = ""
    params: List[OdxParam] = field(default_factory=list)


# =========================================================
# SERVICE
# =========================================================

@dataclass
class OdxService:
    id: str = ""
    shortName: str = ""
    longName: str = ""
    description: str = ""
    semantic: str = ""
    addressing: str = ""

    # UDS
    sid: Optional[int] = None

    # Messages
    request: Optional[OdxMessage] = None
    posResponses: List[OdxMessage] = field(default_factory=list)
    negResponses: List[OdxMessage] = field(default_factory=list)

    # Attributes
    attrs: Dict[str, Any] = field(default_factory=dict)


# =========================================================
# DATA OBJECT PROP (DOP)
# =========================================================

@dataclass
class OdxDataObjectProp:
    id: str = ""
    shortName: str = ""
    longName: str = ""
    description: str = ""

    baseDataType: str = ""
    physicalBaseDataType: str = ""
    bitLength: str = ""

    unitRefId: str = ""
    compuCategory: str = ""

    # STRUCTURE support
    structureParams: List[Any] = field(default_factory=list)


# =========================================================
# UNIT
# =========================================================

@dataclass
class OdxUnit:
    id: str = ""
    shortName: str = ""
    longName: str = ""
    displayName: str = ""
    factorSiToUnit: str = ""
    offsetSiToUnit: str = ""
    physicalDimensionRef: str = ""


# =========================================================
# COMPU METHOD
# =========================================================

@dataclass
class OdxCompuScale:
    lowerLimit: str = ""
    upperLimit: str = ""
    compuConstV: str = ""
    compuConstVT: str = ""
    numerators: List[str] = field(default_factory=list)
    denominators: List[str] = field(default_factory=list)


@dataclass
class OdxTableRow:
    id: str = ""
    shortName: str = ""
    longName: str = ""
    description: str = ""
    key: str = ""
    structureRefId: str = ""


@dataclass
class OdxCompuMethod:
    id: str = ""
    shortName: str = ""
    longName: str = ""
    category: str = ""
    scales: List[OdxCompuScale] = field(default_factory=list)
    tableRows: List[OdxTableRow] = field(default_factory=list)


# =========================================================
# DTC
# =========================================================

@dataclass
class OdxDTC:
    id: str = ""
    shortName: str = ""
    longName: str = ""
    description: str = ""
    troubleCode: str = ""
    displayTroubleCode: str = ""
    level: str = ""


# =========================================================
# LAYER
# =========================================================

@dataclass
class OdxLayer:
    layerType: str = ""
    id: str = ""
    shortName: str = ""
    longName: str = ""
    description: str = ""

    parentId: str = ""
    rxId: str = ""
    txId: str = ""

    services: List[OdxService] = field(default_factory=list)
    units: List[OdxUnit] = field(default_factory=list)
    compuMethods: List[OdxCompuMethod] = field(default_factory=list)
    dataObjectProps: List[OdxDataObjectProp] = field(default_factory=list)
    dtcs: List[OdxDTC] = field(default_factory=list)

    linkedLayerIds: List[str] = field(default_factory=list)
    attrs: Dict[str, Any] = field(default_factory=dict)


# =========================================================
# CONTAINER / DATABASE
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
    ecuSharedData: List[OdxLayer] = field(default_factory=list)

    allParams: List[OdxParam] = field(default_factory=list)
    allUnits: List[Dict[str, Any]] = field(default_factory=list)
    allCompuMethods: List[Dict[str, Any]] = field(default_factory=list)
    allDataObjects: List[Dict[str, Any]] = field(default_factory=list)
    allDTCs: List[Dict[str, Any]] = field(default_factory=list)
