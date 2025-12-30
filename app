//
// src/parsers/odxParsers.js
//

export const buildStableParamId = ({ layerId, serviceId, messageKind, parentName, paramIndex, paramShortName }) => (
  [layerId || 'layer', serviceId || 'service', messageKind || 'MSG', parentName || 'parent', String(paramIndex ?? 0), paramShortName || 'param'].join('::')
);

// ============================================================================
// UNIT
// ============================================================================
export const parseUnit = (unitEl, h) => {
  const dimRefEl = h.getFirstNS(unitEl, 'UNIT-PHYSICAL-DIMENSION-REF') || h.getFirstNS(unitEl, 'PHYSICAL-DIMENSION-REF');

  return {
    id: h.getAttr(unitEl, 'ID') || h.getAttr(unitEl, 'id'),
    shortName: h.getText(unitEl, 'SHORT-NAME'),
    longName: h.getText(unitEl, 'LONG-NAME'),
    displayName: h.getText(unitEl, 'DISPLAY-NAME'),
    factorSiToUnit: h.getText(unitEl, 'FACTOR-SI-TO-UNIT'),
    offsetSiToUnit: h.getText(unitEl, 'OFFSET-SI-TO-UNIT'),
    physicalDimensionRefId: dimRefEl ? (h.getAttr(dimRefEl, 'ID-REF') || h.getAttr(dimRefEl, 'id-ref')) : '',
  };
};

// ============================================================================
// COMPU-METHOD
// ============================================================================
export const parseCompuMethod = (compuEl, h) => {
  const itp = h.getFirstNS(compuEl, 'COMPU-INTERNAL-TO-PHYS');

  const scales = itp
    ? h.getElementsNS(itp, 'COMPU-SCALE').map(scale => {
      const compuConst = h.getFirstNS(scale, 'COMPU-CONST');
      const compuRat = h.getFirstNS(scale, 'COMPU-RATIONAL-COEFFS');

      const nums = compuRat ? h.getElementsNS(compuRat, 'NUM').map(n => n.textContent) : [];
      const dens = compuRat ? h.getElementsNS(compuRat, 'DEN').map(d => d.textContent) : [];

      return {
        lowerLimit: h.getText(scale, 'LOWER-LIMIT'),
        upperLimit: h.getText(scale, 'UPPER-LIMIT'),
        compuConstV: compuConst ? h.getText(compuConst, 'V') : '',
        compuConstVT: compuConst ? h.getText(compuConst, 'VT') : '',
        numerators: nums,
        denominators: dens
      };
    })
    : [];

  return {
    id: h.getAttr(compuEl, 'ID') || h.getAttr(compuEl, 'id'),
    shortName: h.getText(compuEl, 'SHORT-NAME'),
    longName: h.getText(compuEl, 'LONG-NAME'),
    category: h.getText(compuEl, 'CATEGORY'),
    scales,
  };
};

// ============================================================================
// DATA OBJECT PROP
// ============================================================================
export const parseDataObjectProp = (dopEl, h) => {
  const diagCodedType = h.getFirstNS(dopEl, 'DIAG-CODED-TYPE');
  const physType = h.getFirstNS(dopEl, 'PHYSICAL-TYPE');
  const unitRef = h.getFirstNS(dopEl, 'UNIT-REF');

  return {
    id: h.getAttr(dopEl, 'ID') || h.getAttr(dopEl, 'id'),
    shortName: h.getText(dopEl, 'SHORT-NAME'),
    longName: h.getText(dopEl, 'LONG-NAME'),
    description: h.getText(dopEl, 'DESC'),

    baseDataType: diagCodedType ? (h.getAttr(diagCodedType, 'BASE-DATA-TYPE') || h.getAttr(diagCodedType, 'base-data-type')) : '',
    bitLength: diagCodedType ? h.getText(diagCodedType, 'BIT-LENGTH') : '',
    physicalBaseDataType: physType ? h.getAttr(physType, 'BASE-DATA-TYPE') || h.getAttr(physType, 'base-data-type') : '',

    unitRefId: unitRef ? (h.getAttr(unitRef, 'ID-REF') || h.getAttr(unitRef, 'id-ref')) : '',
  };
};

// ============================================================================
// DTC
// ============================================================================
export const parseDTC = (dtcEl, h) => ({
  id: h.getAttr(dtcEl, 'ID') || h.getAttr(dtcEl, 'id'),
  shortName: h.getText(dtcEl, 'SHORT-NAME'),
  longName: h.getText(dtcEl, 'LONG-NAME'),
  description: h.getText(dtcEl, 'DESC'),
  troubleCode: h.getText(dtcEl, 'TROUBLE-CODE'),
  displayTroubleCode: h.getText(dtcEl, 'DISPLAY-TROUBLE-CODE'),
  level: h.getText(dtcEl, 'LEVEL'),
});

// ============================================================================
// PARAM  (FINAL STRUCTURE SUPPORT - CORRECTED ODX LOGIC)
// ============================================================================
export const parseParam = (paramEl, ctx, idIndex, h) => {
  const { layerId, layerShortName, serviceId, serviceShortName, messageKind, parentName, paramIndex } = ctx;

  const attrs = h.getAllAttrs(paramEl);
  const codedConst = h.getFirstNS(paramEl, 'CODED-CONST');
  const physConst = h.getFirstNS(paramEl, 'PHYS-CONST');
  const dopRefEl = h.getFirstNS(paramEl, 'DOP-REF');
  const dopSnRef = h.getFirstNS(paramEl, 'DOP-SNREF');
  const compuRef = h.getFirstNS(paramEl, 'COMPU-METHOD-REF');
  const diagCodedType = h.getFirstNS(paramEl, 'DIAG-CODED-TYPE');
  const physType = h.getFirstNS(paramEl, 'PHYSICAL-TYPE');
  const shortName = h.getText(paramEl, 'SHORT-NAME');

  const id = buildStableParamId({
    layerId,
    serviceId,
    messageKind,
    parentName,
    paramIndex,
    paramShortName: shortName
  });

  // ======================================================================
  // TRUE STRUCTURE RESOLUTION (ODX REALITY)
  // ======================================================================
  let structureEl = null;
  let structureChildren = [];
  let dopNode = null;

  // --- Resolve DOP via ID ---
  if (dopRefEl) {
    const refId = h.getAttr(dopRefEl, 'ID-REF') || h.getAttr(dopRefEl, 'id-ref');
    if (refId) dopNode = idIndex.get(refId);
  }

  // --- Resolve DOP via Short Name fallback ---
  if (!dopNode && dopSnRef) {
    const sn = h.getText(dopSnRef, 'SHORT-NAME');
    for (const el of idIndex.values()) {
      if (h.getText(el, 'SHORT-NAME') === sn) {
        dopNode = el;
        break;
      }
    }
  }

  // --- Locate STRUCTURE node ---
  if (dopNode) {
    structureEl = h.getFirstNS(dopNode, 'STRUCTURE');

    if (!structureEl) {
      const structRef = h.getFirstNS(dopNode, 'STRUCTURE-REF');
      if (structRef) {
        const sid = h.getAttr(structRef, 'ID-REF') || h.getAttr(structRef, 'id-ref');
        if (sid) structureEl = idIndex.get(sid);
      }
    }
  }

  // --- Extract STRUCTURE → PARAMS → PARAM ---
  if (structureEl) {
    const paramsNode = h.getFirstNS(structureEl, 'PARAMS');
    if (paramsNode) {
      const structParams = h.getElementsNS(paramsNode, 'PARAM');

      structureChildren = structParams.map((childEl, idx) =>
        parseParam(
          childEl,
          {
            layerId,
            layerShortName,
            serviceId,
            serviceShortName,
            messageKind: 'STRUCTURE',
            parentName: shortName,
            paramIndex: idx
          },
          idIndex,
          h
        )
      );
    }
  }

  // ======================================================================

  const p = {
    id,
    shortName,
    longName: h.getText(paramEl, 'LONG-NAME'),
    description: h.getText(paramEl, 'DESC'),
    semantic: attrs['SEMANTIC'] || attrs['semantic'] || '',
    bytePosition: h.getText(paramEl, 'BYTE-POSITION'),

    bitPosition: h.getText(paramEl, 'BIT-POSITION'),
    bitLength: diagCodedType ? h.getText(diagCodedType, 'BIT-LENGTH') : '',
    minLength: diagCodedType ? h.getText(diagCodedType, 'MIN-LENGTH') : '',
    maxLength: diagCodedType ? h.getText(diagCodedType, 'MAX-LENGTH') : '',

    baseDataType: diagCodedType ? (h.getAttr(diagCodedType, 'BASE-DATA-TYPE') || h.getAttr(diagCodedType, 'base-data-type')) : '',
    physicalBaseType: physType ? (h.getAttr(physType, 'BASE-DATA-TYPE') || h.getAttr(physType, 'base-data-type')) : '',

    isHighLowByteOrder: diagCodedType ? h.getAttr(diagCodedType, 'IS-HIGHLOW-BYTE-ORDER') || h.getAttr(diagCodedType, 'is-highlow-byte-order') : '',

    codedConstValue: codedConst ? (h.getText(codedConst, 'CODED-VALUE') || h.getAttr(codedConst, 'CODED-VALUE')) : '',
    physConstValue: physConst ? h.getText(physConst, 'V') : '',

    dopRefId: dopRefEl ? (h.getAttr(dopRefEl, 'ID-REF') || h.getAttr(dopRefEl, 'id-ref')) : '',
    dopSnRefName: dopSnRef ? h.getAttr(dopSnRef, 'SHORT-NAME') || h.getAttr(dopSnRef, 'short-name') : '',

    compuMethodRefId: compuRef ? (h.getAttr(compuRef, 'ID-REF') || h.getAttr(compuRef, 'id-ref')) : '',

    parentType: messageKind,
    parentName,
    layerName: layerShortName,
    serviceShortName,

    children: structureChildren,

    ...attrs,
  };

  return p;
};

// ============================================================================
// DIAG LAYER
// ============================================================================
export const parseDiagLayer = (layerEl, layerType, idIndex, h) => {
  const attrs = h.getAllAttrs(layerEl);
  const layerShortName = h.getText(layerEl, 'SHORT-NAME');
  const layerLongName = h.getText(layerEl, 'LONG-NAME');
  const layerId = attrs['ID'] || attrs['id'] || '';
  const parentRefEl = h.getFirstNS(layerEl, 'PARENT-REF');

  const requestMap = new Map();
  const posMap = new Map();
  const negMap = new Map();

  h.getElementsNS(layerEl, 'REQUEST').forEach(reqEl => {
    const id = h.getAttr(reqEl, 'ID') || h.getAttr(reqEl, 'id');
    const reqShortName = h.getText(reqEl, 'SHORT-NAME');

    const params = h.getElementsNS(reqEl, 'PARAM').map((pEl, idx) =>
      parseParam(pEl, { layerId, layerShortName, serviceId: '', serviceShortName: '', messageKind: 'REQUEST', parentName: reqShortName, paramIndex: idx }, idIndex, h)
    );

    requestMap.set(id, {
      id,
      shortName: reqShortName,
      longName: h.getText(reqEl, 'LONG-NAME'),
      params
    });
  });

  h.getElementsNS(layerEl, 'POS-RESPONSE').forEach(resEl => {
    const id = h.getAttr(resEl, 'ID') || h.getAttr(resEl, 'id');
    const resShortName = h.getText(resEl, 'SHORT-NAME');

    const params = h.getElementsNS(resEl, 'PARAM').map((pEl, idx) =>
      parseParam(pEl, { layerId, layerShortName, serviceId: '', serviceShortName: '', messageKind: 'POS_RESPONSE', parentName: resShortName, paramIndex: idx }, idIndex, h)
    );

    posMap.set(id, {
      id,
      shortName: resShortName,
      longName: h.getText(resEl, 'LONG-NAME'),
      params
    });
  });

  h.getElementsNS(layerEl, 'NEG-RESPONSE').forEach(resEl => {
    const id = h.getAttr(resEl, 'ID') || h.getAttr(resEl, 'id');
    const resShortName = h.getText(resEl, 'SHORT-NAME');

    const params = h.getElementsNS(resEl, 'PARAM').map((pEl, idx) =>
      parseParam(pEl, { layerId, layerShortName, serviceId: '', serviceShortName: '', messageKind: 'NEG_RESPONSE', parentName: resShortName, paramIndex: idx }, idIndex, h)
    );

    negMap.set(id, {
      id,
      shortName: resShortName,
      longName: h.getText(resEl, 'LONG-NAME'),
      params
    });
  });

  const services = h.getElementsNS(layerEl, 'DIAG-SERVICE').map(svcEl => {
    const svcAttrs = h.getAllAttrs(svcEl);
    const svcShortName = h.getText(svcEl, 'SHORT-NAME');
    const svcId = svcAttrs['ID'] || svcAttrs['id'] || '';

    const requestRefId = (h.getAttr(h.getFirstNS(svcEl, 'REQUEST-REF'), 'ID-REF') || h.getAttr(h.getFirstNS(svcEl, 'REQUEST-REF'), 'id-ref')) || '';
    const posResponseRefIds = h.getElementsNS(svcEl, 'POS-RESPONSE-REF').map(r => h.getAttr(r, 'ID-REF') || h.getAttr(r, 'id-ref'));
    const negResponseRefIds = h.getElementsNS(svcEl, 'NEG-RESPONSE-REF').map(r => h.getAttr(r, 'ID-REF') || h.getAttr(r, 'id-ref'));

    const request = requestMap.get(requestRefId) || null;
    const posResponses = posResponseRefIds.map(id => posMap.get(id)).filter(Boolean);
    const negResponses = negResponseRefIds.map(id => negMap.get(id)).filter(Boolean);

    const stampParams = (msg, msgKind) =>
      msg
        ? {
          ...msg,
          params: msg.params.map((p, i) => ({
            ...p,
            id: buildStableParamId({
              layerId,
              serviceId: svcId,
              messageKind: msgKind,
              parentName: msg.shortName,
              paramIndex: i,
              paramShortName: p.shortName,
              serviceShortName: svcShortName,
            }),
          })),
        }
        : null;

    return {
      id: svcId,
      shortName: svcShortName,
      longName: h.getText(svcEl, 'LONG-NAME'),
      description: h.getText(svcEl, 'DESC'),
      semantic: svcAttrs['SEMANTIC'] || svcAttrs['semantic'] || '',
      addressing: svcAttrs['ADDRESSING'] || svcAttrs['addressing'] || '',
      request: stampParams(request, 'REQUEST'),
      posResponses: posResponses.map(r => stampParams(r, 'POS_RESPONSE')).filter(Boolean),
      negResponses: negResponses.map(r => stampParams(r, 'NEG_RESPONSE')).filter(Boolean),
      ...svcAttrs,
    };
  });

  const units = [];
  const compuMethods = [];
  const dataObjectProps = [];
  const dtcs = h.getElementsNS(layerEl, 'DTC').map(dtc => parseDTC(dtc, h));

  const diagDataDict = h.getFirstNS(layerEl, 'DIAG-DATA-DICTIONARY-SPEC');
  if (diagDataDict) {
    h.getElementsNS(diagDataDict, 'DATA-OBJECT-PROP').forEach(dop => dataObjectProps.push(parseDataObjectProp(dop, h)));
    h.getElementsNS(diagDataDict, 'UNIT').forEach(u => units.push(parseUnit(u, h)));
  }

  const unitSpec = h.getFirstNS(layerEl, 'UNIT-SPEC');
  if (unitSpec) h.getElementsNS(unitSpec, 'UNIT').forEach(u => units.push(parseUnit(u, h)));

  return {
    layerType: attrs['DIAG-LAYER-TYPE'] || attrs['TYPE'] || attrs['diag-layer-type'] || attrs['type'] || layerType,
    id: layerId,
    shortName: layerShortName,
    longName: layerLongName,
    description: h.getText(layerEl, 'DESC'),
    parentId: parentRefEl ? (h.getAttr(parentRefEl, 'ID-REF') || h.getAttr(parentRefEl, 'id-ref')) : '',
    rxId: h.getText(layerEl, 'RECEIVE-ID'),
    txId: h.getText(layerEl, 'TRANSMIT-ID'),

    services,
    units,
    compuMethods,
    dataObjectProps,
    dtcs,

    ...attrs,
  };
};

// ============================================================================
// DIAG-LAYER-CONTAINER (your screenshot style)
// ============================================================================
export const parseDiagLayerContainer = (doc, h, idIndex) => {

  if (!idIndex) idIndex = new Map();

  const registerIds = (tag) => {
    h.getElementsNS(doc, tag).forEach(el => {
      const id = h.getAttr(el, 'ID') || h.getAttr(el, 'id');
      if (id) idIndex.set(id, el);
    });
  };

  registerIds('STRUCTURE');
  registerIds('DATA-OBJECT-PROP');
  registerIds('UNIT');
  registerIds('COMPU-METHOD');
  registerIds('REQUEST');
  registerIds('POS-RESPONSE');
  registerIds('NEG-RESPONSE');
  registerIds('DIAG-SERVICE');
  registerIds('DIAG-LAYER');

  const container = {
    protocols: [],
    functionalGroups: [],
    baseVariants: [],
    ecuVariants: [],
    ecuSharedData: []
  };

  h.getElementsNS(doc, 'PROTOCOL')
    .forEach(p => container.protocols.push(parseDiagLayer(p, 'PROTOCOL', idIndex, h)));

  h.getElementsNS(doc, 'FUNCTIONAL-GROUP')
    .forEach(fg => container.functionalGroups.push(parseDiagLayer(fg, 'FUNCTIONAL-GROUP', idIndex, h)));

  h.getElementsNS(doc, 'BASE-VARIANT')
    .forEach(bv => container.baseVariants.push(parseDiagLayer(bv, 'BASE-VARIANT', idIndex, h)));

  h.getElementsNS(doc, 'ECU-VARIANT')
    .forEach(ev => container.ecuVariants.push(parseDiagLayer(ev, 'ECU-VARIANT', idIndex, h)));

  h.getElementsNS(doc, 'ECU-SHARED-DATA')
    .forEach(sd => container.ecuSharedData.push(parseDiagLayer(sd, 'ECU-SHARED-DATA', idIndex, h)));

  return container;
};
