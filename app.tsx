import React, {
  useState,
  useCallback,
  useMemo,
  createContext,
  useContext,
} from "react";
import JSZip from "jszip";
import * as XLSX from "xlsx";

//
// ODX DIAGNOSTIC EXPLORER
// ----------------------------------------------------------------------
// Custom Hooks + Parsing Pipeline
//

// =======================================================
// HOOK: useXMLParser
// =======================================================
const useXMLParser = () => {
  const parseXMLToDocument = useCallback((xmlString) => {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xmlString, "text/xml");
    if (doc.querySelector("parsererror"))
      throw new Error("XML Parse Error");
    return doc;
  }, []);

  const getText = useCallback((el, tag) => {
    return el?.getElementsByTagName(tag)?.[0]?.textContent?.trim() || "";
  }, []);

  const getAttr = useCallback((el, attr) => {
    return el?.getAttribute(attr) || "";
  }, []);

  const getElements = useCallback((parent, tag) => {
    return parent ? Array.from(parent.getElementsByTagName(tag)) : [];
  }, []);

  const getAllAttrs = useCallback((el) => {
    if (!el?.attributes) return {};
    const attrs = {};
    for (let i = 0; i < el.attributes.length; i++) {
      attrs[el.attributes[i].name] = el.attributes[i].value;
    }
    return attrs;
  }, []);

  return {
    parseXMLToDocument,
    getText,
    getAttr,
    getElements,
    getAllAttrs,
  };
};

// =======================================================
// HOOK: useODXParser
// =======================================================
const useODXParser = () => {
  const {
    parseXMLToDocument,
    getText,
    getAttr,
    getElements,
    getAllAttrs,
  } = useXMLParser();

  // ---------------- PARAM ------------------
  const parseParam = useCallback(
    (paramEl, parentType, parentName, layerName, serviceShortName) => {
      const attrs = getAllAttrs(paramEl);

      const codedConst = paramEl.getElementsByTagName("CODED-CONST")[0];
      const physConst = paramEl.getElementsByTagName("PHYS-CONST")[0];
      const dopRef = paramEl.getElementsByTagName("DOP-REF")[0];
      const dopSnRef = paramEl.getElementsByTagName("DOP-SNREF")[0];
      const compuRef = paramEl.getElementsByTagName("COMPU-METHOD-REF")[0];
      const diagCodedType =
        paramEl.getElementsByTagName("DIAG-CODED-TYPE")[0];
      const physType =
        paramEl.getElementsByTagName("PHYSICAL-TYPE")[0];

      const shortName = getText(paramEl, "SHORT-NAME");

      const id = `${layerName}::${serviceShortName}::${parentType}::${shortName}::${Math.random()
        .toString(36)
        .substr(2, 9)}`;

      return {
        id,
        shortName,
        longName: getText(paramEl, "LONG-NAME"),
        description: getText(paramEl, "DESC"),
        semantic: attrs["SEMANTIC"] || "",

        bytePosition: getText(paramEl, "BYTE-POSITION"),
        bitPosition: getText(paramEl, "BIT-POSITION"),

        bitLength: diagCodedType
          ? getText(diagCodedType, "BIT-LENGTH")
          : "",
        minLength: diagCodedType
          ? getText(diagCodedType, "MIN-LENGTH")
          : "",
        maxLength: diagCodedType
          ? getText(diagCodedType, "MAX-LENGTH")
          : "",

        baseDataType: diagCodedType
          ? getAttr(diagCodedType, "BASE-DATA-TYPE")
          : "",

        physicalBaseType: physType
          ? getAttr(physType, "BASE-DATA-TYPE")
          : "",

        isHighLowByteOrder: diagCodedType
          ? getAttr(diagCodedType, "IS-HIGHLOW-BYTE-ORDER")
          : "",

        codedConstValue:
          (codedConst && getText(codedConst, "CODED-VALUE")) ||
          (codedConst && getText(codedConst, "V")) ||
          (codedConst && getAttr(codedConst, "CODED-VALUE")) ||
          "",

        physConstValue: physConst ? getText(physConst, "V") : "",

        dopRefId: dopRef ? getAttr(dopRef, "ID-REF") : "",
        dopSnRefName: dopSnRef ? getAttr(dopSnRef, "SHORT-NAME") : "",
        compuMethodRefId: compuRef ? getAttr(compuRef, "ID-REF") : "",

        parentType,
        parentName,
        layerName,
        serviceShortName,

        ...attrs,
      };
    },
    [getText, getAttr, getAllAttrs]
  );

  // ---------------- UNIT ------------------
  const parseUnit = useCallback(
    (unitEl) => ({
      id: getAttr(unitEl, "ID"),
      shortName: getText(unitEl, "SHORT-NAME"),
      longName: getText(unitEl, "LONG-NAME"),
      displayName: getText(unitEl, "DISPLAY-NAME"),
      factorSiToUnit: getText(unitEl, "FACTOR-SI-TO-UNIT"),
      offsetSiToUnit: getText(unitEl, "OFFSET-SI-TO-UNIT"),
      physicalDimensionRef: getAttr(
        unitEl.getElementsByTagName("PHYSICAL-DIMENSION-REF")[0],
        "ID-REF"
      ),
    }),
    [getText, getAttr]
  );

  // ---------------- COMPU METHOD ------------------
  const parseCompuMethod = useCallback(
    (compuEl) => {
      const internalToPhys =
        compuEl.getElementsByTagName("COMPU-INTERNAL-TO-PHYS")[0];

      const scales = internalToPhys
        ? getElements(internalToPhys, "COMPU-SCALE").map((scale) => {
            const compuConst =
              scale.getElementsByTagName("COMPU-CONST")[0];
            const compuRational =
              scale.getElementsByTagName("COMPU-RATIONAL-COEFFS")[0];

            return {
              lowerLimit: getText(scale, "LOWER-LIMIT"),
              upperLimit: getText(scale, "UPPER-LIMIT"),
              compuConstV: compuConst ? getText(compuConst, "V") : "",
              compuConstVT: compuConst ? getText(compuConst, "VT") : "",

              numerators: compuRational
                ? getElements(compuRational, "NUM").map(
                    (n) => n.textContent
                  )
                : [],

              denominators: compuRational
                ? getElements(compuRational, "DEN").map(
                    (d) => d.textContent
                  )
                : [],
            };
          })
        : [];

      return {
        id: getAttr(compuEl, "ID"),
        shortName: getText(compuEl, "SHORT-NAME"),
        longName: getText(compuEl, "LONG-NAME"),
        category: getText(compuEl, "CATEGORY"),
        scales,
      };
    },
    [getText, getAttr, getElements]
  );

  // ---------------- DATA OBJECT PROP ------------------
  const parseDataObjectProp = useCallback(
    (dopEl) => {
      const diagCodedType =
        dopEl.getElementsByTagName("DIAG-CODED-TYPE")[0];
      const physType =
        dopEl.getElementsByTagName("PHYSICAL-TYPE")[0];
      const unitRef = dopEl.getElementsByTagName("UNIT-REF")[0];
      const compuMethod =
        dopEl.getElementsByTagName("COMPU-METHOD")[0];

      return {
        id: getAttr(dopEl, "ID"),
        shortName: getText(dopEl, "SHORT-NAME"),
        longName: getText(dopEl, "LONG-NAME"),
        description: getText(dopEl, "DESC"),

        baseDataType: diagCodedType
          ? getAttr(diagCodedType, "BASE-DATA-TYPE")
          : "",

        bitLength: diagCodedType
          ? getText(diagCodedType, "BIT-LENGTH")
          : "",

        physicalBaseDataType: physType
          ? getAttr(physType, "BASE-DATA-TYPE")
          : "",

        unitRefId: unitRef ? getAttr(unitRef, "ID-REF") : "",
        compuCategory: compuMethod
          ? getText(compuMethod, "CATEGORY")
          : "",
      };
    },
    [getText, getAttr]
  );

  // ---------------- DTC ------------------
  const parseDTC = useCallback(
    (dtcEl) => ({
      id: getAttr(dtcEl, "ID"),
      shortName: getText(dtcEl, "SHORT-NAME"),
      longName: getText(dtcEl, "LONG-NAME"),
      description: getText(dtcEl, "DESC"),
      troubleCode: getText(dtcEl, "TROUBLE-CODE"),
      displayTroubleCode: getText(dtcEl, "DISPLAY-TROUBLE-CODE"),
      level: getText(dtcEl, "LEVEL"),
    }),
    [getText, getAttr]
  );

  // =======================================================
  // Parse Diagnostic Layer
  // =======================================================
  const parseDiagLayer = useCallback(
    (layerEl, layerType) => {
      const attrs = getAllAttrs(layerEl);
      const shortName = getText(layerEl, "SHORT-NAME");
      const longName = getText(layerEl, "LONG-NAME");

      // ---------------- Build Maps ----------------
      const requestMap = new Map();
      const posResponseMap = new Map();
      const negResponseMap = new Map();

      // Build REQUEST map
      getElements(layerEl, "REQUEST").forEach((req) => {
        const id = getAttr(req, "ID");
        const reqShortName = getText(req, "SHORT-NAME");

        const params = getElements(req, "PARAM").map((p) =>
          parseParam(p, "REQUEST", reqShortName, shortName, "")
        );

        requestMap.set(id, {
          id,
          shortName: reqShortName,
          longName: getText(req, "LONG-NAME"),
          params,
        });
      });

      // POS RESPONSE
      getElements(layerEl, "POS-RESPONSE").forEach((res) => {
        const id = getAttr(res, "ID");
        const shortName = getText(res, "SHORT-NAME");

        const params = getElements(res, "PARAM").map((p) =>
          parseParam(p, "POS_RESPONSE", shortName, shortName, "")
        );

        posResponseMap.set(id, {
          id,
          shortName,
          longName: getText(res, "LONG-NAME"),
          params,
        });
      });

      // NEG RESPONSE
      getElements(layerEl, "NEG-RESPONSE").forEach((res) => {
        const id = getAttr(res, "ID");
        const shortName = getText(res, "SHORT-NAME");

        const params = getElements(res, "PARAM").map((p) =>
          parseParam(p, "NEG_RESPONSE", shortName, shortName, "")
        );

        negResponseMap.set(id, {
          id,
          shortName,
          longName: getText(res, "LONG-NAME"),
          params,
        });
      });

      // ---------------- Parse Services ----------------
      const services = getElements(layerEl, "DIAG-SERVICE").map((svcEl) => {
        const svcAttrs = getAllAttrs(svcEl);
        const svcShortName = getText(svcEl, "SHORT-NAME");

        const requestRefEl =
          svcEl.getElementsByTagName("REQUEST-REF")[0];

        const requestRefId = requestRefEl
          ? getAttr(requestRefEl, "ID-REF")
          : "";

        const posResponseRefIds = getElements(
          svcEl,
          "POS-RESPONSE-REF"
        ).map((r) => getAttr(r, "ID-REF"));

        const negResponseRefIds = getElements(
          svcEl,
          "NEG-RESPONSE-REF"
        ).map((r) => getAttr(r, "ID-REF"));

        const request = requestMap.get(requestRefId);
        if (request) {
          request.params = request.params.map((p) => ({
            ...p,
            serviceShortName: svcShortName,
          }));
        }

        const posResponses = posResponseRefIds
          .map((id) => {
            const res = posResponseMap.get(id);
            if (res) {
              res.params = res.params.map((p) => ({
                ...p,
                serviceShortName: svcShortName,
              }));
            }
            return res;
          })
          .filter(Boolean);

        const negResponses = negResponseRefIds
          .map((id) => {
            const res = negResponseMap.get(id);
            if (res) {
              res.params = res.params.map((p) => ({
                ...p,
                serviceShortName: svcShortName,
              }));
            }
            return res;
          })
          .filter(Boolean);

        return {
          id: svcAttrs["ID"] || "",
          shortName: svcShortName,
          longName: getText(svcEl, "LONG-NAME"),
          description: getText(svcEl, "DESC"),
          semantic: svcAttrs["SEMANTIC"] || "",
          addressing: svcAttrs["ADDRESSING"] || "",

          request,
          posResponses,
          negResponses,

          ...svcAttrs,
        };
      });

      // ---------------- Other items ----------------
      const units = [
        ...getElements(layerEl, "UNIT").map((u) => parseUnit(u)),
      ];

      const compuMethods = [
        ...getElements(layerEl, "COMPU-METHOD").map((cm) =>
          parseCompuMethod(cm)
        ),
      ];

      const dataObjectProps = [
        ...getElements(layerEl, "DATA-OBJECT-PROP").map((dop) =>
          parseDataObjectProp(dop)
        ),
      ];

      const dtcs = [
        ...getElements(layerEl, "DTC").map((dtc) => parseDTC(dtc)),
      ];

      // ---- DIAG-DATA-DICTIONARY-SPEC ----
      const diagDataDict = layerEl.getElementsByTagName(
        "DIAG-DATA-DICTIONARY-SPEC"
      )[0];

      if (diagDataDict) {
        getElements(diagDataDict, "DATA-OBJECT-PROP").forEach((dop) =>
          dataObjectProps.push(parseDataObjectProp(dop))
        );

        getElements(diagDataDict, "UNIT").forEach((u) =>
          units.push(parseUnit(u))
        );
      }

      // ---- UNIT-SPEC ----
      const unitSpec =
        layerEl.getElementsByTagName("UNIT-SPEC")[0];

      if (unitSpec) {
        getElements(unitSpec, "UNIT").forEach((u) =>
          units.push(parseUnit(u))
        );
      }

      const parentRef =
        layerEl.getElementsByTagName("PARENT-REF")[0];

      return {
        layerType,
        id: attrs["ID"] || "",
        shortName,
        longName,
        description: getText(layerEl, "DESC"),

        parentId: parentRef ? getAttr(parentRef, "ID-REF") : "",

        rxId: getText(layerEl, "RECEIVE-ID"),
        txId: getText(layerEl, "TRANSMIT-ID"),

        services,
        units,
        compuMethods,
        dataObjectProps,
        dtcs,

        ...attrs,
      };
    },
    [
      getText,
      getAttr,
      getElements,
      getAllAttrs,
      parseParam,
      parseUnit,
      parseCompuMethod,
      parseDataObjectProp,
      parseDTC,
    ]
  );

  // =======================================================
  // Parse DIAG-LAYER-CONTAINER
  // =======================================================
  const parseDiagLayerContainer = useCallback(
    (doc) => {
      const container = {
        protocols: [],
        functionalGroups: [],
        baseVariants: [],
        ecuVariants: [],
        ecuSharedData: [],
      };

      getElements(doc, "PROTOCOL").forEach((p) =>
        container.protocols.push(parseDiagLayer(p, "PROTOCOL"))
      );

      getElements(doc, "FUNCTIONAL-GROUP").forEach((fg) =>
        container.functionalGroups.push(
          parseDiagLayer(fg, "FUNCTIONAL-GROUP")
        )
      );

      getElements(doc, "BASE-VARIANT").forEach((bv) =>
        container.baseVariants.push(
          parseDiagLayer(bv, "BASE-VARIANT")
        )
      );

      getElements(doc, "ECU-VARIANT").forEach((ev) =>
        container.ecuVariants.push(
          parseDiagLayer(ev, "ECU-VARIANT")
        )
      );

      getElements(doc, "ECU-SHARED-DATA").forEach((sd) =>
        container.ecuSharedData.push(
          parseDiagLayer(sd, "ECU-SHARED-DATA")
        )
      );

      return container;
    },
    [getElements, parseDiagLayer]
  );

  // =======================================================
  // Parse ODX FILE
  // =======================================================
  const parseODXFile = useCallback(
    async (filename, content) => {
      const doc = parseXMLToDocument(content);
      return {
        filename,
        diagLayerContainers: parseDiagLayerContainer(doc),
      };
    },
    [parseXMLToDocument, parseDiagLayerContainer]
  );

  return { parseODXFile, parseDiagLayerContainer };
};

// =======================================================
// HOOK: useFileUpload
// =======================================================
const useFileUpload = () => {
  const [database, setDatabase] = useState(null);
  const [rawFiles, setRawFiles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const { parseODXFile } = useODXParser();

  const processFiles = useCallback(
    async (files) => {
      setLoading(true);
      setError(null);

      try {
        const fileList = Array.from(files);
        const allFiles = [];

        const mergedDatabase = {
          ecuVariants: [],
          baseVariants: [],
          protocols: [],
          functionalGroups: [],
          ecuSharedData: [],

          allParams: [],
          allUnits: [],
          allCompuMethods: [],
          allDataObjects: [],
          allDTCs: [],
        };

        const flattenLayer = (layer) => {
          layer.services.forEach((svc) => {
            if (svc.request) {
              svc.request.params.forEach((p) =>
                mergedDatabase.allParams.push(p)
              );
            }

            svc.posResponses.forEach((res) =>
              res.params.forEach((p) =>
                mergedDatabase.allParams.push(p)
              )
            );

            svc.negResponses.forEach((res) =>
              res.params.forEach((p) =>
                mergedDatabase.allParams.push(p)
              )
            );
          });

          layer.units.forEach((u) =>
            mergedDatabase.allUnits.push({
              ...u,
              layerName: layer.shortName,
            })
          );

          layer.compuMethods.forEach((cm) =>
            mergedDatabase.allCompuMethods.push({
              ...cm,
              layerName: layer.shortName,
            })
          );

          layer.dataObjectProps.forEach((dop) =>
            mergedDatabase.allDataObjects.push({
              ...dop,
              layerName: layer.shortName,
            })
          );

          layer.dtcs.forEach((dtc) =>
            mergedDatabase.allDTCs.push({
              ...dtc,
              layerName: layer.shortName,
            })
          );
        };

        for (const file of fileList) {
          const fileName = file.name.toLowerCase();

          // ZIP or PDX
          if (fileName.endsWith(".pdx") || fileName.endsWith(".zip")) {
            const zip = new JSZip();
            const contents = await zip.loadAsync(file);

            for (const [path, zipEntry] of Object.entries(
              contents.files
            )) {
              if (zipEntry.dir) continue;
              const lowerPath = path.toLowerCase();

              const isODX =
                lowerPath.match(/\.odx(-[a-z]+)?$/) ||
                (lowerPath.endsWith(".xml") &&
                  lowerPath.includes("index"));

              if (isODX) {
                const content = await zipEntry.async("string");
                try {
                  const parsed = await parseODXFile(path, content);

                  allFiles.push({
                    name: path,
                    source: file.name,
                    size: content.length,
                  });

                  const dlc = parsed.diagLayerContainers;

                  mergedDatabase.ecuVariants.push(...dlc.ecuVariants);
                  mergedDatabase.baseVariants.push(
                    ...dlc.baseVariants
                  );
                  mergedDatabase.protocols.push(...dlc.protocols);
                  mergedDatabase.functionalGroups.push(
                    ...dlc.functionalGroups
                  );
                  mergedDatabase.ecuSharedData.push(
                    ...dlc.ecuSharedData
                  );

                  [
                    ...dlc.ecuVariants,
                    ...dlc.baseVariants,
                    ...dlc.protocols,
                    ...dlc.functionalGroups,
                    ...dlc.ecuSharedData,
                  ].forEach(flattenLayer);
                } catch (e) {
                  console.warn(
                    `Error parsing ${path}`,
                    e
                  );
                }
              }
            }
          } else {
            // Single XML
            const content = await file.text();
            try {
              const parsed = await parseODXFile(file.name, content);

              allFiles.push({
                name: file.name,
                source: file.name,
                size: content.length,
              });

              const dlc = parsed.diagLayerContainers;

              mergedDatabase.ecuVariants.push(...dlc.ecuVariants);
              mergedDatabase.baseVariants.push(
                ...dlc.baseVariants
              );
              mergedDatabase.protocols.push(...dlc.protocols);
              mergedDatabase.functionalGroups.push(
                ...dlc.functionalGroups
              );
              mergedDatabase.ecuSharedData.push(
                ...dlc.ecuSharedData
              );

              [
                ...dlc.ecuVariants,
                ...dlc.baseVariants,
                ...dlc.protocols,
                ...dlc.functionalGroups,
                ...dlc.ecuSharedData,
              ].forEach(flattenLayer);
            } catch (e) {
              console.warn(
                `Error parsing ${file.name}`,
                e
              );
            }
          }
        }

        setRawFiles(allFiles);
        setDatabase(mergedDatabase);
      } catch (err) {
        setError(`Error: ${err.message}`);
      } finally {
        setLoading(false);
      }
    },
    [parseODXFile]
  );

  const reset = useCallback(() => {
    setDatabase(null);
    setRawFiles([]);
    setError(null);
  }, []);

  return {
    database,
    rawFiles,
    loading,
    error,
    processFiles,
    reset,
    setError,
  };
};

export { useFileUpload, useODXParser, useXMLParser };
