const exportDiagnosticJSON = useCallback(() => {
  if (!database || !database.allParams) {
    console.warn("No database loaded");
    return;
  }

  const groups = database.allParams.map((p) => {
    return {
      service: "ReadDataByIdentifier",

      sid: "0x22",     // we can fill later properly
      did: extractDid(p), // placeholder – will refine

      direction: p.parentType === "REQUEST"
        ? "REQUEST"
        : p.parentType === "POS_RESPONSE"
          ? "RESPONSE_POS"
          : "RESPONSE_NEG",

      semantic: p.semantic || "",
      description: p.description || "",

      runtime: {
        supportsSimulation: true,
        sampleRequestHex: "",
        sampleResponseHex: "",
        decodedSample: {}
      },

      selection: buildSelection(p),

      finalParameters: [
        {
          name: p.shortName,
          path: buildPath(p),
          arrayIndex: 0,
          dataType: p.baseDataType || "",
          bitlength: Number(p.bitLength || 0),
          endianness: p.isHighLowByteOrder ? "MOTOROLA" : "INTEL",
          description: p.description || ""
        }
      ]
    };
  });

  const exportJson = {
    meta: {
      formatVersion: "1.2",
      toolName: "DiagnosticUI",
      createdDate: new Date().toISOString()
    },
    ecuInfo: {
      ecuName: "UNKNOWN",
      variant: "AUTO",
      description: "Auto exported diagnostic map"
    },
    read_did_groups: groups
  };

  const blob = new Blob(
    [JSON.stringify(exportJson, null, 2)],
    { type: "application/json" }
  );

  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "ecu_diagnostic_structure_combined.json";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);

}, [database]);
