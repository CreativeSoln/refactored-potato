import argparse
import logging
import json
import os

import odxtools
from odxtools.diaglayers.protocolraw import ProtocolRaw
from odxtools.odxlink import OdxLinkDatabase

from odx_json_exporter import OdxDataExporter


logger = logging.getLogger("CLI")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)


# ============================================================
# PATCH: SAFE COMPARAM + PROTOCOL RESOLUTION
# ============================================================
_real_resolve = OdxLinkDatabase.resolve
def _patched_resolve(self, ref, expected_type=None, *, use_weakrefs=None):
    try:
        return _real_resolve(self, ref, expected_type, use_weakrefs=use_weakrefs)
    except Exception as e:
        msg = str(e)
        if "COMPARAM" in msg or "ODXLINK" in msg or "ISO_15765" in msg:
            logger.debug(f"ODXLINK suppressed: {msg}")
            return None
        raise
OdxLinkDatabase.resolve = _patched_resolve


_real_snrefs = ProtocolRaw._resolve_snrefs
def _patched_snrefs(self, context):
    try:
        if getattr(self, "_comparan_spec", None) is None:
            return
        return _real_snrefs(self, context)
    except Exception as e:
        if "prot_stacks" in str(e):
            logger.debug("Protocol stack suppressed")
            return
        raise
ProtocolRaw._resolve_snrefs = _patched_snrefs


# ============================================================
# EXPORT FUNCTION
# ============================================================
def export_final_json(pdx_path: str, out_file: str):
    logger.info(f"Loading PDX: {pdx_path}")

    db = odxtools.load_file(pdx_path, use_weakrefs=True)

    try:
        db.refresh()
    except Exception as e:
        logger.warning(f"Database.refresh() warning ignored: {e}")

    exporter = OdxDataExporter()

    final_output = []

    ecus = getattr(db, "ecus", []) or []
    logger.info(f"Found {len(ecus)} ECU(s)")

    for ecu in ecus:
        logger.info(f"Exporting ECU: {ecu.short_name}")

        ecu_json = exporter.export_ecu(db, ecu)
        final_output.append(ecu_json)

    # --------------------------------------------------------
    # Ensure directory exists
    # --------------------------------------------------------
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2)

    logger.info(f"Export completed successfully → {out_file}")


# ============================================================
# CLI ENTRY
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="ODX JSON Enhanced Exporter"
    )

    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Input PDX/ODX file path"
    )

    parser.add_argument(
        "-o", "--output",
        default="odx_final_export.json",
        help="Output JSON filename"
    )

    args = parser.parse_args()

    export_final_json(args.input, args.output)


if __name__ == "__main__":
    main()
