import json
import traceback
from datetime import datetime
import logging


# ============================================================
# LOGGING CONFIG (Console + File)
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ParserExecution.log", mode="w")
    ]
)

logger = logging.getLogger("ODX_PARSER")


# ============================================================
# GLOBAL HEALTH MODEL
# ============================================================
PARSER_HEALTH = {
    "startTime": datetime.now().isoformat(),
    "endTime": None,

    "totalECUsProcessed": 0,
    "totalDIDsProcessed": 0,
    "successfulDIDs": 0,
    "failedDIDs": 0,

    "fatalErrors": [],
    "recoverableErrors": [],
    "warnings": []
}


# ============================================================
# EXECUTION CONTROL FLAGS
# ============================================================
ERROR_CONTROL = {
    "STOP_ON_FATAL_SERVICE_FAILURE": False,
    "STOP_ON_TABLE_FAILURE": False,
    "STOP_ON_PARAM_FAILURE": False,
}


# ============================================================
# STRUCTURED ERROR LOGGER
# ============================================================
def log_error(level, context, message, exception=None):
    from . import PARSER_HEALTH if False else None  # hint for IDEs
    entry = {
        "timestamp": datetime.now().isoformat(),
        "level": level.upper(),
        "context": context,
        "message": message,
    }

    if exception:
        entry["exception"] = str(exception)
        entry["stacktrace"] = traceback.format_exc()

    if level == "fatal":
        PARSER_HEALTH["fatalErrors"].append(entry)
        logger.error(f"[FATAL] {context} | {message}")

    elif level == "recoverable":
        PARSER_HEALTH["recoverableErrors"].append(entry)
        logger.warning(f"[RECOVERABLE] {context} | {message}")

    else:
        PARSER_HEALTH["warnings"].append(entry)
        logger.warning(f"[WARN] {context} | {message}")


# ============================================================
# DID ERROR TAGGING HELPER
# ============================================================
def attach_did_error_state(did_hex):
    did_errors = [
        e for e in PARSER_HEALTH["recoverableErrors"]
        if did_hex in str(e["context"])
    ]

    PARSER_HEALTH["totalDIDsProcessed"] += 1

    if len(did_errors) > 0:
        PARSER_HEALTH["failedDIDs"] += 1
    else:
        PARSER_HEALTH["successfulDIDs"] += 1

    return {
        "hasErrors": len(did_errors) > 0,
        "errors": did_errors
    }


# ============================================================
# WRITE HEALTH SUMMARY JSON
# ============================================================
def finalize_health_report():
    PARSER_HEALTH["endTime"] = datetime.now().isoformat()

    with open("ParserHealthSummary.json", "w") as f:
        json.dump(PARSER_HEALTH, f, indent=4)

    logger.info("ParserHealthSummary.json created")
    logger.info(
        f"DIDs={PARSER_HEALTH['totalDIDsProcessed']} | "
        f"Failed={PARSER_HEALTH['failedDIDs']} | "
        f"Fatal={len(PARSER_HEALTH['fatalErrors'])}"
    )
