from error_handler import (
    logger,
    log_error,
    PARSER_HEALTH,
    ERROR_CONTROL,
    finalize_health_report
)


def run_parser(db):
    did_groups = []

    try:
        logger.info("ODX Parsing Started")

        for ecu in getattr(db, "ecus", []) or []:

            try:
                PARSER_HEALTH["totalECUsProcessed"] += 1

                for svc in getattr(ecu, "services", []) or []:
                    svc_name = getattr(svc, "short_name", "UNKNOWN")

                    try:
                        extract_normal_dids(ecu, svc, db, did_groups)
                        extract_tablekey_dids(ecu, db, did_groups)

                    except Exception as e:
                        log_error(
                            "recoverable",
                            f"ECU={ecu.short_name} Service={svc_name}",
                            "Service processing failed",
                            e
                        )

                        if ERROR_CONTROL["STOP_ON_FATAL_SERVICE_FAILURE"]:
                            raise

            except Exception as e:
                log_error(
                    "fatal",
                    f"ECU={getattr(ecu,'short_name','UNKNOWN')}",
                    "ECU processing failed",
                    e
                )

                if ERROR_CONTROL["STOP_ON_FATAL_SERVICE_FAILURE"]:
                    raise

    except Exception as e:
        log_error("fatal", "GLOBAL", "Unhandled top-level failure", e)

    finally:
        finalize_health_report()

    return did_groups
