#!/usr/bin/env python3
import argparse
import logging
import sys
from config import load_config
from logger import setup_logging
from snapshot import run_snapshot
from cdc import run_cdc, CriticalCDCError

def main():
    ap = argparse.ArgumentParser(prog="migres")
    ap.add_argument("--config", required=True, help="Path to config.yml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    setup_logging()

    mode = (cfg.get("migration", {}).get("mode") or "snapshot").lower()
    if mode == "snapshot":
        logging.info("Starting migres (snapshot) mode...")
        try:
            run_snapshot(cfg)
        except Exception:
            logging.exception("Snapshot failed:")
            sys.exit(2)
        logging.info("Snapshot finished successfully.")
    elif mode == "cdc":
        logging.info("Starting migres (CDC) mode...")
        try:
            # ptionally, run a snapshot first to build a complete baseline
            cdc_cfg = (cfg.get("migration", {}).get("cdc", {}) or {})
            snapshot_before = bool(cdc_cfg.get("snapshot_before", True))
            if snapshot_before:
                logging.info("CDC: running initial snapshot before starting binlog streaming...")
                try:
                    run_snapshot(cfg)
                except Exception:
                    logging.exception("The Initial snapshot failed before the CDC started:")
                    raise
                logging.info("CDC: initial snapshot completed, starting binlog streaming...")

            run_cdc(cfg)
        except CriticalCDCError as e:
            logging.critical("CDC failed with critical error: %s", str(e))
            sys.exit(1)  # Exit with error code 1 for critical errors
        except Exception:
            logging.exception("CDC failed:")
            sys.exit(3)
        logging.info("CDC terminated.")
    else:
        logging.error("Unknown migration.mode: %s", mode)
        sys.exit(4)
    return 0

if __name__ == "__main__":
    main()
