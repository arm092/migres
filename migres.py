#!/usr/bin/env python3
import argparse
import logging
import sys
from config import load_config
from logger import setup_logging
from snapshot import run_snapshot

def main():
    ap = argparse.ArgumentParser(prog="migres")
    ap.add_argument("--config", required=True, help="Path to config.yml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    setup_logging()

    logging.info("Starting migres (snapshot-only)...")
    try:
        run_snapshot(cfg)
    except Exception as e:
        logging.exception("migres failed:")
        sys.exit(2)
    logging.info("migres finished successfully.")
    return 0

if __name__ == "__main__":
    main()
