#!/usr/bin/env python3
import argparse
import logging
import sys
import threading
import time
from config import load_config
from logger import setup_logging
from snapshot import run_snapshot
from cdc import CriticalCDCError
from pipeline_producer import PipelineProducer
from pipeline_transformer import PipelineTransformer
from pipeline_consumer import PipelineConsumer
from buffer import BufferDB
from notifications import notify_cdc_error, notify_cdc_startup, notify_cdc_info, notify_cdc_shutdown, initialize_notifications

def run_cdc_pipeline(cfg):
    """Run the 3-stage CDC pipeline in separate threads"""
    import os
    pid = os.getpid()
    logging.info(f"Starting CDC Pipeline (3-Stage Architecture)... [PID: {pid}]")
    
    # Initialize components
    producer = PipelineProducer(cfg)
    transformer = PipelineTransformer(cfg)
    consumer = PipelineConsumer(cfg)
    buffer_monitor = BufferDB()  # Separate connection for monitoring
    
    # Create threads
    producer_thread = threading.Thread(target=producer.run, name="Producer", daemon=True)
    transformer_thread = threading.Thread(target=transformer.run, name="Transformer", daemon=True)
    consumer_thread = threading.Thread(target=consumer.run, name="Consumer", daemon=True)
    
    # Start threads
    producer_thread.start()
    transformer_thread.start()
    consumer_thread.start()
    
    logging.info("All CDC pipeline stages started.")
    config_summary = {
        "MySQL": f"{cfg['mysql']['host']}:{cfg['mysql']['port']}/{cfg['mysql']['database']}",
        "ClickHouse": f"{cfg['clickhouse']['host']}:{cfg['clickhouse']['port']}/{cfg['clickhouse']['database']}",
        "Batch Delay": f"{cfg['migration']['cdc']['batch_delay_seconds']}s",
        "Mode": "CDC",
    }
    notify_cdc_startup(config_summary)
    
    last_stats_log = 0
    stats_interval = 10  # Log stats every 10 seconds
    
    try:
        while True:
            # Monitor threads
            if not producer_thread.is_alive():
                logging.critical("Producer thread died! Exiting.")
                sys.exit(1)
            if not transformer_thread.is_alive():
                logging.critical("Transformer thread died! Exiting.")
                sys.exit(1)
            if not consumer_thread.is_alive():
                logging.critical("Consumer thread died! Exiting.")
                sys.exit(1)
            
            # Periodic stats logging
            now = time.time()
            if now - last_stats_log >= stats_interval:
                try:
                    stats = buffer_monitor.get_queue_stats()
                    if stats['raw_events'] > 0 or stats['prepared_queries'] > 0:
                        logging.info(f"[QUEUE STATS] raw_events: {stats['raw_events']}, prepared_queries: {stats['prepared_queries']}")
                except Exception as e:
                    logging.warning(f"Failed to get queue stats: {e}")
                last_stats_log = now
            
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping CDC pipeline...")
        # Daemon threads will be killed when main process exits
        sys.exit(0)

def main():
    # Setup logging FIRST, before any other imports or logging calls
    setup_logging()
    
    ap = argparse.ArgumentParser(prog="migres")
    ap.add_argument("--config", required=True, help="Path to config.yml")
    args = ap.parse_args()

    try:
        cfg = load_config(args.config)
    except (IOError, OSError) as e:
        logging.error("Failed to load config file")
        notify_cdc_shutdown(f"Failed to load config file: {str(e)}")
        sys.exit(1)
    except (ValueError, KeyError) as e:
        logging.error("Invalid config file format")
        notify_cdc_shutdown(f"Invalid config file format: {str(e)}")
        sys.exit(1)

    # Initialize notifications BEFORE any notify_* calls
    initialize_notifications(
        cfg.get("notifications", {}),
        cfg.get("environment", "prod")
    )

    mode = (cfg.get("migration", {}).get("mode") or "snapshot").lower()
    # Send startup notification
    config_summary = {
        "MySQL": f"{cfg['mysql']['host']}:{cfg['mysql']['port']}/{cfg['mysql']['database']}",
        "ClickHouse": f"{cfg['clickhouse']['host']}:{cfg['clickhouse']['port']}/{cfg['clickhouse']['database']}",
        "Batch Delay": f"{cfg['migration']['cdc']['batch_delay_seconds']}s",
        "Mode": mode
    }
    notify_cdc_startup(config_summary)
    if mode == "snapshot":
        logging.info("Starting migres (snapshot) mode...")
        try:
            run_snapshot(cfg)
        except (IOError, OSError) as e:
            logging.exception("Snapshot failed due to a file or system error")
            notify_cdc_shutdown(f"Snapshot failed due to a file or system error: {str(e)}")
            sys.exit(2)
        except (ValueError, KeyError) as e:
            logging.exception("Snapshot failed due to a configuration or data error")
            notify_cdc_shutdown(f"Snapshot failed due to a configuration or data error: {str(e)}")
            sys.exit(2)
        except Exception as e:
            logging.exception("An unexpected error occurred during snapshot")
            notify_cdc_shutdown(f"An unexpected error occurred during snapshot: {str(e)}")
            sys.exit(2)
        logging.info("Snapshot finished successfully.")
        notify_cdc_info("Snapshot", "Snapshot finished successfully.")
    elif mode == "cdc":
        logging.info("Starting migres (CDC) mode...")
        try:
            cdc_cfg = (cfg.get("migration", {}).get("cdc", {}) or {})
            snapshot_before = bool(cdc_cfg.get("snapshot_before", True))
            if snapshot_before:
                logging.info("CDC: running initial snapshot before starting binlog streaming...")
                try:
                    run_snapshot(cfg)
                except (IOError, OSError) as e:
                    logging.exception("The initial snapshot failed before the CDC started due to a file or system error:")
                    notify_cdc_error("Snapshot", "N/A", f"The initial snapshot failed before the CDC started due to a file or system error: {str(e)}")
                    raise
                except (ValueError, KeyError) as e:
                    logging.exception("The initial snapshot failed before the CDC started due to a configuration or data error")
                    notify_cdc_error("Snapshot", "N/A",
                                     f"The initial snapshot failed before the CDC started due to a configuration or data error: {str(e)}")
                    raise
                except Exception as e:
                    logging.exception("An unexpected error occurred during the initial snapshot before the CDC started")
                    notify_cdc_error("Snapshot", "N/A",
                                     f"An unexpected error occurred during the initial snapshot before the CDC started: {str(e)}")
                    raise
                logging.info("CDC: initial snapshot completed, starting binlog streaming...")
                notify_cdc_info("Snapshot", "Snapshot finished successfully, starting binlog streaming...")

            # Run the new pipeline architecture
            # Legacy: run_cdc(cfg) is replaced by run_cdc_pipeline(cfg)
            run_cdc_pipeline(cfg)
            
        except CriticalCDCError as e:
            logging.critical("CDC failed with critical error: %s", str(e))
            notify_cdc_shutdown(f"CDC failed with critical error: {str(e)}")
            sys.exit(1)  # Exit with error code 1 for critical errors
        except (IOError, OSError) as e:
            logging.exception("CDC failed due to a file or system error")
            notify_cdc_shutdown(f"CDC failed due to a file or system error: {str(e)}")
            sys.exit(3)
        except (ValueError, KeyError) as e:
            logging.exception("CDC failed due to a configuration or data error")
            notify_cdc_shutdown(f"CDC failed due to a configuration or data error: {str(e)}")
            sys.exit(3)
        except Exception as e:
            logging.exception("An unexpected error occurred during CDC")
            notify_cdc_shutdown(f"An unexpected error occurred during CDC: {str(e)}")
            sys.exit(3)
        logging.info("CDC terminated.")
    else:
        logging.error("Unknown migration.mode: %s", mode)
        sys.exit(4)
    return 0

if __name__ == "__main__":
    main()
