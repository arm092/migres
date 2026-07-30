#!/usr/bin/env python3
import argparse
import logging
import sys
import threading
import time
import signal
import os
from config import load_config
from logger import setup_logging
from snapshot import run_snapshot, SnapshotError
from pipeline_producer import PipelineProducer
from pipeline_transformer import PipelineTransformer
from pipeline_consumer import PipelineConsumer
from buffer import BufferDB
from notifications import (
    notify_cdc_error, notify_cdc_startup, notify_cdc_info,
    notify_cdc_shutdown, initialize_notifications,
)
from schema_and_ddl import quote_ident

# Global context for signal handlers / watchdog
_reset_context = {}
_signal_flags = {
    "reset": threading.Event(),
    "reposition": threading.Event(),
}


def _cleanup_reset_artifacts(cfg, buffer_monitor=None):
    """Best-effort cleanup used by reset handler: drop migres tables only, delete local state."""
    logging.info("Dropping migres ClickHouse tables...")
    from clickhouse_client import CHClient
    ch_client = CHClient(cfg["clickhouse"], cfg.get("migration", {}))
    db_name = cfg["clickhouse"]["database"]
    qdb = quote_ident(db_name)

    try:
        tables = ch_client.list_migres_tables()
        dropped_count = 0
        for table_name in tables:
            try:
                logging.info(f"Dropping migres table: {table_name}")
                ch_client.execute(f"DROP TABLE IF EXISTS {qdb}.{quote_ident(table_name)}")
                dropped_count += 1
            except Exception as e:
                logging.error(f"Error dropping table {table_name}: {e}")
        logging.info(f"Dropped {dropped_count} migres tables")
    except Exception as e:
        logging.error(f"Error listing/dropping tables: {e}")

    logging.info("Deleting buffer database...")
    try:
        if buffer_monitor is not None:
            buffer_path = buffer_monitor.db_path
        else:
            tmp_buf = BufferDB(db_debug=False, cfg=cfg)
            buffer_path = tmp_buf.db_path
        if os.path.exists(buffer_path):
            os.remove(buffer_path)
            logging.info(f"Deleted buffer database: {buffer_path}")
        else:
            logging.info(f"Buffer database does not exist: {buffer_path}")
    except Exception as e:
        logging.error(f"Error deleting buffer database: {e}")

    logging.info("Deleting state file...")
    try:
        state_file = cfg.get("state_file")
        if state_file and os.path.exists(state_file):
            os.remove(state_file)
            logging.info(f"Deleted state file: {state_file}")
        else:
            logging.info(f"State file does not exist: {state_file}")
    except Exception as e:
        logging.error(f"Error deleting state file: {e}")


def _signal_reset(signum, frame):
    logging.info("Received reset signal (SIGUSR1). Queuing reset...")
    _signal_flags["reset"].set()


def _signal_reposition(signum, frame):
    logging.info("Received reposition signal (SIGUSR2). Queuing reposition...")
    _signal_flags["reposition"].set()


def _shutdown_pipeline_threads(timeout=30):
    producer = _reset_context.get('producer')
    transformer = _reset_context.get('transformer')
    consumer = _reset_context.get('consumer')
    producer_thread = _reset_context.get('producer_thread')
    transformer_thread = _reset_context.get('transformer_thread')
    consumer_thread = _reset_context.get('consumer_thread')

    if not all([producer, transformer, consumer, producer_thread, transformer_thread, consumer_thread]):
        logging.info("Pipeline threads not fully initialized")
        return

    logging.info("Signaling all threads to shutdown...")
    producer._shutdown_flag.set()
    transformer._shutdown_flag.set()
    consumer._shutdown_flag.set()

    logging.info("Waiting for threads to finish...")
    producer_thread.join(timeout=timeout)
    transformer_thread.join(timeout=timeout)
    consumer_thread.join(timeout=timeout)

    if producer_thread.is_alive() or transformer_thread.is_alive() or consumer_thread.is_alive():
        logging.warning("Some threads did not finish within timeout")


def _start_pipeline_threads(cfg):
    producer = PipelineProducer(cfg)
    transformer = PipelineTransformer(cfg)
    consumer = PipelineConsumer(cfg)
    cdc_cfg = cfg.get("migration", {}).get("cdc", {})
    buffer_monitor = BufferDB(db_debug=cdc_cfg.get("db_debug", False), cfg=cfg)

    producer_thread = threading.Thread(target=producer.run, name="Producer", daemon=True)
    transformer_thread = threading.Thread(target=transformer.run, name="Transformer", daemon=True)
    consumer_thread = threading.Thread(target=consumer.run, name="Consumer", daemon=True)

    producer_thread.start()
    transformer_thread.start()
    consumer_thread.start()

    _reset_context.update({
        'producer': producer,
        'transformer': transformer,
        'consumer': consumer,
        'cfg': cfg,
        'buffer_monitor': buffer_monitor,
        'producer_thread': producer_thread,
        'transformer_thread': transformer_thread,
        'consumer_thread': consumer_thread,
    })
    return producer_thread, transformer_thread, consumer_thread


def _perform_reset():
    cfg = _reset_context.get('cfg')
    buffer_monitor = _reset_context.get('buffer_monitor')
    if not cfg:
        logging.error("Reset requested but config is not initialized")
        os._exit(1)
    try:
        _shutdown_pipeline_threads(timeout=30)
        _cleanup_reset_artifacts(cfg, buffer_monitor=buffer_monitor)
        logging.info("Reset completed. Exiting immediately...")
        os._exit(0)
    except Exception as e:
        logging.exception(f"Error during reset: {e}")
        os._exit(1)


def _perform_reposition():
    cfg = _reset_context.get('cfg')
    buffer_monitor = _reset_context.get('buffer_monitor')
    if not all([cfg, buffer_monitor]):
        logging.error("Reposition requested but context not initialized")
        return

    try:
        cdc_cfg = cfg.get("migration", {}).get("cdc", {})
        force_pos = cdc_cfg.get("force_binlog_position")

        if not force_pos:
            logging.warning("SIGUSR2 received but force_binlog_position is not set, skipping reposition")
            return

        if ":" not in force_pos:
            logging.error("force_binlog_position must be in format 'file:position'")
            return

        file, pos_str = force_pos.split(":", 1)
        try:
            pos = int(pos_str)
        except ValueError:
            logging.error(f"Invalid position in force_binlog_position: {pos_str}")
            return

        logging.info(f"Repositioning to binlog position: {file}:{pos}")
        _shutdown_pipeline_threads(timeout=60)

        logging.info("Deleting buffer database...")
        try:
            buffer_path = buffer_monitor.db_path
            if os.path.exists(buffer_path):
                os.remove(buffer_path)
                logging.info(f"Deleted buffer database: {buffer_path}")
        except Exception as e:
            logging.error(f"Error deleting buffer database: {e}")
            return

        logging.info("Updating state.json with new binlog position...")
        try:
            from state_json import StateJson
            state = StateJson(cfg.get("state_file"))
            state.set_binlog(file, pos)
            logging.info(f"Updated state.json with binlog position: {file}:{pos}")
        except Exception as e:
            logging.error(f"Error updating state.json: {e}")
            return

        logging.info("Restarting all pipeline threads...")
        _start_pipeline_threads(cfg)
        logging.info("Reposition completed successfully. All threads restarted.")

    except Exception as e:
        logging.exception(f"Error during reposition: {e}")


def run_cdc_pipeline(cfg):
    """Run the 3-stage CDC pipeline in separate threads"""
    pid = os.getpid()
    logging.info(f"Starting CDC Pipeline (3-Stage Architecture)... [PID: {pid}]")

    _start_pipeline_threads(cfg)

    logging.info("All CDC pipeline stages started.")
    config_summary = {
        "MySQL": f"{cfg['mysql']['host']}:{cfg['mysql']['port']}/{cfg['mysql']['database']}",
        "ClickHouse": f"{cfg['clickhouse']['host']}:{cfg['clickhouse']['port']}/{cfg['clickhouse']['database']}",
        "Batch Delay": f"{cfg['migration']['cdc']['batch_delay_seconds']}s",
        "Mode": "CDC",
    }
    notify_cdc_startup(config_summary, "cdc")

    last_stats_log = 0
    last_idle_log = 0
    stats_interval = 10 if cfg.get("migration", {}).get("debug", False) else 60
    idle_interval = 60

    try:
        while True:
            if _signal_flags["reset"].is_set():
                _signal_flags["reset"].clear()
                _perform_reset()
            if _signal_flags["reposition"].is_set():
                _signal_flags["reposition"].clear()
                _perform_reposition()

            current_producer_thread = _reset_context.get('producer_thread')
            current_transformer_thread = _reset_context.get('transformer_thread')
            current_consumer_thread = _reset_context.get('consumer_thread')
            current_producer = _reset_context.get('producer')
            current_transformer = _reset_context.get('transformer')
            current_consumer = _reset_context.get('consumer')
            current_buffer_monitor = _reset_context.get('buffer_monitor')

            if not current_producer_thread or not current_producer_thread.is_alive():
                logging.critical("Producer thread died! Shutting down pipeline.")
                notify_cdc_shutdown("Producer thread died")
                sys.exit(1)
            if not current_transformer_thread or not current_transformer_thread.is_alive():
                logging.critical("Transformer thread died! Shutting down pipeline.")
                notify_cdc_shutdown("Transformer thread died")
                sys.exit(1)
            if not current_consumer_thread or not current_consumer_thread.is_alive():
                logging.critical("Consumer thread died! Shutting down pipeline gracefully.")
                if current_producer:
                    current_producer._shutdown_flag.set()
                if current_transformer:
                    current_transformer._shutdown_flag.set()
                if current_consumer:
                    current_consumer._shutdown_flag.set()

                logging.info("Waiting for producer and transformer to finish current operations...")
                if current_producer_thread:
                    current_producer_thread.join(timeout=30)
                if current_transformer_thread:
                    current_transformer_thread.join(timeout=30)

                notify_cdc_shutdown("Consumer thread died - queries remain in prepared_queries for retry")
                sys.exit(1)

            now = time.time()
            debug_mode = cfg.get("migration", {}).get("debug", False)

            if debug_mode:
                if now - last_stats_log >= stats_interval:
                    try:
                        if current_buffer_monitor:
                            stats = current_buffer_monitor.get_queue_stats()
                            if stats['raw_events'] > 0 or stats['prepared_queries'] > 0 or stats.get('failed_queries', 0) > 0:
                                logging.info(
                                    f"[QUEUE STATS] raw_events: {stats['raw_events']}, "
                                    f"prepared_queries: {stats['prepared_queries']}, "
                                    f"failed_queries: {stats.get('failed_queries', 0)}"
                                )
                    except Exception as e:
                        logging.warning(f"Failed to get queue stats: {e}")
                    last_stats_log = now
            else:
                if now - last_idle_log >= idle_interval:
                    try:
                        if current_buffer_monitor:
                            stats = current_buffer_monitor.get_queue_stats()
                            if stats['raw_events'] == 0 and stats['prepared_queries'] == 0:
                                logging.info(
                                    f"[IDLE] CDC pipeline running - queues empty "
                                    f"(failed_queries: {stats.get('failed_queries', 0)})"
                                )
                            else:
                                logging.info(
                                    f"[STATUS] raw_events: {stats['raw_events']}, "
                                    f"prepared_queries: {stats['prepared_queries']}, "
                                    f"failed_queries: {stats.get('failed_queries', 0)}"
                                )
                    except Exception as e:
                        logging.warning(f"Failed to get queue stats: {e}")
                    last_idle_log = now

            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping CDC pipeline...")
        sys.exit(0)


def main():
    setup_logging()

    ap = argparse.ArgumentParser(prog="migres")
    ap.add_argument("--config", required=True, help="Path to config.yml")
    args = ap.parse_args()

    try:
        cfg = load_config(args.config)
    except (IOError, OSError) as e:
        logging.error("Failed to load config file: %s", e)
        sys.exit(1)
    except (ValueError, KeyError) as e:
        logging.error("Invalid config file format: %s", e)
        sys.exit(1)

    _reset_context.update({
        'cfg': cfg,
        'buffer_monitor': BufferDB(db_debug=False, cfg=cfg)
    })

    # Register signal handlers once (set flags only; work happens in main loop)
    try:
        signal.signal(signal.SIGUSR1, _signal_reset)
        logging.info("Reset signal handler registered (SIGUSR1)")
    except (AttributeError, OSError) as e:
        logging.warning(f"Cannot register SIGUSR1 handler (not available on this platform): {e}")
    try:
        signal.signal(signal.SIGUSR2, _signal_reposition)
        logging.info("Reposition signal handler registered (SIGUSR2)")
    except (AttributeError, OSError) as e:
        logging.warning(f"Cannot register SIGUSR2 handler (not available on this platform): {e}")

    initialize_notifications(
        cfg.get("notifications", {}),
        cfg.get("environment", "prod")
    )

    # Process reset even during snapshot phase
    if _signal_flags["reset"].is_set():
        _perform_reset()

    mode = (cfg.get("migration", {}).get("mode") or "snapshot").lower()
    config_summary = {
        "MySQL": f"{cfg['mysql']['host']}:{cfg['mysql']['port']}/{cfg['mysql']['database']}",
        "ClickHouse": f"{cfg['clickhouse']['host']}:{cfg['clickhouse']['port']}/{cfg['clickhouse']['database']}",
        "Batch Delay": f"{cfg['migration']['cdc']['batch_delay_seconds']}s",
        "Mode": mode
    }

    if mode == "snapshot":
        logging.info("Starting migres (snapshot) mode...")
        try:
            notify_cdc_startup(config_summary, 'snapshot')
            if _signal_flags["reset"].is_set():
                _perform_reset()
            run_snapshot(cfg)
        except SnapshotError as e:
            logging.exception("Snapshot failed: %s", e)
            notify_cdc_shutdown(f"Snapshot failed: {str(e)}")
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
                if cfg.get("migration", {}).get("debug", False):
                    logging.info("CDC: running initial snapshot before starting binlog streaming...")
                try:
                    notify_cdc_startup(config_summary, 'snapshot')
                    if _signal_flags["reset"].is_set():
                        _perform_reset()
                    run_snapshot(cfg)
                except SnapshotError as e:
                    logging.exception("The initial snapshot failed before the CDC started: %s", e)
                    notify_cdc_error("Snapshot", "N/A", str(e), exc=e)
                    raise
                except Exception as e:
                    logging.exception("An unexpected error occurred during the initial snapshot before the CDC started")
                    notify_cdc_error("Snapshot", "N/A", str(e), exc=e)
                    raise
                if cfg.get("migration", {}).get("debug", False):
                    logging.info("CDC: initial snapshot completed, starting binlog streaming...")
                notify_cdc_info("Snapshot", "Snapshot finished successfully, starting binlog streaming...")

            run_cdc_pipeline(cfg)

        except SnapshotError as e:
            logging.critical("CDC failed: snapshot error: %s", str(e))
            notify_cdc_shutdown(f"CDC failed: snapshot error: {str(e)}")
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
