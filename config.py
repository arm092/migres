import yaml
import os

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # defaults
    cfg.setdefault("migration", {})
    cfg["migration"].setdefault("batch_rows", 5000)
    cfg["migration"].setdefault("mode", "snapshot")
    cfg.setdefault("checkpoint_file", "/app/binlog_checkpoint.json")
    cfg.setdefault("state_file", "/app/state.json")
    # include_tables empty => all tables
    if cfg["mysql"].get("include_tables") is None:
        cfg["mysql"]["include_tables"] = []
    return cfg
