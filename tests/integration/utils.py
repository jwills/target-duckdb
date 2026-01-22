import os
import json


def get_db_config():
    config = {}

    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # Use MotherDuck if MOTHERDUCK_TOKEN is set, otherwise use local DuckDB file
    # --------------------------------------------------------------------------
    if os.environ.get("MOTHERDUCK_TOKEN"):
        config["path"] = "md:target_duckdb"
    else:
        config["path"] = "/tmp/target_duckdb_test.db"
    config["default_target_schema"] = "integration_test_schema"

    # --------------------------------------------------------------------------
    # The following variables needs to be empty.
    # The tests cases will set them automatically whenever it's needed
    # --------------------------------------------------------------------------
    config["disable_table_cache"] = None
    config["schema_mapping"] = None
    config["add_metadata_columns"] = None
    config["hard_delete"] = None
    config["flush_all_streams"] = None

    return config


def get_test_config():
    db_config = get_db_config()

    return db_config


def get_test_tap_lines(filename):
    lines = []
    with open(
        "{}/resources/{}".format(os.path.dirname(__file__), filename)
    ) as tap_stdout:
        for line in tap_stdout.readlines():
            lines.append(line)

    return lines
