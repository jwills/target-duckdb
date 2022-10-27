#!/usr/bin/env python3

import argparse
import io
import json
import os
import sys
import copy
from datetime import datetime
from decimal import Decimal
from tempfile import mkstemp

import duckdb
from jsonschema import Draft7Validator, FormatChecker

from target_duckdb.db_sync import DbSync
from target_duckdb.logger import get_logger

LOGGER = get_logger("target_duckdb")

DEFAULT_BATCH_SIZE_ROWS = 100000


class RecordValidationException(Exception):
    """Exception to raise when record validation failed"""


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed"""


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message["schema"]["properties"]["_sdc_extracted_at"] = {
        "type": ["null", "string"],
        "format": "date-time",
    }
    extended_schema_message["schema"]["properties"]["_sdc_batched_at"] = {
        "type": ["null", "string"],
        "format": "date-time",
    }
    extended_schema_message["schema"]["properties"]["_sdc_deleted_at"] = {
        "type": ["null", "string"]
    }

    return extended_schema_message


def add_metadata_values_to_record(record_message):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    extended_record = record_message["record"]
    extended_record["_sdc_extracted_at"] = record_message.get("time_extracted")
    extended_record["_sdc_batched_at"] = datetime.now().isoformat()
    extended_record["_sdc_deleted_at"] = record_message.get("record", {}).get(
        "_sdc_deleted_at"
    )

    return extended_record


def emit_state(state):
    """Emit state message to standard output then it can be
    consumed by other components"""
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug("Emitting state %s", line)
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


# pylint: disable=missing-function-docstring,missing-class-docstring
def validate_config(config):
    errors = []
    required_config_keys = [
        "filepath",
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    # Check target schema config
    config_default_target_schema = config.get("default_target_schema", None)
    config_schema_mapping = config.get("schema_mapping", None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append(
            "Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config."
        )

    return errors


def duckdb_connect(config):
    # Validate connection configuration
    config_errors = validate_config(config)

    # Exit if config has errors
    if len(config_errors) > 0:
        LOGGER.error("Invalid configuration:\n   * %s", "\n   * ".join(config_errors))
        sys.exit(1)

    # TODO(jwills): make this richer-- threads, pre-load extensions, etc.
    return duckdb.connect(config["filepath"])


# pylint: disable=too-many-locals,too-many-branches,too-many-statements,invalid-name,consider-iterating-dictionary
def persist_lines(connection, config, lines) -> None:
    """Read singer messages and process them line by line"""
    state = None
    flushed_state = None
    schemas = {}
    key_properties = {}
    validators = {}
    records_to_load = {}
    row_count = {}
    stream_to_sync = {}
    total_row_count = {}
    batch_size_rows = config.get("batch_size_rows", DEFAULT_BATCH_SIZE_ROWS)

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n%s", line)
            raise

        if "type" not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o["type"]

        if t == "RECORD":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )
            if o["stream"] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        o["stream"]
                    )
                )

            # Get schema for this record's stream
            stream = o["stream"]

            # Validate record
            if config.get("validate_records"):
                try:
                    validators[stream].validate(float_to_decimal(o["record"]))
                except Exception as ex:
                    if type(ex).__name__ == "InvalidOperation":
                        raise InvalidValidationOperationException(
                            f"Data validation failed and cannot load to destination. RECORD: {o['record']}\n"
                            "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                            "or more) Try removing 'multipleOf' methods from JSON schema."
                        ) from ex
                    raise RecordValidationException(
                        f"Record does not pass schema validation. RECORD: {o['record']}"
                    ) from ex

            primary_key_string = stream_to_sync[stream].record_primary_key_string(
                o["record"]
            )
            if not primary_key_string:
                primary_key_string = "RID-{}".format(total_row_count[stream])

            if stream not in records_to_load:
                records_to_load[stream] = {}

            # increment row count only when a new PK is encountered in the current batch
            if primary_key_string not in records_to_load[stream]:
                row_count[stream] += 1
                total_row_count[stream] += 1

            # append record
            if config.get("add_metadata_columns") or config.get("hard_delete"):
                records_to_load[stream][
                    primary_key_string
                ] = add_metadata_values_to_record(o)
            else:
                records_to_load[stream][primary_key_string] = o["record"]

            row_count[stream] = len(records_to_load[stream])

            if row_count[stream] >= batch_size_rows:
                # flush all streams, delete records if needed, reset counts and then emit current state
                if config.get("flush_all_streams"):
                    filter_streams = None
                else:
                    filter_streams = [stream]

                # Flush and return a new state dict with new positions only for the flushed streams
                flushed_state = flush_streams(
                    records_to_load,
                    row_count,
                    stream_to_sync,
                    config,
                    state,
                    flushed_state,
                    filter_streams=filter_streams,
                )

                # emit last encountered state
                emit_state(copy.deepcopy(flushed_state))

        elif t == "STATE":
            LOGGER.debug("Setting state to %s", o["value"])
            state = o["value"]

            # Initially set flushed state
            if not flushed_state:
                flushed_state = copy.deepcopy(state)

        elif t == "SCHEMA":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )
            stream = o["stream"]

            schemas[stream] = float_to_decimal(o["schema"])
            validators[stream] = Draft7Validator(
                schemas[stream], format_checker=FormatChecker()
            )

            # flush records from previous stream SCHEMA
            if row_count.get(stream, 0) > 0:
                flushed_state = flush_streams(
                    records_to_load,
                    row_count,
                    stream_to_sync,
                    config,
                    state,
                    flushed_state,
                )

                # emit latest encountered state
                emit_state(flushed_state)

            # key_properties key must be available in the SCHEMA message.
            if "key_properties" not in o:
                raise Exception("key_properties field is required")

            # Log based and Incremental replications on tables with no Primary Key
            # cause duplicates when merging UPDATE events.
            # Stop loading data by default if no Primary Key.
            #
            # If you want to load tables with no Primary Key:
            #  1) Set ` 'primary_key_required': false ` in the target-duckdb config.json
            if (
                config.get("primary_key_required", True)
                and len(o["key_properties"]) == 0
            ):
                LOGGER.critical(
                    "Primary key is set to mandatory but not defined in the [%s] stream",
                    stream,
                )
                raise Exception("key_properties field is required")

            key_properties[stream] = o["key_properties"]

            if config.get("add_metadata_columns") or config.get("hard_delete"):
                stream_to_sync[stream] = DbSync(
                    connection, config, add_metadata_columns_to_schema(o)
                )
            else:
                stream_to_sync[stream] = DbSync(connection, config, o)

            stream_to_sync[stream].create_schema_if_not_exists()
            stream_to_sync[stream].sync_table()

            row_count[stream] = 0
            total_row_count[stream] = 0

        elif t == "ACTIVATE_VERSION":
            LOGGER.debug("ACTIVATE_VERSION message")

            # Initially set flushed state
            if not flushed_state:
                flushed_state = copy.deepcopy(state)

        else:
            raise Exception(
                "Unknown message type {} in message {}".format(o["type"], o)
            )

    # if some bucket has records that need to be flushed but haven't reached batch size
    # then flush all buckets.
    if sum(row_count.values()) > 0:
        # flush all streams one last time, delete records if needed, reset counts and then emit current state
        flushed_state = flush_streams(
            records_to_load, row_count, stream_to_sync, config, state, flushed_state
        )

    # emit latest state
    emit_state(copy.deepcopy(flushed_state))


# pylint: disable=too-many-arguments
def flush_streams(
    streams,
    row_count,
    stream_to_sync,
    config,
    state,
    flushed_state,
    filter_streams=None,
):
    """
    Flushes all buckets and resets records count to 0 as well as empties records to load list
    :param streams: dictionary with records to load per stream
    :param row_count: dictionary with row count per stream
    :param stream_to_sync: DuckDB db sync instance per stream
    :param config: dictionary containing the configuration
    :param state: dictionary containing the original state from tap
    :param flushed_state: dictionary containing updated states only when streams got flushed
    :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
    :return: State dict with flushed positions
    """

    # Select the required streams to flush
    if filter_streams:
        streams_to_flush = filter_streams
    else:
        streams_to_flush = streams.keys()

    for stream in streams_to_flush:
        load_stream_batch(
            stream=stream,
            records_to_load=streams[stream],
            row_count=row_count,
            db_sync=stream_to_sync[stream],
            delete_rows=config.get("hard_delete"),
            temp_dir=config.get("temp_dir"),
        )

    # reset flushed stream records to empty to avoid flushing same records
    for stream in streams_to_flush:
        streams[stream] = {}

        # Update flushed streams
        if filter_streams:
            # update flushed_state position if we have state information for the stream
            if state is not None and stream in state.get("bookmarks", {}):
                # Create bookmark key if not exists
                if "bookmarks" not in flushed_state:
                    flushed_state["bookmarks"] = {}
                # Copy the stream bookmark from the latest state
                flushed_state["bookmarks"][stream] = copy.deepcopy(
                    state["bookmarks"][stream]
                )

        # If we flush every bucket use the latest state
        else:
            flushed_state = copy.deepcopy(state)

    # Return with state message with flushed positions
    return flushed_state


# pylint: disable=too-many-arguments
def load_stream_batch(
    stream, records_to_load, row_count, db_sync, delete_rows=False, temp_dir=None
):
    """Load a batch of records and do post load operations, like creating
    or deleting rows"""
    # Load into DuckDB
    if row_count[stream] > 0:
        flush_records(stream, records_to_load, row_count[stream], db_sync, temp_dir)

    # NOTE(jwills): taking index creation out for now as it causes more headaches than it's
    # worth for DuckDB; see https://github.com/duckdb/duckdb/issues/3265
    # db_sync.create_indices(stream)

    # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
    if delete_rows:
        db_sync.delete_rows(stream)

    # reset row count for the current stream
    row_count[stream] = 0


# pylint: disable=unused-argument
def flush_records(stream, records_to_load, row_count, db_sync, temp_dir=None):
    """Take a list of records and load into database"""
    db_sync.load_rows(records_to_load.values(), row_count)


def main():
    """Main entry point"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--config", help="Config file")
    args = arg_parser.parse_args()

    if args.config:
        with open(args.config) as config_input:
            config = json.load(config_input)
    else:
        config = {}

    # Consume singer messages
    singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    connection = duckdb_connect(config)
    persist_lines(connection, config, singer_messages)
    connection.close()

    LOGGER.debug("Exiting normally")


if __name__ == "__main__":
    main()
