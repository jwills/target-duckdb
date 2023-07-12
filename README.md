# target-duckdb

[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)

[Singer](https://www.singer.io/) target that loads data into DuckDB following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

### Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
make venv
```

### To run

Like any other target that's following the singer specification:

`some-singer-tap | target-duckdb --config [config.json]`

It's reading incoming messages from STDIN and using the properties in `config.json` to load data into DuckDB.

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.


### Configuration settings

Running the target connector requires a `config.json` file. An example with the minimal settings:

```json
{
    "path": "/path/to/local/file.duckdb",
    "default_target_schema": "main"
}
```

Additional options in `config.json`:

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| path (alias: filepath)              | String  | Yes        | The path to use for the `duckdb.connect` call; either a local file or a MotherDuck connection uri. |
| dbname (alias: database)            | String  |            | The database name to write to; this will be inferred from the path property if it is not specified. |
| token                               | String  |            | For MotherDuck connections, the auth token to use (this may also be set directly via the MOTHERDUCK_TOKEN environment variable. |
| delimiter                           | String  |            | (Default: ',') The delimiter to use for the CSV files that are used for record imports. |
| quotechar                           | String  |            | (Default: '"') The quote character to use for the CSV files that are used for record imports. |
| batch_size_rows                     | Integer |            | (Default: 100000) Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into DuckDB. |
| flush_all_streams                   | Boolean |            | (Default: False) Flush and load every stream into DuckDB when one batch is full. Warning: This may trigger the COPY command to use files with low number of records. |
| default_target_schema               | String  |            | Name of the schema where the tables will be created. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.    |
| schema_mapping                      | Object  |            | Useful if you want to load multiple streams from one tap to multiple DuckDB schemas.<br><br>If the tap sends the `stream_id` in `<schema_name>-<table_name>` format then this option overwrites the `default_target_schema` value. |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in postgres etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recognisable in DuckDB. |
| hard_delete                         | Boolean |            | (Default: False) When `hard_delete` option is true then DELETE SQL commands will be performed in DuckDB to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. |
| data_flattening_max_level           | Integer |            | (Default: 0) Object type RECORD items from taps can be transformed to flattened columns by creating columns automatically.<br><br>When value is 0 (default) then flattening functionality is turned off. |
| primary_key_required                | Boolean |            | (Default: True) Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events. When set to true, stop loading data if no Primary Key is defined. |
| validate_records                    | Boolean |            | (Default: False) Validate every single record message to the corresponding JSON schema. This option is disabled by default and invalid RECORD messages will fail only at load time by DuckDB. Enabling this option will detect invalid records earlier but could cause performance degradation. |
| temp_dir                            | String  |            | (Default: platform-dependent) Directory of temporary CSV files with RECORD messages. |

### To run tests:

1. Install python dependencies in a virtual env and run unit and integration tests
```
  make venv
```

2. To run unit tests:
```
  make unit_test
```

3. To run integration tests:
```
  make integration_test
```

### To run pylint:

1. Install python dependencies and run python linter
```
 make venv pylint
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
