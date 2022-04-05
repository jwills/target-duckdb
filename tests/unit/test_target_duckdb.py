import unittest
import os

from unittest.mock import patch

import duckdb
import target_duckdb


def _mock_record_to_csv_line(record):
    return record


class TestTargetDuckDB(unittest.TestCase):

    def setUp(self):
        self.config = {}
        self.connection = duckdb.connect()
    
    def tearDown(self) -> None:
        self.connection.close()

    @patch('target_duckdb.flush_streams')
    @patch('target_duckdb.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self,
                                                                                     dbsync_mock,
                                                                                     flush_streams_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbsync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_duckdb.persist_lines(self.connection, self.config, lines)

        flush_streams_mock.assert_called_once()
