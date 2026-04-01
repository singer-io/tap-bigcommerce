"""Unit tests for tap_bigcommerce.sync — sync_stream function."""
import unittest
from unittest.mock import MagicMock, patch, call

from tap_bigcommerce.sync import sync_stream


class TestSyncStreamBasic(unittest.TestCase):
    """Test sync_stream writes records and returns counter."""

    def _make_instance(self, records):
        """Build a mock stream instance that yields (stream, record) tuples."""
        instance = MagicMock()
        stream = MagicMock()
        stream.tap_stream_id = 'orders'
        stream.schema.to_dict.return_value = {
            'type': 'object',
            'properties': {'id': {'type': 'integer'}},
        }
        stream.metadata = []
        instance.stream = stream
        instance.sync.return_value = iter([(stream, r) for r in records])
        return instance

    @patch('tap_bigcommerce.sync.singer.write_state')
    @patch('tap_bigcommerce.sync.singer.write_record')
    def test_returns_record_count(self, mock_write_record, mock_write_state):
        records = [{'id': 1}, {'id': 2}, {'id': 3}]
        instance = self._make_instance(records)
        state = {'bookmarks': {}}
        count = sync_stream(state, instance)
        self.assertEqual(count, 3)

    @patch('tap_bigcommerce.sync.singer.write_state')
    @patch('tap_bigcommerce.sync.singer.write_record')
    def test_calls_write_record_for_each_row(self, mock_write_record, mock_write_state):
        records = [{'id': 1}, {'id': 2}]
        instance = self._make_instance(records)
        state = {'bookmarks': {}}
        sync_stream(state, instance)
        self.assertEqual(mock_write_record.call_count, 2)

    @patch('tap_bigcommerce.sync.singer.write_state')
    @patch('tap_bigcommerce.sync.singer.write_record')
    def test_write_record_called_with_stream_id(self, mock_write_record, mock_write_state):
        records = [{'id': 1}]
        instance = self._make_instance(records)
        state = {'bookmarks': {}}
        sync_stream(state, instance)
        args = mock_write_record.call_args
        self.assertEqual(args[0][0], 'orders')

    @patch('tap_bigcommerce.sync.singer.write_state')
    @patch('tap_bigcommerce.sync.singer.write_record')
    def test_empty_stream_returns_zero(self, mock_write_record, mock_write_state):
        instance = self._make_instance([])
        state = {'bookmarks': {}}
        count = sync_stream(state, instance)
        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()


class TestSyncStreamBatchedState(unittest.TestCase):
    """Test that write_state is called every 1000 records."""

    def _make_instance(self, n_records):
        instance = MagicMock()
        stream = MagicMock()
        stream.tap_stream_id = 'orders'
        stream.schema.to_dict.return_value = {
            'type': 'object',
            'properties': {'id': {'type': 'integer'}},
        }
        stream.metadata = []
        instance.stream = stream
        records = [{'id': i} for i in range(n_records)]
        instance.sync.return_value = iter([(stream, r) for r in records])
        return instance

    @patch('tap_bigcommerce.sync.singer.write_state')
    @patch('tap_bigcommerce.sync.singer.write_record')
    def test_write_state_called_at_1000(self, mock_write_record, mock_write_state):
        instance = self._make_instance(1001)
        state = {'bookmarks': {}}
        sync_stream(state, instance)
        # write_state should be called at record 1000
        self.assertGreaterEqual(mock_write_state.call_count, 1)

    @patch('tap_bigcommerce.sync.singer.write_state')
    @patch('tap_bigcommerce.sync.singer.write_record')
    def test_write_state_not_called_under_1000(self, mock_write_record, mock_write_state):
        instance = self._make_instance(999)
        state = {'bookmarks': {}}
        sync_stream(state, instance)
        mock_write_state.assert_not_called()

    @patch('tap_bigcommerce.sync.singer.write_state')
    @patch('tap_bigcommerce.sync.singer.write_record')
    def test_write_state_called_with_state(self, mock_write_record, mock_write_state):
        instance = self._make_instance(1000)
        state = {'bookmarks': {'orders': {'date_modified': '2024-01'}}}
        sync_stream(state, instance)
        mock_write_state.assert_called_with(state)


class TestSyncStreamExceptionHandling(unittest.TestCase):
    """Test that transform exceptions are caught and the loop continues."""

    @patch('tap_bigcommerce.sync.singer.write_state')
    @patch('tap_bigcommerce.sync.singer.write_record')
    @patch('tap_bigcommerce.sync.Transformer')
    def test_bad_record_is_skipped(self, MockTransformer, mock_write_record, mock_write_state):
        """If Transformer.transform raises, that record is skipped."""
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.transform.side_effect = [Exception("bad data"), {'id': 2}]
        MockTransformer.return_value = ctx

        instance = MagicMock()
        stream = MagicMock()
        stream.tap_stream_id = 'orders'
        stream.schema.to_dict.return_value = {
            'type': 'object',
            'properties': {'id': {'type': 'integer'}},
        }
        stream.metadata = []
        instance.stream = stream
        instance.sync.return_value = iter([
            (stream, {'id': 1}),  # will raise
            (stream, {'id': 2}),  # will succeed
        ])

        state = {'bookmarks': {}}
        count = sync_stream(state, instance)
        # Both records counted (counter increments before transform)
        self.assertEqual(count, 2)
        # Only 1 write_record (the second) succeeds
        self.assertEqual(mock_write_record.call_count, 1)


if __name__ == '__main__':
    unittest.main()
