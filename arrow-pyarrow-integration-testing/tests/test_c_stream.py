import unittest

import pyarrow.ipc

import arrow_pyarrow_integration_testing


class TestCase(unittest.TestCase):
    def test_rust_reads(self):
        schema = pyarrow.schema([pyarrow.field("aa", pyarrow.int32())])
        a = pyarrow.array([1, None, 2], type=pyarrow.int32())

        batch = pyarrow.record_batch([a], schema)
        reader = pyarrow.ipc.RecordBatchStreamReader.from_batches(schema, [batch])

        arrays = arrow_pyarrow_integration_testing.to_rust_iterator(reader)
        
        array = arrays[0].field(0)
        assert array == a

    # see https://issues.apache.org/jira/browse/ARROW-15747
    def _test_pyarrow_reads(self):
        stream = arrow_pyarrow_integration_testing.from_rust_iterator()

        arrays = [a for a in stream]

        assert False
