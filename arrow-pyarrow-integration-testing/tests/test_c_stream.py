import unittest

import pyarrow.ipc

import arrow_pyarrow_integration_testing


class TestCase(unittest.TestCase):
    def test_rust_reads(self):
        """
        Python -> Rust -> Python
        """
        schema = pyarrow.schema([pyarrow.field("aa", pyarrow.int32())])
        a = pyarrow.array([1, None, 2], type=pyarrow.int32())

        batch = pyarrow.record_batch([a], schema)
        reader = pyarrow.ipc.RecordBatchStreamReader.from_batches(schema, [batch])

        arrays = arrow_pyarrow_integration_testing.to_rust_iterator(reader)
        
        array = arrays[0].field(0)
        assert array == a
