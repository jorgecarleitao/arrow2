import subprocess
import os

import pyarrow.ipc
import pyarrow.parquet as pq


def get_file_path(file: str):
    return f"../testing/arrow-testing/data/arrow-ipc-stream/integration/1.0.0-littleendian/{file}.arrow_file"


def _prepare(file: str):
    write = f"{file}.parquet"
    subprocess.call(["cargo", "run", "--", "--json", file, "--output", write])
    return write


def _expected(file: str):
    return pyarrow.ipc.RecordBatchFileReader(get_file_path(file)).read_all()


for file in [
    "generated_primitive",
    "generated_primitive_no_batches",
    "generated_primitive_zerolength",
    "generated_null",
    "generated_null_trivial",
    "generated_primitive_large_offsets",
    # requires writing Dictionary
    # "generated_dictionary",
    # requires writing Duration
    # "generated_interval",
    # requires writing Struct
    # "generated_duplicate_fieldnames",
    # requires writing Decimal
    # "generated_decimal",
    # requires writing Date64
    # "generated_datetime",
    # requires writing un-nested List
    # "generated_custom_metadata",
]:
    expected = _expected(file)
    path = _prepare(file)

    table = pq.read_table(path)
    os.remove(path)

    for c1, c2 in zip(expected, table):
        assert c1 == c2
