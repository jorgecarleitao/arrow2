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


# types without a native parquet logical representation
# There is currently no specification on how to represent these in parquet,
# and thus we ignore them in comparisons
non_native_types = [
    pyarrow.date64(),
    pyarrow.time32("s"),
    pyarrow.timestamp("s"),
    pyarrow.timestamp("s", tz="UTC"),
    pyarrow.duration("s"),
    pyarrow.duration("ms"),
    pyarrow.duration("us"),
    pyarrow.duration("ns"),
]


for file in [
    "generated_primitive",
    "generated_primitive_no_batches",
    "generated_primitive_zerolength",
    "generated_null",
    "generated_null_trivial",
    "generated_primitive_large_offsets",
    "generated_datetime",
    "generated_decimal",
    "generated_interval",
    # requires writing Dictionary
    # "generated_dictionary",
    # requires writing Struct
    # "generated_duplicate_fieldnames",
    # requires writing un-nested List
    # "generated_custom_metadata",
]:
    expected = _expected(file)
    path = _prepare(file)

    table = pq.read_table(path)
    os.remove(path)

    for c1, c2 in zip(expected, table):
        if c1.type in non_native_types:
            continue
        if str(c1.type) in ["month_interval", "day_time_interval"]:
            # pyarrow does not support interval types from parquet
            continue
        assert c1 == c2
