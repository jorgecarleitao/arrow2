import pyarrow as pa
import pyarrow.parquet
import os

PYARROW_PATH = "fixtures/pyarrow3"


def case_basic_nullable(size=1):
    int64 = [0, 1, None, 3, None, 5, 6, 7, None, 9]
    float64 = [0.0, 1.0, None, 3.0, None, 5.0, 6.0, 7.0, None, 9.0]
    string = ["Hello", None, "aa", "", None, "abc", None, None, "def", "aaa"]
    boolean = [True, None, False, False, None, True, None, None, True, True]

    fields = [
        pa.field("int64", pa.int64()),
        pa.field("float64", pa.float64()),
        pa.field("string", pa.utf8()),
        pa.field("bool", pa.bool_()),
        pa.field("date", pa.timestamp("ms")),
        pa.field("uint32", pa.uint32()),
    ]
    schema = pa.schema(fields)

    return (
        {
            "int64": int64 * size,
            "float64": float64 * size,
            "string": string * size,
            "bool": boolean * size,
            "date": int64 * size,
            "uint32": int64 * size,
        },
        schema,
        f"basic_nullable_{size*10}.parquet",
    )


def case_basic_required(size=1):
    int64 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    float64 = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    string = ["Hello", "bbb", "aa", "", "bbb", "abc", "bbb", "bbb", "def", "aaa"]
    boolean = [True, True, False, False, False, True, True, True, True, True]

    fields = [
        pa.field("int64", pa.int64(), nullable=False),
        pa.field("float64", pa.float64(), nullable=False),
        pa.field("string", pa.utf8(), nullable=False),
        pa.field("bool", pa.bool_(), nullable=False),
        pa.field(
            "date",
            pa.timestamp(
                "ms",
            ),
            nullable=False,
        ),
        pa.field("uint32", pa.uint32(), nullable=False),
    ]
    schema = pa.schema(fields)

    return (
        {
            "int64": int64 * size,
            "float64": float64 * size,
            "string": string * size,
            "bool": boolean * size,
            "date": int64 * size,
            "uint32": int64 * size,
        },
        schema,
        f"basic_required_{size*10}.parquet",
    )


def case_nested(size):
    items_nullable = [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
    items_required = [[0, 1], None, [2, 0, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
    all_required = [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
    i16 = [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
    boolean = [
        [False, True],
        None,
        [True, None, False],
        [True, False, True],
        [],
        [False, False, False],
        None,
        [True],
    ]
    items_nested = [
        [[0, 1]],
        None,
        [[2, None], [3]],
        [[4, 5], [6]],
        [],
        [[7], None, [9]],
        None,
        [[10]],
    ]
    string = [
        ["Hello", "bbb"],
        None,
        ["aa", None, ""],
        ["bbb", "aa", "ccc"],
        [],
        ["abc", "bbb", "bbb"],
        None,
        [""],
    ]
    fields = [
        pa.field("list_int64", pa.list_(pa.int64())),
        pa.field("list_int64_required", pa.list_(pa.field("item", pa.int64(), False))),
        pa.field(
            "list_int64_required_required",
            pa.list_(pa.field("item", pa.int64(), False)),
            False,
        ),
        pa.field("list_int16", pa.list_(pa.int16())),
        pa.field("list_bool", pa.list_(pa.bool_())),
        pa.field("list_utf8", pa.list_(pa.utf8())),
        pa.field("list_large_binary", pa.list_(pa.large_binary())),
        pa.field("list_nested_i64", pa.list_(pa.list_(pa.int64()))),
    ]
    schema = pa.schema(fields)
    return (
        {
            "list_int64": items_nullable * size,
            "list_int64_required": items_required * size,
            "list_int64_required_required": all_required * size,
            "list_int16": i16 * size,
            "list_bool": boolean * size,
            "list_utf8": string * size,
            "list_large_binary": string * size,
            "list_nested_i64": items_nested * size,
        },
        schema,
        f"nested_nullable_{size*10}.parquet",
    )


def write_pyarrow(case, size=1, page_version=1, use_dictionary=False):
    data, schema, path = case(size)

    base_path = f"{PYARROW_PATH}/v{page_version}"
    if use_dictionary:
        base_path = f"{base_path}/dict"

    t = pa.table(data, schema=schema)
    os.makedirs(base_path, exist_ok=True)
    pa.parquet.write_table(
        t,
        f"{base_path}/{path}",
        row_group_size=2 ** 40,
        use_dictionary=use_dictionary,
        compression=None,
        write_statistics=True,
        data_page_size=2 ** 40,  # i.e. a large number to ensure a single page
        data_page_version=f"{page_version}.0",
    )


for case in [case_basic_nullable, case_basic_required, case_nested]:
    for version in [1, 2]:
        for use_dict in [True, False]:
            write_pyarrow(case, 1, version, use_dict)


def case_benches(size):
    assert size % 8 == 0
    size //= 8
    data, schema, path = case_basic_nullable(1)
    for k in data:
        data[k] = data[k][:8] * size
    return data, schema, f"benches_{size}.parquet"


# for read benchmarks
for i in range(3 + 10, 3 + 22, 2):
    write_pyarrow(case_benches, 2 ** i, 1)  # V1
