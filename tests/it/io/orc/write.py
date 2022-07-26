import os

import pyorc


data = {
    "a": [1.0, 2.0, None, 4.0, 5.0],
    "b": [True, False, None, True, False],
    "str_direct": ["a", "cccccc", None, "ddd", "ee"],
    "d": ["a", "bb", None, "ccc", "ddd"],
    "e": ["ddd", "cc", None, "bb", "a"],
    "f": ["aaaaa", "bbbbb", None, "ccccc", "ddddd"],
    "int_short_repeated": [5, 5, None, 5, 5],
    "int_neg_short_repeated": [-5, -5, None, -5, -5],
    "int_delta": [1, 2, None, 4, 5],
    "int_neg_delta": [5, 4, None, 2, 1],
    "int_direct": [1, 6, None, 3, 2],
    "int_neg_direct": [-1, -6, None, -3, -2],
}


def _write(
    schema: str,
    data,
    file_name: str,
    compression=pyorc.CompressionKind.NONE,
    dict_key_size_threshold=0.0,
):
    output = open(file_name, "wb")
    writer = pyorc.Writer(
        output,
        schema,
        dict_key_size_threshold=dict_key_size_threshold,
        compression=compression,
    )
    num_rows = len(list(data.values())[0])
    for x in range(num_rows):
        row = tuple(values[x] for values in data.values())
        writer.write(row)
    writer.close()

os.makedirs("fixtures/pyorc", exist_ok=True)
_write(
    "struct<a:float,b:boolean,str_direct:string,d:string,e:string,f:string,int_short_repeated:int,int_neg_short_repeated:int,int_delta:int,int_neg_delta:int,int_direct:int,int_neg_direct:int>",
    data,
    "fixtures/pyorc/test.orc",
)
