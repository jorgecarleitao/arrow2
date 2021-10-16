import subprocess


# run pyarrow
subprocess.call(["python", "benchmarks/bench_read.py"])


for ty in ["i64", "bool", "utf8", "utf8 dict"]:
    args = [
        "cargo",
        "bench",
        "--features",
        "io_parquet,io_parquet_compression",
        "--bench",
        "read_parquet",
        "--",
        f"{ty} 2",
    ]

    subprocess.call(args)
