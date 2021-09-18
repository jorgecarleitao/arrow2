import subprocess


def _run_arrow2():
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


def _run_arrow():
    for ty in ["i64", "bool", "utf8", "utf8 dict"]:
        args = [
            "cargo",
            "bench",
            "--features",
            "benchmarks,io_parquet,io_parquet_compression",
            "--bench",
            "read_parquet_arrow",
            "--",
            f"{ty} 2",
        ]

        subprocess.call(args)


# run pyarrow
# subprocess.call(["python", "benchmarks/bench_read.py"])
_run_arrow()
# _run_arrow2()
