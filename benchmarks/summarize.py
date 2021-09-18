import json
import os


def _read_reports(engine: str):
    root = {
        "arrow2": "target/criterion",
        "pyarrow": "benchmarks/runs",
        "arrow": "target/criterion",
    }[engine]

    result = []
    for item in os.listdir(root):
        if item == "report":
            continue

        with open(os.path.join(root, item, "new", "estimates.json")) as f:
            data = json.load(f)

        ms = data["mean"]["point_estimate"] / 1000
        task = item.split()[0]
        type = " ".join(item.split()[1:-1])
        size = int(item.split()[-1].split("_")[1])
        result.append(
            {
                "engine": engine,
                "task": task,
                "type": type,
                "size": size,
                "time": ms,
            }
        )
    return result


def _print_report(engine, result):
    result = list(filter(lambda x: x["engine"] == engine, result))

    task = {
        "arrow2": "read",
        "pyarrow": "read",
        "arrow": "read_arrow",
    }[engine]

    for ty in ["i64", "bool", "utf8", "utf8 dict"]:
        print(engine, ty)
        r = filter(lambda x: x["type"] == ty, result)
        r = filter(lambda x: x["task"] == task, r)
        r = sorted(r, key=lambda x: x["size"])
        for row in r:
            print(row["time"])


def print_report():
    for engine in ["arrow2", "pyarrow", "arrow"]:
        print(engine)
        result = _read_reports(engine)
        _print_report(engine, result)


print_report()
