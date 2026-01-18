import json
import os
import glob


def test_silver_outputs_exist():
    # Deben existir las carpetas silver principales
    expected_dirs = [
        "warehouse/silver/users",
        "warehouse/silver/orders",
        "warehouse/silver/events",
        "warehouse/silver/ads",
    ]
    for d in expected_dirs:
        assert os.path.isdir(d), f"Missing directory: {d}"


def test_dq_report_exists_and_has_expected_keys():
    files = glob.glob("warehouse/silver/dq_report/part-*.json")
    assert files, "DQ report file not found under warehouse/silver/dq_report"

    # Leemos todas las líneas JSON
    rows = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))

    assert rows, "DQ report is empty"

    required = {"table", "rows", "null_rate_max", "dup_rate_id"}
    for r in rows:
        assert required.issubset(r.keys()), f"Missing keys in dq row: {r}"
        assert isinstance(r["table"], str) and r["table"], "Invalid table"
        assert isinstance(r["rows"], int) and r["rows"] > 0, "Invalid rows"
        assert isinstance(r["null_rate_max"], (int, float)), "Invalid null_rate_max"
        assert isinstance(r["dup_rate_id"], (int, float)), "Invalid dup_rate_id"
