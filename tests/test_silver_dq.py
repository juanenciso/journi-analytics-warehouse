import json
from pathlib import Path

def test_dq_report_exists():
    p = Path("warehouse/silver/dq_report")
    assert p.exists(), "dq_report directory should exist after running make silver"

def test_dq_report_has_expected_tables():
    p = Path("warehouse/silver/dq_report")
    files = list(p.glob("part-*.json"))
    assert files, "Expected part-*.json inside dq_report"

    rows = []
    for f in files:
        for line in f.read_text().splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    tables = {r["table"] for r in rows}
    assert tables == {"users", "orders", "events", "ads"}

