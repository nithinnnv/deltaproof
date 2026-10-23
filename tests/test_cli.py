from decimal import Decimal

import numpy as np

from deltaproof import cli
from deltaproof.store import Table
from deltaproof.verify import table_rescan, verify

SPEC = """
[[table]]
name = "accounts"
key = ["id"]
columns = "balance:d3 owner:t note:t?"

[[table]]
name = "lines"
key = ["order_id", "line"]
columns = "qty:i"
"""


def rows(n, changed=()):
    acc = [{"id": i, "balance": Decimal(i) / 1000, "owner": f"o{i}", "note": None} for i in range(n)]
    lines = [{"order_id": i, "line": j, "qty": i + j} for i in range(n // 4) for j in range(1, 4)]
    for i in changed:
        acc[i]["balance"] += Decimal("0.001")
    return acc, lines


def table(specs, acc, lines):
    els = [specs[0].element(r) for r in acc] + [specs[1].element(r) for r in lines]
    u = lambda i: np.array([e[i] for e in els], dtype=np.uint64)
    return Table(u(0), u(1), u(2), np.zeros(len(els), np.int64))


def test_sketch_files_round_trip_and_ladders_of_different_size_compare(tmp_path):
    (tmp_path / "s.toml").write_text(SPEC)
    specs = cli.load_spec(str(tmp_path / "s.toml"))
    assert specs[0].columns[0].type.scale == 3 and specs[0].columns[2].nullable
    src = table(specs, *rows(40_000))
    acc, lines = rows(40_000, changed=range(0, 300, 3))
    del acc[500:520]  # missing in target, and a smaller target ladder
    lines.append({"order_id": 99_999, "line": 1, "qty": 0})  # extra in target
    tgt = table(specs, acc[:2_000], lines[:1_000])
    tgt_full = table(specs, acc, lines)
    for name, t in (("a", src), ("b", tgt)):
        cli.save(cli.build(t), len(t), str(tmp_path / f"{name}.npz"))
    a, _ = cli.load(str(tmp_path / "a.npz"))
    b, rows_b = cli.load(str(tmp_path / "b.npz"))
    assert rows_b == len(tgt) and len(b.levels) < len(a.levels)
    rep = verify(*cli.common(a, b), table_rescan(src, tgt))
    want = verify(*cli.common(cli.build(src), cli.build(tgt)), table_rescan(src, tgt))
    assert rep.keys == want.keys and len(rep.keys) > 0

    rep = verify(*cli.common(cli.build(src), cli.build(tgt_full)), table_rescan(src, tgt_full))
    out = cli.describe(rep, specs, limit=-1)
    assert out["value_mismatch"]["count"] == 100
    assert out["missing_in_target"]["count"] == 20
    assert {"table": "accounts", "id": 500} in out["missing_in_target"]["keys"]
    assert out["extra_in_target"]["keys"] == [{"table": "lines", "order_id": 99_999, "line": 1}]
