from decimal import Decimal

import numpy as np
import pytest

from deltaproof import canon, cli
from deltaproof.store import Table
from deltaproof.verify import table_rescan, verify

SPEC = """
[[table]]
name = "accounts"
key = ["region:c", "account:t"]
columns = "balance:d2 owner:t"

[[table]]
name = "lines"
key = ["order_id", "line"]
columns = "qty:i"
"""


@pytest.fixture
def specs(tmp_path):
    (tmp_path / "s.toml").write_text(SPEC)
    return cli.load_spec(str(tmp_path / "s.toml"))


def test_text_keys_are_hashed_and_keep_the_table_tag(specs):
    acc, lines = specs
    assert acc.hashed and not lines.hashed
    k = acc.key_of({"region": "EU  ", "account": "ACME-001"})
    assert canon.unpack_key(*k, 1)[0] == acc.tag
    assert k == acc.key_of({"region": "EU", "account": "ACME-001"})  # CHAR padding is not part of the key
    assert k != acc.key_of({"region": "EU", "account": "ACME-002"})
    assert acc.key_of({"region": "E", "account": "UACME-001"}) != k  # parts are length-prefixed
    many = cli.TableSpec("t", 3, ("a", "b", "c", "d", "e"), ())
    assert many.hashed and many.key_of(dict(zip("abcde", range(5)))) != many.key_of(dict(zip("abcde", range(1, 6))))


def test_hashed_keys_verify_exactly_and_report_unresolved_without_stores(specs):
    acc, _ = specs
    rows = [{"region": "EU", "account": f"A{i:05d}", "balance": Decimal(i) / 100, "owner": "o"} for i in range(5000)]
    changed = [dict(r) for r in rows]
    changed[7]["balance"] += Decimal("0.01")
    del changed[9]

    def table(rs):
        els = [acc.element(r) for r in rs]
        return Table(*(np.array([e[i] for e in els], np.uint64) for i in range(3)), np.zeros(len(els), np.int64))

    s, t = table(rows), table(changed)
    rep = verify(*cli.common(cli.build(s), cli.build(t)), table_rescan(s, t))
    out = cli.describe(rep, specs, limit=-1)
    assert out["value_mismatch"]["count"] == 1 and out["missing_in_target"]["count"] == 1
    assert set(out["value_mismatch"]["keys"][0]) == {"table", "key_hash"}


class FakeStore:
    """Enough of cli.Store for fetch_row: a PostgreSQL-like store backed by dicts."""

    kind = "postgres"

    def __init__(self, rows: dict):
        self.rows = rows
        self.conn = self

    def execute(self, query, params):
        table = query.split(" FROM ")[1].split()[0]
        row = self.rows.get((table, tuple(params)))
        return type("R", (), {"fetchone": lambda _: row})()


def test_inspect_names_only_the_columns_whose_canonical_values_differ(specs):
    _, lines = specs
    lines = cli.TableSpec("lines", 2, ("order_id", "line"), cli._cols("qty:i note:c amount:d2"))
    src = FakeStore({("lines", (1, 1)): (1, 1, 5, "x  ", Decimal("1.50")),
                     ("lines", (2, 1)): (2, 1, 7, "y", Decimal("2.00"))})
    tgt = FakeStore({("lines", (1, 1)): (1, 1, 6, "x", Decimal("1.5"))})
    report = {"missing_in_target": {"keys": [{"table": "lines", "order_id": 2, "line": 1}]},
              "extra_in_target": {"keys": []},
              "value_mismatch": {"keys": [{"table": "lines", "order_id": 1, "line": 1}]}}
    found = cli.inspect_keys(report, [lines], src, tgt, limit=-1)
    missing, mismatch = found
    assert missing["class"] == "missing_in_target" and missing["source_row"] and not missing["target_row"]
    assert [c["name"] for c in mismatch["columns"]] == ["qty"]  # padding and decimal scale are not differences


def test_cli_exit_codes(tmp_path, specs):
    (tmp_path / "s.toml").write_text(SPEC)
    with pytest.raises(SystemExit) as e:
        cli.main(["diff", "--spec", str(tmp_path / "s.toml"), "missing-a.npz", "missing-b.npz"])
    assert e.value.code == 2
