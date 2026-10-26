import struct

from deltaproof.canon import Float
from deltaproof.relational import TPCC, TPCC_MYSQL_BY_NAME


def test_float32_agrees_across_representations():
    f = Float(bits=32)
    stored = struct.unpack("<f", struct.pack("<f", 10110.35))[0]  # what a real/FLOAT column holds
    widened = float(stored)  # the binlog's and CAST(... AS DOUBLE)'s value: 10110.349609375
    assert f.canon(10110.35) == f.canon(widened) == f.canon(stored)
    assert f.canon(10110.3) != f.canon(stored)  # MySQL's 6-digit text form is a different value
    assert Float().canon(10110.35) != Float().canon(widened)  # a double column would not agree


def test_mysql_spec_differs_only_in_the_float_column():
    my = TPCC_MYSQL_BY_NAME["customer"]
    pg = next(t for t in TPCC if t.name == "customer")
    diff = [(a.name, a.type, b.type) for a, b in zip(my.columns, pg.columns) if a != b]
    assert diff == [("c_ytd_payment", Float(bits=32), Float())]
    assert "c_ytd_payment real NOT NULL" in my.pg_ddl("src_v bigint")
    assert my.pg_ddl().endswith("PRIMARY KEY (c_w_id, c_d_id, c_id))")
