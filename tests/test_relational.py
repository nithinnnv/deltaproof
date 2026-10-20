from datetime import datetime, timezone
from decimal import Decimal

import pytest

from deltaproof import canon
from deltaproof.relational import TPCC_BY_NAME


def test_pack_key_round_trip_and_tag_separation():
    hi, lo = canon.pack_key(8, (4, 10, 3001, 15))
    assert canon.unpack_key(hi, lo, 4) == (8, (4, 10, 3001, 15))
    assert canon.pack_key(1, (5,)) != canon.pack_key(2, (5,))
    assert canon.unpack_key(*canon.pack_key(3, (1, 2, 3)), 3) == (3, (1, 2, 3))
    with pytest.raises(canon.CanonError):
        canon.pack_key(1, (1 << 30,))


def test_char_ignores_padding_and_float_uses_bits():
    assert canon.Char().canon("AB      ") == canon.Char().canon("AB") == canon.Text().canon("AB")
    f = canon.Float()
    assert f.canon(10.0) == f.canon(Decimal("10.0")) == f.canon(10)
    assert f.canon(-0.0) == f.canon(0.0)
    assert f.canon(0.1) != f.canon(0.1000001)


def test_customer_row_matches_across_wal2json_and_driver_forms():
    spec = TPCC_BY_NAME["customer"]
    driver = {
        "c_w_id": 1, "c_d_id": 2, "c_id": 3, "c_discount": Decimal("0.1234"), "c_credit": "GC",
        "c_last": "BARBARABLE", "c_first": "x7f", "c_credit_lim": Decimal("50000.00"),
        "c_balance": Decimal("-10.00"), "c_ytd_payment": 10.0, "c_payment_cnt": 1, "c_delivery_cnt": 0,
        "c_street_1": "a", "c_street_2": "b", "c_city": "c", "c_state": "CA", "c_zip": "123411111",
        "c_phone": "1234567890123456", "c_since": datetime(2026, 10, 22, 7, 21, 40, 123456),
        "c_middle": "OE", "c_data": "data",
    }
    wal2json = {**driver, "c_discount": Decimal("0.1234"), "c_balance": Decimal("-10"),
                "c_ytd_payment": Decimal("10.0"), "c_since": "2026-10-22 07:21:40.123456", "c_state": "CA"}
    cassandra = {**driver, "c_since": datetime(2026, 10, 22, 7, 21, 40, 123000), "c_credit": "GC"}
    padded = {**driver, "c_credit": "GC"}
    e = spec.element(driver)
    assert spec.element(wal2json) == e
    assert spec.element(cassandra) == e  # Cassandra keeps milliseconds only
    assert spec.element(padded) == e
    assert spec.element({**driver, "c_balance": Decimal("-10.01")}) != e
    assert spec.element({**driver, "c_since": datetime(2026, 10, 22, 7, 21, 41, tzinfo=timezone.utc)}) != e


def test_nullable_columns():
    spec = TPCC_BY_NAME["oorder"]
    row = {"o_w_id": 1, "o_d_id": 1, "o_id": 1, "o_c_id": 7, "o_carrier_id": None, "o_ol_cnt": 5,
           "o_all_local": 1, "o_entry_d": datetime(2026, 1, 1)}
    assert spec.element(row) != spec.element({**row, "o_carrier_id": 3})
    with pytest.raises(canon.CanonError):
        spec.element({**row, "o_c_id": None})
