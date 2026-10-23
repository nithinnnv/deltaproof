"""Turn results/*.csv into the data files the LaTeX figures read (paper/data/)."""

import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "paper" / "data"
METHODS = ("deltaproof", "pinsketch", "merkle", "bisection", "full_scan")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    rows = list(csv.DictReader((ROOT / "results" / "rq2_scaling.csv").open()))
    for n in sorted({int(r["n"]) for r in rows}):
        table = defaultdict(dict)
        for r in rows:
            if int(r["n"]) == n and int(r["d"]) > 0:
                table[int(r["d"])][r["method"]] = (int(r["bytes"]) / 1e6, int(r["rows_scanned"]) / 1e6)
        with (OUT / f"rq2_n{n}.dat").open("w") as f:
            f.write("d " + " ".join(f"{m}_mb {m}_mrows" for m in METHODS) + "\n")
            for d in sorted(table):
                vals = " ".join(f"{table[d][m][0]:.4f} {table[d][m][1]:.4f}" for m in METHODS)
                f.write(f"{d} {vals}\n")
    print("wrote", sorted(p.name for p in OUT.iterdir()))


if __name__ == "__main__":
    main()
