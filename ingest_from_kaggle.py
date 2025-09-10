from pathlib import Path
import pandas as pd

TMP = Path("_kaggle_tmp")
OUT = Path("data/raw")

def _pick_csv_with_ohlc(root: Path) -> pd.DataFrame:
    for p in root.rglob("*.csv"):
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        cols_lower = {c.lower(): c for c in df.columns}
        needed = {"open", "high", "low", "close", "date"}
        if not needed.issubset(set(cols_lower.keys())):
            continue
        df.rename(columns=lambda c: c.replace(" ", "_"), inplace=True)
        date_col = cols_lower["date"]
        df["Date"] = pd.to_datetime(df[date_col])
        df = df.set_index("Date").sort_index()
        if "Adj_Close" not in df.columns:
            df["Adj_Close"] = df.get("Adj_Close", df["Close"])
        if "Volume" not in df.columns:
            df["Volume"] = df.get("Volume", 0)
        cols = ["Open", "High", "Low", "Close", "Adj_Close", "Volume"]
        if not set(cols).issubset(df.columns):
            continue
        return df[cols].copy()
    raise FileNotFoundError(f"No suitable CSV with OHLC found under: {root}")

def _save_partitions(df: pd.DataFrame, sym: str) -> None:
    for y, part in df.groupby(df.index.year):
        out_dir = OUT / f"symbol={sym}" / f"year={int(y)}"
        out_dir.mkdir(parents=True, exist_ok=True)
        part.to_csv(out_dir / "data.csv", index_label="Date")
        print(f"[WRITE] {sym} {y} -> {out_dir/'data.csv'}")

def main():
    gspc = _pick_csv_with_ohlc(TMP)
    _save_partitions(gspc, "GSPC")
    gold = _pick_csv_with_ohlc(TMP)
    _save_partitions(gold, "GC_F")
    print("[DONE] Kaggle ingestion normalized to data/raw/")

if __name__ == "__main__":
    main()
