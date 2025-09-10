from pathlib import Path
import pandas as pd

RAW = Path("data/raw")
OUT = Path("data/processed")

def _load_symbol(sym: str) -> pd.DataFrame:
    files = sorted(RAW.glob(f"symbol={sym}/year=*/data.csv"))
    dfs = [pd.read_csv(f, parse_dates=["Date"], index_col="Date") for f in files]
    return pd.concat(dfs).sort_index()

def _save_partitions(df: pd.DataFrame, sym: str) -> None:
    for y, part in df.groupby(df.index.year):
        out_dir = OUT / f"symbol={sym}" / f"year={int(y)}"
        out_dir.mkdir(parents=True, exist_ok=True)
        part.to_parquet(out_dir / "data.parquet")
        print(f"[WRITE] {sym} {y} -> {out_dir/'data.parquet'}")

def main():
    gspc = _load_symbol("GSPC")
    gold = _load_symbol("GC_F")

    gspc["daily_return"] = gspc["Adj_Close"].pct_change()
    gold["daily_return"] = gold["Adj_Close"].pct_change()

    gspc["vol_30d"] = gspc["daily_return"].rolling(30, min_periods=10).std()
    gold["vol_30d"] = gold["daily_return"].rolling(30, min_periods=10).std()

    gspc["price_norm"] = gspc["Adj_Close"] / gspc["Adj_Close"].iloc[0] * 100
    gold["price_norm"] = gold["Adj_Close"] / gold["Adj_Close"].iloc[0] * 100
    corr = gspc["daily_return"].rolling(30, min_periods=10).corr(gold["daily_return"])

    gspc["vol_30d_ann"] = gspc["vol_30d"] * (252**0.5)
    gold["vol_30d_ann"] = gold["vol_30d"] * (252**0.5)

    ann_returns = gspc["daily_return"].resample("Y").sum()
    print("[INFO] Annual returns (SPX):", ann_returns.tail())

    out_corr = []
    corr = gspc["daily_return"].rolling(30).corr(gold["daily_return"])
    for y, part in corr.groupby(corr.index.year):
        out_dir = OUT / "correlation" / f"year={int(y)}"
        out_dir.mkdir(parents=True, exist_ok=True)
        part.to_frame("rolling_corr_30d").to_parquet(out_dir / "data.parquet")
        print(f"[WRITE] CORR {y} -> {out_dir/'data.parquet'}")

    _save_partitions(gspc, "GSPC")
    _save_partitions(gold, "GC_F")
    print("[DONE] Local transforms completed.")

if __name__ == "__main__":
    main()
