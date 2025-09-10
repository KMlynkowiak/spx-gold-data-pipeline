from __future__ import annotations
from pathlib import Path
import pandas as pd
import kagglehub

OUT = Path("data/raw")

DATASETS = {
    "GSPC": "joebeachcapital/dow-jones-and-s-and-p500-indices-daily-update",  # FRED: DATE, SP500
    "GC_F": "lbronchal/gold-and-silver-prices-dataset",                       # LBMA: różne kolumny cen
}

def _list_csvs(root: Path) -> list[Path]:
    return sorted(root.rglob("*.csv"))

def _read_csv_any(path: Path) -> pd.DataFrame:
    # spróbuj standard, a jak nie – średnik
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.read_csv(path, sep=";")

def _standardize_generic(df: pd.DataFrame) -> pd.DataFrame:
    """Standaryzacja do: Open, High, Low, Close, Adj_Close, Volume z index=Date."""
    orig_cols = list(df.columns)
    df = df.rename(columns=lambda c: c.replace(" ", "_"))
    cols = {c.lower(): c for c in df.columns}

    # Date / DATE
    date_c = (
        cols.get("date")
        or ("DATE" if "DATE" in df.columns else None)
        or cols.get("observation_date")            # <-- FRED (SP500.csv)
    )
    if date_c is None:
        raise ValueError(f"Brak kolumny daty (Date/DATE/observation_date) w {orig_cols}")
    df["Date"] = pd.to_datetime(df[date_c])

    # Kandydaci na „Close”
    priority = [
        "close", "adj_close", "price", "value",
        "sp500", "spx", "gold", "xauusd",
        "usd_(pm)", "usd_(am)", "usd"
    ]
    close_c = None
    for k in priority:
        if k in cols:
            close_c = cols[k]
            break
    # jeśli nadal nie znaleziono – wybierz pierwszą liczbową kolumnę ≠ Date
    if close_c is None:
        numeric = df.select_dtypes("number").columns.tolist()
        if not numeric:
            raise ValueError(f"Nie znaleziono kolumny liczbowej dla Close w {orig_cols}")
        close_c = numeric[0]

    out = pd.DataFrame(index=df["Date"])
    out["Close"] = pd.to_numeric(df[close_c], errors="coerce")

    # OHLC: jeśli brak, wypełnij Close
    open_c = cols.get("open")
    high_c = cols.get("high")
    low_c  = cols.get("low")
    out["Open"]  = pd.to_numeric(df[open_c], errors="coerce") if open_c else out["Close"]
    out["High"]  = pd.to_numeric(df[high_c], errors="coerce") if high_c else out["Close"]
    out["Low"]   = pd.to_numeric(df[low_c],  errors="coerce") if low_c  else out["Close"]

    # Adj_Close: jeśli brak – równe Close
    adj_c = cols.get("adj_close") or ("Adj_Close" if "Adj_Close" in df.columns else None)
    out["Adj_Close"] = pd.to_numeric(df[adj_c], errors="coerce") if (adj_c and adj_c in df.columns) else out["Close"]

    # Volume: jeśli brak – 0
    vol_c = cols.get("volume") or ("Volume" if "Volume" in df.columns else None)
    out["Volume"] = pd.to_numeric(df[vol_c], errors="coerce") if (vol_c and vol_c in df.columns) else 0

    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out

def _save_partitions(df: pd.DataFrame, sym: str) -> None:
    for y, part in df.groupby(df.index.year):
        out_dir = OUT / f"symbol={sym}" / f"year={int(y)}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "data.csv").write_text(part.to_csv(index_label="Date"), encoding="utf-8")
        print(f"[WRITE] {sym} {y} -> {out_dir/'data.csv'}")

def ingest_symbol(sym: str, slug: str) -> None:
    print(f"[DL] {sym} <- {slug}")
    ds_path = Path(kagglehub.dataset_download(slug))
    csvs = _list_csvs(ds_path)
    if not csvs:
        raise FileNotFoundError(f"Brak plików CSV w: {ds_path}")

    # heurystyka wyboru pliku: najpierw nazwa, potem kolumny
    pick: Path | None = None
    if sym == "GSPC":
        # preferuj plik z 'sp500' w nazwie (FRED)
        for p in csvs:
            if "sp500" in p.name.lower():
                pick = p; break
    elif sym == "GC_F":
        # preferuj 'gold' / 'xau' w nazwie
        for p in csvs:
            n = p.name.lower()
            if "gold" in n or "xau" in n:
                pick = p; break
    # jeśli nie znaleziono po nazwie – bierz pierwszy
    if pick is None:
        pick = csvs[0]

    print(f"[CSV] {sym}: {pick}")
    df = _read_csv_any(pick)

    # specyficzne mapowania (FRED SP500: kolumny DATE, SP500)
    cols_lower = {c.lower(): c for c in df.columns}
    if sym == "GSPC" and "sp500" in cols_lower and ("date" in cols_lower or "DATE" in df.columns):
        # zrób z SP500 „Close”
        df_std = df.rename(columns={"SP500": "Close", "sp500": "Close"})
        # reszta ustandaryzuje się w generic
    else:
        df_std = df

    std = _standardize_generic(df_std)
    _save_partitions(std, sym)

def main() -> None:
    ingest_symbol("GSPC", DATASETS["GSPC"])
    ingest_symbol("GC_F", DATASETS["GC_F"])
    print("[DONE] Kaggle API ingestion finished → data/raw/")

if __name__ == "__main__":
    main()
