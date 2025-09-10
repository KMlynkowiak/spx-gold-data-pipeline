# SPX-GOLD Financial Data Pipeline (Kaggle Edition)

## Overview
This project builds a financial data pipeline analyzing S&P500 and Gold.
Data is ingested from Kaggle datasets (batch snapshots), transformed locally,
and stored in year-partitioned Parquet files.

## Project Flow
1. **Ingestion (Kaggle CLI)**  
   Download datasets with Kaggle CLI into `_kaggle_tmp/` and normalize them via `ingest_from_kaggle.py`.  
   Data is stored under:
   ```
   data/raw/symbol=GSPC/year=YYYY/data.csv
   data/raw/symbol=GC_F/year=YYYY/data.csv
   ```

2. **Transformation**  
   Run `transform_local.py` to compute:
   - daily returns,
   - 30-day rolling volatility,
   - annual returns,
   - 30-day rolling correlation (SPX vs Gold).

   Outputs stored as Parquet in:
   ```
   data/processed/symbol=GSPC/year=YYYY/data.parquet
   data/processed/symbol=GC_F/year=YYYY/data.parquet
   data/processed/correlation/year=YYYY/data.parquet
   ```

3. **Analysis**  
   Processed data can be queried locally or via Athena (future extension).

4. **Visualization**  
   Power BI dashboard: price trends, annual returns, volatility, rolling correlation.

## Requirements
- Python 3.10+
- Kaggle CLI configured with API token in `~/.kaggle/kaggle.json`

Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage
1. Download datasets:
```bash
kaggle datasets download -d paveljurke/s-and-p-500-gspc-historical-data -p ./_kaggle_tmp -o
kaggle datasets download -d guillemservera/precious-metals-data -p ./_kaggle_tmp -o
```

2. Extract zips to `_kaggle_tmp/`:
```bash
unzip ./_kaggle_tmp/*.zip -d ./_kaggle_tmp/
```

3. Normalize to raw partitions:
```bash
python ingest_from_kaggle.py
```

4. Transform to Parquet:
```bash
python transform_local.py
```

## Roadmap
- [x] Local Kaggle ingestion
- [x] Local ETL to Parquet
- [ ] Upload to S3 (raw + processed)
- [ ] Glue jobs (PySpark)
- [ ] Athena queries (COVID-2020 correlation)
- [ ] Power BI Dashboard
