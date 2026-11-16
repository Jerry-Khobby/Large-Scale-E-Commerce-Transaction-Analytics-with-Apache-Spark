# Large-Scale E-Commerce Transaction Analytics with Apache Spark

Generates a synthetic e-commerce dataset (10M+ transactions, multiple GB of Parquet)
with Faker, shows why Pandas struggles with it, then cleans, analyses and optimises
it with PySpark. See [specification.md](specification.md) for the full brief.

## Project structure

```
.
├── data/                      # generated, git-ignored (folders kept with .gitkeep)
│   ├── raw/                   # Stage 1 output - Parquet partitioned by year_month
│   ├── cleaned/               # Stage 5 output - Parquet partitioned by year/month
│   └── analytics/             # Stage 6 output - one CSV folder per query
├── src/
│   ├── config.py              # paths, dataset distributions, Spark settings
│   ├── utils.py               # logger, SparkSession builder, timer, CSV export
│   ├── generate_data.py       # Stage 1   - data generation
│   ├── pandas_test.py         # Stage 2   - Pandas limitation demo
│   ├── spark_cleaning.py      # Stage 3-5 - load, quality report, cleaning
│   ├── spark_analytics.py     # Stage 6   - 5 query groups + Spark SQL
│   ├── spark_optimization.py  # Stage 7   - caching / broadcast / parallelism
│   └── visualizations.py      # Stage 8   - charts
├── notebooks/                 # exploratory notebooks (scripts are the source of truth)
├── outputs/
│   ├── visualizations/        # PNG charts
│   └── reports/               # quality report, timings, explain plans, screenshots
├── logs/                      # one log file per script
├── PROJECT_REPORT.md
├── requirements.txt
└── specification.md
```

## Setup

~15 GB free disk is needed for the full dataset.

### Option A - Docker (recommended, especially on Windows)

The image bundles Python 3.12, a Java runtime and all requirements, so there is no
local Java/Hadoop setup. The project folder is mounted at `/app`, so data and
outputs appear in your local folders.

```bash
docker compose build
docker compose run --rm spark python src/generate_data.py --records 25000000
```

Prefix every command in the table below with `docker compose run --rm spark`.
The Spark UI is at <http://localhost:4040> while a job runs.

### Option B - local Python

Requirements: Python 3.10+, **Java 17 or 21** (needed by PySpark 4.x).

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Running Spark natively on Windows is not recommended: pyspark 4.1.0 fails to import
there (`socketserver.UnixStreamServer`), and writing files needs `winutils.exe` /
`HADOOP_HOME`. Use Docker instead. `generate_data.py` and `pandas_test.py` do not use
Spark and run fine natively.

## How to run

All scripts use paths relative to the project root, so you can run them from anywhere.
Run them in order:

| Step | Command | Output |
|------|---------|--------|
| 1. Generate data | `python src/generate_data.py --records 20000000` | `data/raw/`, `logs/data_generation.log` |
| 2. Pandas test | `python src/pandas_test.py` | `outputs/reports/pandas_test_results.csv` |
| 3-5. Clean | `python src/spark_cleaning.py` | `data/cleaned/`, `outputs/reports/data_quality_report.csv`, `cleaning_summary.csv` |
| 6. Analytics | `python src/spark_analytics.py` | `data/analytics/*`, `outputs/reports/analytics_timings.csv` |
| 7. Optimisation | `python src/spark_optimization.py` | `outputs/reports/performance_results.csv`, `explain_*.txt` |
| 8. Charts | `python src/visualizations.py` | `outputs/visualizations/*.png` |

Useful flags:
- `generate_data.py --records N --batch-size N --seed N --overwrite`
- `pandas_test.py --sample-rows N --skip-full`
- `spark_optimization.py --skip-parallelism`

Tip: do a quick end-to-end smoke test with `--records 200000` before generating the full dataset.

## Dataset

14 columns per spec section 3.1. Transactions are spread over 2022-01-01 to 2024-12-31,
8 product categories, 4 payment methods (40/30/20/10 %) and an 85/15 SUCCESS/FAILED
split. Intentional quality issues: ~4 % missing city and ~4 % missing email, ~1.5 %
duplicate transactions, 0.5 % negative price/quantity, 2 % wrong `total_amount`.

## Results

_Fill in after running the pipeline._

| Metric | Value |
|--------|-------|
| Records generated | |
| Raw size on disk | |
| Generation time | |
| Rows after cleaning | |
| Data-quality score | |

### Pandas vs Spark

| Test | Rows | Load time | Memory | Result |
|------|------|-----------|--------|--------|
| Pandas - 1M sample | | | | |
| Pandas - full dataset | | | | |
| Spark - full dataset | | | | |

### Performance optimisation

See `outputs/reports/performance_results.csv`.

## Technologies

Python, PySpark 4.1, Faker, PyArrow/Parquet, Pandas, Matplotlib.
