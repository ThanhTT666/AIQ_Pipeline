# 🌍 AIQ (Air Intelligence Quality) Pipeline

A production-grade data engineering pipeline for processing air quality data from 7 Asian cities using Parquet data lake and DuckDB warehouse.

## 📊 Dataset Overview

- **Cities**: Bangkok, Beijing, Hanoi, Ho Chi Minh, Jakarta, Nantong, Singapore
- **Time Period**: August 2022 - February 2026
- **Records**: 9,085 observations
- **Features**: 11 air quality metrics (PM2.5, PM10, CO, NO2, SO2, O3, etc.)
- **Size**: ~2.6 MB (lightweight - included in repo)

## 🏗️ Architecture

```
RAW DATA (CSV) → INGESTION → CLEANING → VALIDATION → DATA LAKE (Parquet) + WAREHOUSE (DuckDB) → REPORTS (HTML)
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Git

### Installation

```bash
# Clone repository
git clone https://github.com/ThanhTT666/AIQ-Pipeline.git
cd AIQ-Pipeline

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.\.venv\Scripts\Activate.ps1
# On macOS/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install duckdb pyarrow
```

### Run Pipeline

```bash
# Navigate to pipeline directory
cd src/pipeline

# Execute pipeline (all 7 phases)
python run_pipeline.py
```

### Generate Report

```bash
# From project root
cd src/inspection
python generate_report.py

# Open the HTML report in browser
# File location: ../../../reports/pipeline_report.html
```

## 📂 Project Structure

```
AIQ-Pipeline/
├── AIQ_datasets/              # Source data (included - 2.6 MB)
│   ├── Bangkok/
│   ├── Beijing/
│   ├── Hanoi/
│   ├── HoChiMinh/
│   ├── Jakarta/
│   ├── Nantong/
│   └── Singapore/
│
├── src/
│   ├── ingestion/             # Data loading
│   │   └── load_data.py
│   ├── pipeline/              # Orchestration
│   │   └── run_pipeline.py
│   ├── processing/            # Transformations
│   │   ├── clean_data.py
│   │   ├── transform_data.py
│   │   ├── parquet_lake.py
│   │   └── duckdb_warehouse.py
│   ├── quality/               # Validation & logging
│   │   ├── data_quality.py
│   │   └── logger.py
│   └── inspection/            # Reporting
│       ├── generate_report.py
│       └── inspect_results.py
│
├── requirements.txt
├── README.md
├── PROJECT_SUMMARY.md
└── .gitignore
```

## 🔄 Pipeline Phases

| Phase | Description | Output |
|-------|-------------|--------|
| 1 | Load CSV from 7 cities | Combined DataFrame (9,085 rows) |
| 2 | Clean & validate data | Deduplicated, standardized dates |
| 3a | Export to Parquet Data Lake | city=Bangkok/, city=Beijing/, ... |
| 3b | Calculate analytics | summary_metrics.csv |
| 4 | Load to DuckDB Warehouse | fact_air_quality, dim_city, daily_city_metrics |
| Report | Generate interactive dashboard | pipeline_report.html |

## 💡 Key Features

✅ **Relative Paths** - Works on Windows, macOS, Linux  
✅ **Automatic City Detection** - Extracts city from folder structure  
✅ **Multi-format Date Parsing** - Handles M/D/YYYY and YYYY-MM-DD  
✅ **Structured Logging** - Track pipeline execution  
✅ **Data Validation** - Quality checks at each phase  
✅ **Parquet Partitioning** - Efficient data lake organization  
✅ **DuckDB OLAP** - Fast analytical queries  
✅ **HTML Reports** - Interactive dashboards  

## 🛠️ Tech Stack

- **Python 3.8+** - Language
- **Pandas** - Data manipulation
- **PyArrow** - Parquet support
- **DuckDB** - OLAP database
- **Logging** - Pipeline monitoring

## 📈 Performance

- **Runtime**: 3-5 seconds
- **Data Lake Size**: ~2.5 MB (Parquet)
- **Warehouse Size**: ~1.2 MB (DuckDB)
- **Memory Usage**: <500 MB

## 📄 Documentation

See [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) for detailed architecture and DE concepts.

## 📄 License

MIT License

## 👤 Author

ThanhTT666

---

**Status**: ✅ Production Ready  
**Last Updated**: March 9, 2026
