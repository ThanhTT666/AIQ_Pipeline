# 🌍 AIQ (Air Intelligence Quality) - Data Engineering Project

## 📋 Project Overview

AIQ là một data pipeline xử lý dữ liệu chất lượng không khí từ 7 thành phố châu Á:
- Bangkok, Beijing, Hanoi, Ho Chi Minh, Jakarta, Nantong, Singapore

**Timeline:** Aug 2022 - Feb 2026 | **Total Records:** 9,085 observations

---

## 🏗️ Architecture & Data Flow

```
RAW DATA (CSV)
    ↓
[Phase 1: Ingestion] → Load all CSV files + Extract city info
    ↓
[Phase 2: Processing] → Clean data (duplicates, date parsing)
    ↓
[Phase 2.5: Validation] → Data quality checks → JSON report
    ↓
├─→ [Phase 3a: Parquet Data Lake] → Partition by city
├─→ [Phase 3b: Analytics] → Summary metrics CSV
└─→ [Phase 4: DuckDB Warehouse] → 3 tables (fact/dimension/aggregates)
    ↓
[Reporting] → HTML Report
```

---

## 🔧 DE Responsibilities & Work Done

### **1. Data Ingestion**
- ✅ Load CSV files từ 7 folder khác nhau (AIQ_datasets/Bangkok/, Beijing/, ...)
- ✅ Automatically detect city từ folder path
- ✅ Handle multiple CSV files per city
- ✅ Aggregate vào single pandas DataFrame

**File:** `src/ingestion/load_data.py`

### **2. Data Cleaning & Transformation**
- ✅ Remove duplicates
- ✅ Parse dates với multiple formats (M/D/YYYY, YYYY-MM-DD)
- ✅ Standardize dates → ISO format (YYYY-MM-DD)
- ✅ Handle missing/invalid data gracefully

**File:** `src/processing/clean_data.py`

### **3. Data Quality Assurance**
- ✅ Row count validation
- ✅ Column statistics
- ✅ Missing value analysis
- ✅ Duplicate detection
- ✅ Generate JSON quality report

**File:** `src/quality/data_quality.py`

### **4. Data Lake (Parquet)**
- ✅ Export data → Parquet format (columnar, compressed)
- ✅ Partition by city (city=Bangkok/, city=Beijing/, ...)
- ✅ Efficient storage → ~50-70% compression
- ✅ Optimized for analytics & big data processing

**File:** `src/processing/parquet_lake.py`

### **5. Data Warehouse (DuckDB)**
- ✅ Create OLAP database (optimized for analytics)
- ✅ 3-table schema:
  - `fact_air_quality` - Raw observations (9,085 rows)
  - `dim_city` - City dimension (7 cities)
  - `daily_city_metrics` - Pre-aggregated daily metrics by city
- ✅ Auto-detect numeric columns → dynamic aggregations
- ✅ Fast analytical queries without re-computation

**File:** `src/processing/duckdb_warehouse.py`

### **6. Logging & Monitoring**
- ✅ Structured logging (info, warning, error)
- ✅ Log file output (`logs/pipeline.log`)
- ✅ Pipeline progress tracking

**File:** `src/quality/logger.py`

### **7. Reporting**
- ✅ HTML report generation (responsive design)
- ✅ Interactive tables & statistics
- ✅ Data lake & warehouse overview
- ✅ City-level breakdown

**File:** `src/inspection/generate_report.py`

### **8. Orchestration**
- ✅ End-to-end pipeline (7 phases)
- ✅ Error handling & validation at each step
- ✅ Logging & reporting integration

**File:** `src/pipeline/run_pipeline.py`

---

## 📂 Project Structure

```
AIQ_code/
├── src/
│   ├── ingestion/          # Data loading
│   │   └── load_data.py
│   ├── pipeline/           # Main orchestration
│   │   └── run_pipeline.py
│   ├── processing/         # Data transformation
│   │   ├── clean_data.py
│   │   ├── transform_data.py
│   │   ├── parquet_lake.py
│   │   └── duckdb_warehouse.py
│   ├── quality/            # QA & validation
│   │   ├── data_quality.py
│   │   └── logger.py
│   └── inspection/         # Reporting
│       ├── generate_report.py
│       └── inspect_results.py
│
├── data/
│   ├── processed/          # Cleaned CSV
│   │   └── clean_dataset.csv
│   └── analytics/          # Summary metrics
│       └── summary_metrics.csv
│
├── data_lake/              # Parquet files (partitioned by city)
│   ├── city=Bangkok/
│   ├── city=Beijing/
│   └── ... (7 partitions)
│
├── warehouse/              # DuckDB database
│   └── air_quality.duckdb
│
├── reports/                # HTML reports
│   └── pipeline_report.html ⭐
│
├── logs/                   # Pipeline logs
│   └── pipeline.log
│
└── requirements.txt        # Dependencies
```

---

## 🚀 How to Run

### **Setup (First Time)**
```powershell
cd d:\AIQ_pj
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r AIQ_code\requirements.txt
pip install duckdb pyarrow
```

### **Run Pipeline**
```powershell
cd AIQ_code\src\pipeline
python run_pipeline.py
```

### **Generate Report**
```powershell
cd ..\inspection
python generate_report.py
```

### **Outputs**
- ✅ `data/processed/clean_dataset.csv` - Cleaned data
- ✅ `data_lake/city=*/` - Parquet partitions
- ✅ `warehouse/air_quality.duckdb` - DuckDB database
- ✅ `data/analytics/summary_metrics.csv` - Aggregates
- ✅ `reports/pipeline_report.html` - Visual report ⭐
- ✅ `logs/pipeline.log` - Execution logs

---

## 📊 Key Metrics

| Metric | Value |
|--------|-------|
| Total Records | 9,085 |
| Cities | 7 |
| Date Range | 2022-08-01 to 2026-02-18 |
| Columns | 12 (11 numeric + city) |
| Data Lake Size | ~2.5MB (Parquet) |
| Warehouse Size | ~1.2MB (DuckDB) |
| Pipeline Runtime | ~3-5 seconds |

---

## 🎯 DE Concepts Demonstrated

✅ **Data Ingestion** - Multi-source CSV loading  
✅ **ETL** - Extract, Transform, Load pipeline  
✅ **Data Cleaning** - Handle date formats, duplicates, missing values  
✅ **Data Validation** - Quality checks & reporting  
✅ **Data Lake** - Columnar storage (Parquet), partitioning  
✅ **Data Warehouse** - OLAP design, fact/dimension tables, aggregations  
✅ **Logging** - Structured logging, error tracking  
✅ **Orchestration** - End-to-end pipeline automation  
✅ **Reporting** - HTML reports, data visualization  

---

## 🔍 Next Steps (Optional Enhancements)

- [ ] Add data visualization dashboard (Plotly/Dash)
- [ ] Implement incremental loading (Delta Lake)
- [ ] Add data lineage tracking
- [ ] Create REST API for data access
- [ ] Add data profiling & anomaly detection
- [ ] Implement dbt for data transformation
- [ ] Deploy to cloud (Azure/AWS)

---

**Project Status:** ✅ Complete  
**Last Updated:** March 9, 2026
