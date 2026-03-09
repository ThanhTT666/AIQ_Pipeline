
# Air Quality Data Pipeline (Mini Data Engineering Project)

## Overview
This project demonstrates a simple data engineering pipeline:

raw CSV → ingestion → cleaning → validation → transformation → analytics dataset

## Project Structure

data/
    raw/            # put your original CSV folders here
    processed/      # cleaned dataset
    analytics/      # aggregated datasets

src/
    ingestion/      # load raw data
    processing/     # cleaning + transformations
    quality/        # data validation
    pipeline/       # pipeline runner

## Run Pipeline

1. Install dependencies

pip install -r requirements.txt

2. Place your folders of CSV files inside:

data/raw/

Example:

data/raw/hanoi/*.csv
data/raw/bangkok/*.csv

3. Run pipeline

python src/pipeline/run_pipeline.py

Outputs will appear in:

data/processed/
data/analytics/
