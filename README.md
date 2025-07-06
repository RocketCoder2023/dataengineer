# P.BAZANOV DATA ENGINEER
# 07.07.2025
# Version 1.0
#
# Data Cleaning & ETL Pipeline for Mock Address Data
# - Reads CSV, normalizes column names, deduplicates records
# - Parses nested address field into separate columns (street, city, post_code, country)
# - Handles malformed/broken address data robustly
# - Exports valid records to SQLite database
# - Saves malformed rows for review
# ------------------------------------------------------------------
# Data Engineer Exercise Solution

bynd_data_engineer/
├── data/
│   └── mock_data.csv
├── pipeline/
│   ├── __init__.py
│   └── main.py
├── requirements.txt
├── Dockerfile
├── README.md
└── setup_local.sh



## Overview

This repository demonstrates a simple data pipeline:
- Reads data from `data/mock_data.csv`
- Transforms the data (deduplicate, normalization)
- Sinks to a SQLite database

## Setup (Local)
1. **Clone repo**
2. **Run setup:**
bash setup_local.sh
3. **Run pipeline:**
source venv/bin/activate
python pipeline/main.py

## Docker

1. **Build:**
docker build -t bynd_data_engineer .
2. **Run:**
docker run --rm -v $(pwd)/data:/app/data bynd_data_engineer


