#!/bin/bash
# Create virtual env and install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# Setup local database (SQLite used for simplicity)
echo "Local SQLite DB at db.sqlite"
