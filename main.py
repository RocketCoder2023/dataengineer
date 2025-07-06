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
import pandas as pd
import os
import re
import json

# Pre-compile regex patterns for efficiency (used repeatedly)
SINGLE_QUOTE = re.compile("'")
POSTCODE = re.compile(r'"post code":\s*([0-9]{1,5}-[0-9]{1,5})')
TRAILING_COMMA = re.compile(r',\s*}')

def read_data(input_path):
    """Read mock data from CSV file."""
    return pd.read_csv(input_path)

def transform_data(df):
    """
    Drop duplicate rows.
    Lowercase all string columns for normalization.
    """
    df = df.drop_duplicates()  # Remove duplicate rows
    # Lowercase string columns only
    string_cols = df.select_dtypes(include='object').columns
    for col in string_cols:
        if df[col].notnull().any():  # Only process if column isn't all NaN
            df[col] = df[col].str.lower()
    return df

def sink_to_db(df, db_url, table_name='mock_data'):
    """
    Write DataFrame to a SQLite database table.
    Overwrites the table if it exists.
    """
    from sqlalchemy import create_engine
    engine = create_engine(db_url)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Data written to {table_name} in {db_url}")

def fix_braces(addr_str):
    """
    If string has more opening than closing braces, append a closing brace.
    This handles common data errors from cut-off dicts.
    """
    if isinstance(addr_str, str) and addr_str.count('{') > addr_str.count('}'):
        return addr_str + '}'
    return addr_str

def parse_address_field(addr_str):
    """
    Parse an address field from a string to a dictionary of its components.
    Handles common formatting issues (single/double quotes, postcode as string, trailing comma).
    Returns a dict with keys: street, city, post_code, address_country.
    Returns None if parsing fails.
    """
    try:
        addr_str = fix_braces(addr_str)  # Try to auto-fix missing closing brace
        # Must be a non-null string starting with '{'
        if not isinstance(addr_str, str) or not addr_str.strip().startswith('{'):
            return None
        # Ensure curly braces match
        if addr_str.count('{') != addr_str.count('}'):
            return None
        # Fix single quotes, ensure post code is a string, fix trailing comma
        fixed = SINGLE_QUOTE.sub('"', addr_str)
        fixed = POSTCODE.sub(r'"post code": "\1"', fixed)
        fixed = TRAILING_COMMA.sub('}', fixed)
        # Parse as JSON
        d = json.loads(fixed)
        address = d.get('address', {})
        return {
            'street': address.get('streeet', None),
            'city': address.get('city', None),
            'post_code': address.get('post code', None),
            'address_country': address.get('country', None)
        }
    except Exception:
        return None  # Any error → treat as malformed

def parse_address_column(df, address_col='address'):
    """
    Parse address column in the dataframe.
    - Extracts address fields into new columns (street, city, post_code, address_country)
    - Logs malformed addresses and writes them to broken_addresses.csv
    Returns DataFrame with new columns.
    """
    # Parse every address entry; returns list of dicts or None
    address_parsed = df[address_col].apply(parse_address_field)
    addr_df = pd.DataFrame(address_parsed.tolist())  # Convert list of dicts to DataFrame

    # Find which rows failed to parse (all fields None)
    malformed_mask = addr_df.isnull().all(axis=1)
    num_malformed = malformed_mask.sum()
    print(f"\nMalformed address rows: {num_malformed}")
    if num_malformed:
        # Print up to 3 examples of malformed address values
        first_bad = df[address_col][malformed_mask].head(3)
        print("First 3 malformed address samples:")
        for idx, sample in first_bad.items():
            print(f"Row {idx}: {str(sample)[:120]}")
        # Save all bad rows to a CSV for review
        df[malformed_mask].to_csv("broken_addresses.csv", index=False)
        print(f"All malformed rows saved to 'broken_addresses.csv'.")
    else:
        print("No malformed addresses detected.")
    # Add each address field as a new column in the main DataFrame
    for col in ['street', 'city', 'post_code', 'address_country']:
        df[col] = addr_df[col]
    return df

def main():
    """
    Main ETL pipeline:
    1. Read data
    2. Clean column names
    3. Parse address fields
    4. Normalize and deduplicate data
    5. Filter for valid address records
    6. Save malformed addresses for review
    7. Sink good records to the database
    """
    data_path = os.path.join(os.path.dirname(__file__), 'data', 'mock_dataset.csv')
    db_url = 'sqlite:///db.sqlite'

    df = read_data(data_path)
    # Clean column names: remove spaces, lowercase, deduplicate
    df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    print("Columns after cleaning:", df.columns.tolist())

    if 'address' in df.columns:
        df = parse_address_column(df, address_col='address')
        # Show first few parsed address records
        print(df[['street', 'city', 'post_code', 'address_country']].head())
    else:
        print("No 'address' column found after cleaning.")

    # Lowercase and deduplicate data
    df = transform_data(df)

    # Select "good" rows: at least one address field is present
    is_good = df[['street', 'city', 'post_code', 'address_country']].notnull().any(axis=1)
    df_good = df[is_good]
    bad_count = (~is_good).sum()
    print(f"Data ready. {len(df_good)} good rows, {bad_count} bad rows written to 'broken_addresses.csv'.")

    # Write good rows to the database
    sink_to_db(df_good, db_url)

if __name__ == "__main__":
    main()
