import pandas as pd
import csv
from fuzzywuzzy import process

def clean_column_header(column_header, is_db_column=False):
    """Cleans column headers by removing leading/trailing spaces, first comma, and brackets."""
    if pd.isna(column_header):
        return ""
    cleaned = str(column_header).strip().replace(",", "").strip("[]")
    if is_db_column:
        # Remove the first comma and opening bracket for DB columns
        cleaned = cleaned.lstrip(',').lstrip('[')
    return cleaned

def create_lookup_table(data):
    lookup_table = {}
    unmatched_db_cols = []
    unmatched_excel_cols = []

    # Clean DB columns first
    db_cols = [clean_column_header(col, is_db_column=True) for col in data['Database Column']]
    excel_cols = [clean_column_header(col) for col in data['Excel Column']]

    # Find exact matches
    for db_col, excel_col in zip(db_cols, excel_cols):
        if db_col and excel_col:
            if db_col == excel_col:
                lookup_table[excel_col] = db_col
            else:
                unmatched_db_cols.append(db_col)
                unmatched_excel_cols.append(excel_col)

    # Fuzzy match remaining columns
    for excel_col in unmatched_excel_cols:
        best_match, score = process.extractOne(excel_col, unmatched_db_cols)
        if score >= 80:  # You can adjust this threshold
            lookup_table[excel_col] = best_match
            unmatched_db_cols.remove(best_match)
        else:
            print(f"No good match found for Excel column: '{excel_col}'")

    return lookup_table

try:
    print("Starting to read the CSV file...")
    data = pd.read_csv("data/headers.csv", sep='|', header=None, names=['Excel Column', 'Database Column'], dtype=str)
    print(f"Data shape: {data.shape}")
    print("First few rows of data:")
    print(data.head())

    lookup_table = create_lookup_table(data)

    print("\nLookup table created. Results:")
    for excel_col, db_col in lookup_table.items():
        print(f"'{excel_col}': '{db_col}',")

    if not lookup_table:
        print("Warning: Lookup table is empty!")

    print(f"\nTotal rows in data: {len(data)}")
    print(f"Total items in lookup table: {len(lookup_table)}")
    print("First 10 items in lookup table:")
    for i, (key, value) in enumerate(list(lookup_table.items())[:10]):
        print(f"{i+1}. '{key}': '{value}'")

    # Write the lookup table to a file
    with open('lookup_table.py', 'w') as f:
        f.write("lookup_table = {\n")
        for key, value in lookup_table.items():
            f.write(f"    '{key}': '{value}',\n")
        f.write("}\n")

    print("\nLookup table has been written to 'lookup_table.py'")

except (csv.Error, ValueError) as e:
    print(f"Error reading CSV file: {e}")
except pd.errors.ParserError as e:
    print(f"Pandas error reading CSV file: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")