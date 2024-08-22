import pandas as pd
import pyodbc
from data.column_mapping import get_column_mapping

def safe_convert(value, target_type):
    if pd.isna(value):
        return None
    try:
        if target_type == float:
            value = ''.join(c for c in str(value) if c.isdigit() or c == '.')
        return target_type(value) if value != '' else None
    except ValueError:
        return None

def compare_and_update_db(csv_file):
    # Read the pipe-delimited CSV file
    df = pd.read_csv(csv_file, sep='|', header=2, dtype=str)

    conn = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=localhost;"
        "Database=kp_p2p;"
        "Trusted_Connection=yes;"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'KP_Master_Raw'")
    db_columns = [row.COLUMN_NAME for row in cursor.fetchall() if row.COLUMN_NAME != 'SupplierID']

    column_mapping = get_column_mapping()
    reverse_mapping = {v: k for k, v in column_mapping.items()}

    # Filter db_columns to only include those in reverse_mapping
    db_columns = [col for col in db_columns if col in reverse_mapping]

    # Construct a fixed UPDATE query with all valid columns
    update_query = "UPDATE KP_Master_Raw SET " + ", ".join([f"{col} = ?" for col in db_columns]) + " WHERE SupplierID = ?"
    print(f"Number of columns in update query: {update_query.count('?') - 1}")  # -1 for SupplierID

    discrepancies = []

    for index, row in df.iterrows():
        excel_supplier_id = row["Supplier ID"]
        update_values = []

        for db_col in db_columns:
            excel_col = reverse_mapping[db_col]
            if excel_col in df.columns:
                value = row[excel_col]
                converted_value = safe_convert(value, float) if 'Amount' in db_col or 'Vchr' in db_col else value
                update_values.append(converted_value)
            else:
                update_values.append(None)

        update_values.append(excel_supplier_id)  # Add SupplierID at the end
        print(f"Number of values being updated: {len(update_values) - 1}")  # -1 for SupplierID

        try:
            cursor.execute(update_query, *update_values)
        except pyodbc.Error as e:
            discrepancies.append(f"Supplier ID {excel_supplier_id}: Error updating - {str(e)}")

    conn.commit()
    conn.close()

    if discrepancies:
        print("\nDiscrepancies found:")
        for discrepancy in discrepancies:
            print(discrepancy)
    else:
        print("\nNo discrepancies found.")

    with open('discrepancies.txt', 'w') as f:
        for discrepancy in discrepancies:
            f.write(f"{discrepancy}\n")

# Example usage
compare_and_update_db("data/sample.csv")