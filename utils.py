import pandas as pd
import sqlite3

# Step 1: Read CSV File
file_path = 'data/waveplnfile.csv'
df = pd.read_csv(file_path, low_memory=False, dtype=str)

# Step 2: Adjust Duplicate Column Names
columns = df.columns.tolist()
unique_columns = []
column_count = {}

for col in columns:
    if col in column_count:
        column_count[col] += 1
        unique_columns.append(f"{col}_{column_count[col]}")
    else:
        column_count[col] = 0
        unique_columns.append(col)

df.columns = unique_columns

# Step 3: Generate SQL Statements
table_name = 'data_dump_raw'
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    {', '.join([f'{col} TEXT' for col in unique_columns])}
)
"""

insert_query = f"""
INSERT INTO {table_name} ({', '.join(unique_columns)})
VALUES ({', '.join(['?' for _ in unique_columns])})
"""

# Step 4: Connect to SQLite
db_path = 'kp_p2p.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Step 5: Create Table
cursor.execute(create_table_query)

# Step 6: Insert Data
for i, row in df.iterrows():
    cursor.execute(insert_query, tuple(row[col] for col in unique_columns))

conn.commit()
cursor.close()
conn.close()
