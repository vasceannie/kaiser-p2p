import pandas as pd
import numpy as np
from utils.import_husk import DataExtractor
from data.column_mapping import column_mapping, additional_columns

def update_supplier_data(source_file, reference_file, output_file):
    # Initialize DataExtractor for the source file
    extractor = DataExtractor()
    extractor.set_source(source_file)

    # Load the supplier data from the source file
    extractor.find_file()
    extractor.load_data(headers=list(column_mapping.keys()) + additional_columns, start_row=2, header_row=0)
    supplier_df = extractor.get_dataframe()

    # Rename columns based on the mapping
    supplier_df.rename(columns=column_mapping, inplace=True)

    # Load the reference sheet (the_sheet)
    reference_df = pd.read_excel(reference_file)

    # Convert 'Supplier ID' to string and remove decimal points in both DataFrames
    supplier_df['Supplier ID'] = supplier_df['Supplier ID'].astype(str).str.replace('.0', '', regex=False)
    reference_df['Supplier ID'] = reference_df['Supplier ID'].astype(str).str.replace('.0', '', regex=False)

    # Remove any non-numeric Supplier IDs from reference_df
    reference_df = reference_df[reference_df['Supplier ID'].str.isnumeric()]

    # Create a new DataFrame for the output
    output_df = supplier_df.copy()

    # Create columns to track changes
    for col in reference_df.columns:
        if col in output_df.columns:
            output_df[f'{col}_changed'] = False

    # Add a column to mark rows with changes
    output_df['row_changed'] = False

    # Update values and mark changes
    for index, row in reference_df.iterrows():
        supplier_id = row['Supplier ID']
        if supplier_id in output_df['Supplier ID'].values:
            for col in reference_df.columns:
                if col in output_df.columns:
                    new_value = row[col]
                    old_value = output_df.loc[output_df['Supplier ID'] == supplier_id, col].iloc[0]
                    if pd.notna(new_value) and not pd.isna(new_value):
                        if isinstance(new_value, pd.Series):
                            new_value = new_value.iloc[0]
                        if isinstance(old_value, pd.Series):
                            old_value = old_value.iloc[0]
                        if new_value != old_value:
                            output_df.loc[output_df['Supplier ID'] == supplier_id, col] = new_value
                            output_df.loc[output_df['Supplier ID'] == supplier_id, f'{col}_changed'] = True
                            output_df.loc[output_df['Supplier ID'] == supplier_id, 'row_changed'] = True

    # Filter the DataFrame to include only rows with changes
    changed_rows_df = output_df[output_df['row_changed']]

    # Drop the 'row_changed' column before exporting
    changed_rows_df = changed_rows_df.drop(columns=['row_changed'])

    # Export the filtered DataFrame
    changed_rows_df.to_excel(output_file, index=False)

    # Diagnostic information
    print(f"Number of suppliers in source file: {len(supplier_df)}")
    print(f"Number of suppliers in reference file: {len(reference_df)}")
    print(f"Number of suppliers in output file: {len(changed_rows_df)}")

    # Count and display the number of changes
    change_counts = changed_rows_df[[col for col in changed_rows_df.columns if col.endswith('_changed')]].sum()
    print("\nNumber of changes per column:")
    print(change_counts)

    print(f"\nUpdated data saved to {output_file}")

if __name__ == "__main__":
    source_file = 'data/the_data.xlsm'
    reference_file = 'data/the_sheet.xlsx'
    output_file = 'data/updated_supplier_enablement_tracking-2.xlsx'

    update_supplier_data(source_file, reference_file, output_file)