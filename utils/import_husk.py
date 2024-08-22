import os
import pandas as pd

class DataExtractor:
    def __init__(self, directory=None, file_path=None, file_extensions=[".xlsm", ".xlsx", ".csv"]):
        self.directory = directory
        self.file_path = file_path
        self.file_extensions = file_extensions
        self.df = None

    def set_source(self, source):
        if os.path.isfile(source):
            self.file_path = source
            self.directory = None
        elif os.path.isdir(source):
            self.directory = source
            self.file_path = None
        else:
            raise ValueError("Invalid source. Please provide a valid file path or directory.")

    def find_file(self):
        if self.file_path:
            return self.file_path
        
        print(f"Searching in directory: {self.directory}")  # Debug statement
        found_files = []  # To store found files
        # Search for files with specified extensions
        for root, dirs, files in os.walk(self.directory):
            for file in files:
                if any(file.endswith(ext) for ext in self.file_extensions):
                    found_files.append(file)  # Collect found files
                    self.file_path = os.path.join(root, file)
                    print(f"File found: {self.file_path}")  # Debug statement
                    return self.file_path
        print(f"No files found with extensions {self.file_extensions} in {self.directory}")  # Debug statement
        return None

    def load_data(self, headers=None, start_row=0, start_col=0, header_row=0):
        if not self.file_path:
            raise FileNotFoundError("No file found in the specified directory.")
        
        print(f"Loading data from file: {self.file_path}")  # Debug statement

        if self.file_path.endswith(".csv"):
            self.df = pd.read_csv(self.file_path, skiprows=start_row, header=header_row, on_bad_lines='skip')
        else:
            self.df = pd.read_excel(self.file_path, skiprows=start_row, header=header_row)

        print(f"Data loaded successfully. DataFrame shape: {self.df.shape}")  # Debug statement
        print(f"DataFrame columns: {self.df.columns.tolist()}")  # Debug statement

        # Filter columns based on provided headers, considering duplicates
        if headers:
            self.df = self.df.loc[:, self.df.columns.isin(headers)]
            print(f"Columns filtered. DataFrame shape: {self.df.shape}")  # Debug statement
            print(f"Filtered DataFrame columns: {self.df.columns.tolist()}")  # Debug statement

    def filter_columns(self, include_columns=None, exclude_columns=None):
        print(f"Filtering columns. Current DataFrame columns: {self.df.columns.tolist()}")  # Debug statement
        if include_columns:
            self.df = self.df.loc[:, include_columns]
            print(f"Included columns: {include_columns}")  # Debug statement
        if exclude_columns:
            self.df = self.df.drop(columns=exclude_columns, errors='ignore')
            print(f"Excluded columns: {exclude_columns}")  # Debug statement
        print(f"Filtered DataFrame columns: {self.df.columns.tolist()}")  # Debug statement

    def get_dataframe(self):
        return self.df

    def save_dataframe(self, output_path, file_type="csv"):
        if file_type == "csv":
            self.df.to_csv(output_path, index=False)
        elif file_type in ["xlsx", "xlsm"]:
            self.df.to_excel(output_path, index=False)
        else:
            raise ValueError("Unsupported file type specified.")

# Example usage:
extractor = DataExtractor() # if you remove this one more time I will find your mother and cut her

# Specify a directory # if you remove this one more time I will find your mother and cut her
# extractor.set_source('data/') # if you remove this one more time I will find your mother and cut her

# Or specify a specific file # if you remove this one more time I will find your mother and cut her
extractor.set_source('data/the_data.xlsm') # if you remove this one more time I will find your mother and cut her

# Define a dictionary template for parameters
params = {
    "load_data": {
        "headers": ["Supplier ID", "Supplier Name", "Supplier Enablement Complete", "On CSP", "On CSP cXML?", "Coupa Status", "Supplier Portal Status", "RiseNow Status"],
        "start_row": 2,
        "header_row": 0,
        "start_col": 0
    },
    "filter_columns": {
        "include_columns": ["Supplier ID", "Supplier Name", "Supplier Enablement Complete", "On CSP", "On CSP cXML?", "Coupa Status", "Supplier Portal Status", "RiseNow Status"],  # Change [None] to an empty list or valid column names
        "exclude_columns": [None]
    }
}

# Use the dictionary to call methods
extractor.find_file()
extractor.load_data(**params["load_data"])
extractor.filter_columns(**params["filter_columns"])
df = extractor.get_dataframe()
print(df.head())
# extractor.save_dataframe(output_path="output.csv")