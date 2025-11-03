import pandas as pd
import os
import sys
import io
import shutil

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Get the script's directory and build relative paths
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(script_dir, '..', '..')

# Input file in root/data folder
input_file = os.path.join(root_dir, 'data', 'TCS Schedule.xlsx')

# Template file in root/template folder
template_file = os.path.join(root_dir, 'template', 'main_carriageway.xlsx')

# Output directory and file in root/output folder
output_dir = os.path.join(root_dir, 'output')
output_file = os.path.join(output_dir, 'main_carriageway.xlsx')

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Copy template file to output directory
shutil.copy2(template_file, output_file)
print(f"Template copied to: {output_file}")

# Read columns B to E from the Excel file starting from 3rd row (row index 2)
# header=None means don't treat any row as header, just read raw data
df = pd.read_excel(input_file, sheet_name='TCS', skiprows=2, usecols='B:E', header=None)

print(f"DataFrame shape: {df.shape}")
print(f"DataFrame columns: {df.columns.tolist()}")
print(f"\nRaw data preview:")
print(df.head())

# Check if dataframe is empty
if df.empty:
    print("\nWarning: No data found after row 3 in columns B:E")
    sys.exit(1)

# Reset column names to ensure they are 0, 1, 2, 3
df.columns = range(len(df.columns))

# Convert numeric columns (first 3 columns are From, To, Length)
df[0] = pd.to_numeric(df[0], errors='coerce')  # From (column B)
df[1] = pd.to_numeric(df[1], errors='coerce')  # To (column C)
df[2] = pd.to_numeric(df[2], errors='coerce')  # Length (column D)
# df[3] is C/S Type (column E) - keep as string

# Remove any rows where all values are NaN
df_output = df.dropna(how='all')

print(f"\nData after cleaning:")
print(df_output.head())
print(f"Total rows to write: {len(df_output)}")

# Load the copied workbook and write to it
with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
    # Write to 'Quantity' sheet starting from row 7, column A (0-indexed row 6, col 0)
    # header=False and index=False ensure only data is written
    df_output.to_excel(writer, sheet_name='Quantity', startrow=6, startcol=0, index=False, header=False)

print(f"\nSuccessfully wrote data to {output_file}")
print(f"Sheet: Quantity")
print(f"Starting from row: 7, column: A")
print(f"Total data rows written: {len(df_output)}")