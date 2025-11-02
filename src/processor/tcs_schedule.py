import pandas as pd
import os
import sys
import io
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Get the script's directory and build relative paths
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(script_dir, '..', '..')

# Input file in root/data folder
input_file = os.path.join(root_dir, 'data', 'TCS Schedule.xlsx')

# Output file in root/data folder
output_file = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')

# Read the Excel file
df = pd.read_excel(input_file, sheet_name='TCS')

# Set proper column names (the first row after the title is the header)
df.columns = ['S#', 'From', 'To', 'Length (m.)', 'C/S Type']

# Remove the first row which was the header
df = df.iloc[1:].reset_index(drop=True)

# Convert numeric columns
df['S#'] = pd.to_numeric(df['S#'], errors='coerce')
df['From'] = pd.to_numeric(df['From'], errors='coerce')
df['To'] = pd.to_numeric(df['To'], errors='coerce')
df['Length (m.)'] = pd.to_numeric(df['Length (m.)'], errors='coerce')

# Rename columns as requested
df = df.rename(columns={
    'From': 'from',
    'To': 'to',
    'Length (m.)': 'length',
    'C/S Type': 'type_of_cross_section'
})

# Select only the requested columns
df_output = df[['from', 'to', 'length', 'type_of_cross_section']]

# Write to new Excel file
df_output.to_excel(output_file, index=False, sheet_name='Main Carriageway')

print(f"Successfully created {output_file}")
print(f"Total rows: {len(df_output)}")
print(f"\nColumn names: {list(df_output.columns)}")
print(f"\nFirst 5 rows:")
print(df_output.head())