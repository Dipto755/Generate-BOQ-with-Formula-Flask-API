import pandas as pd
import os
import sys
import io
import shutil
from dotenv import load_dotenv
import tempfile

# Add project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.append(project_root)

from src.utils.gcs_utils import get_gcs_handler

load_dotenv()

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# NEW CODE:
script_dir = os.path.dirname(os.path.abspath(__file__))
session_id = os.getenv('SESSION_ID', 'default')
template_file = os.path.join(script_dir, '..', '..', 'template', 'main_carriageway_and_boq.xlsx')

# Initialize GCS
gcs = get_gcs_handler()
temp_dir = tempfile.mkdtemp()

# Download input from GCS
input_gcs_path = gcs.get_gcs_path(session_id, 'TCS Schedule.xlsx', 'data')
input_file = gcs.download_to_temp(input_gcs_path, suffix='.xlsx')

# Temp output file
output_file = os.path.join(temp_dir, f"{session_id}_main_carriageway_and_boq.xlsx")
output_dir = temp_dir

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

# Upload to GCS
output_gcs_path = gcs.get_gcs_path(session_id, f"{session_id}_main_carriageway_and_boq.xlsx", 'output')
gcs.upload_file(output_file, output_gcs_path)
print(f"[GCS] Uploaded to: gs://{gcs.bucket.name}/{output_gcs_path}")

# Cleanup temp files
os.remove(input_file)
shutil.rmtree(temp_dir)
