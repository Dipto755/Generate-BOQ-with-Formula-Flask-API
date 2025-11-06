import pandas as pd
import os
from pathlib import Path

def log_debug(message):
    debug_file = Path(__file__).parent / 'calculator_debug.log'
    with open(debug_file, 'a') as f:
        f.write(f"{message}\n")
    print(message)

# Use session directories from environment
output_file = os.getenv('SESSION_OUTPUT_FILE', '')

log_debug("=== PANDAS CALCULATOR STARTED ===")
log_debug(f"Output file: {output_file}")

try:
    # Read the file with pandas - it often handles formula calculation better
    df = pd.read_excel(output_file, sheet_name='Abstract', engine='openpyxl')
    
    log_debug("DataFrame columns:")
    log_debug(str(df.columns.tolist()))
    
    # Check if we have values in columns F and I
    log_debug("Checking values in columns F and I (rows 3-5):")
    for idx in range(3, 6):
        if idx < len(df):
            f_val = df.iloc[idx, 5]  # Column F (index 5)
            i_val = df.iloc[idx, 8]  # Column I (index 8)
            log_debug(f"Row {idx}: F='{f_val}', I='{i_val}'")
    
    # Save back - sometimes this triggers calculation
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name='Abstract', index=False)
    
    log_debug("File saved with pandas")
    
except Exception as e:
    log_debug(f"ERROR: {str(e)}")
    import traceback
    log_debug(f"TRACEBACK: {traceback.format_exc()}")

log_debug("=== PANDAS CALCULATOR FINISHED ===")