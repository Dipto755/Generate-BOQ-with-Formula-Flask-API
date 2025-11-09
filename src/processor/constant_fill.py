"""
Constant Fill Processor
Fills specific columns in main_carriageway.xlsx with constant values
"""

import pandas as pd
import os
import sys
import io
from dotenv import load_dotenv

load_dotenv()
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# ============================================================================
# FILE PATHS - Update these to match your folder structure
# ============================================================================

# NEW CODE:
script_dir = os.path.dirname(os.path.abspath(__file__))
# Use session directories from environment, fallback to original paths
output_file = os.getenv('SESSION_OUTPUT_FILE', os.path.join(script_dir, '..', '..', 'output', 'main_carriageway_and_boq.xlsx'))

MAIN_CARRIAGEWAY_FILE = output_file
OUTPUT_EXCEL = output_file


# ============================================================================
# CONSTANTS CONFIGURATION
# ============================================================================

CONSTANTS = [
    {
        'col_index': 48,  # Column AW
        'col_name': 'SUBGRADE',
        'value': 0.5
    },
    {
        'col_index': 62,  # Column BK
        'col_name': 'LHS_Subgrade_Thickness',
        'value': 0.5
    }
]


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def fill_constant_columns(main_carriageway_file, constants, output_file):
    """
    Reads main_carriageway.xlsx and fills specified columns with constant values
    """
    print("="*80)
    print("CONSTANT FILL PROCESSOR")
    print("="*80)
    
    # Read the main carriageway file
    df = pd.read_excel(main_carriageway_file, sheet_name='Quantity', skiprows=6, header=None)
    print("[OK] Read main_carriageway.xlsx:", len(df), "rows")
    print("  Current columns:", len(df.columns))
    
    # Remove empty rows
    df = df.dropna(how='all')
    
    # Process each constant column
    for const in constants:
        col_idx = const['col_index']
        col_name = const['col_name']
        value = const['value']
        
        print(f"\n[OK] Processing Column {chr(65 + col_idx//26 - 1) if col_idx >= 26 else ''}{chr(65 + col_idx%26)} (index {col_idx})")
        
        # Ensure column exists
        if len(df.columns) <= col_idx:
            # Add empty columns up to target
            while len(df.columns) < col_idx:
                df[f'Empty_{len(df.columns)}'] = None
            # Add target column
            df.insert(col_idx, f'Col_{col_idx}', value)
            print(f"  [OK] Created new column with value {value}")
        else:
            # Column exists, update it
            df.iloc[:, col_idx] = value
            print(f"  [OK] Filled column with value {value}")
        
        print(f"  [OK] All {len(df)} rows set to {value}")
    
    # Save to Excel using ExcelWriter with overlay mode
    print(f"\n[OK] Saving to {output_file}...")
    
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        df.to_excel(writer, sheet_name='Quantity', startrow=6, startcol=0, index=False, header=False)
    
    print("[OK] Saved!")
    print(f"  Total columns: {len(df.columns)}")
    
    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function to execute all steps"""
    print("\n" + "="*80)
    print("CONSTANT FILL PROCESSOR")
    print("="*80)
    print("Configuration:")
    for const in CONSTANTS:
        col_letter = chr(65 + const['col_index']//26 - 1) if const['col_index'] >= 26 else ''
        col_letter += chr(65 + const['col_index']%26)
        print(f"  • Column {col_letter} ({const['col_name']}): {const['value']}")
    print("="*80 + "\n")
    
    try:
        # Fill constant columns
        df = fill_constant_columns(MAIN_CARRIAGEWAY_FILE, CONSTANTS, OUTPUT_EXCEL)
        
        # Success summary
        print("\n" + "="*80)
        print("SUCCESS! [OK]")
        print("="*80)
        print("Output file:", OUTPUT_EXCEL)
        print("Total rows:", len(df))
        print("Total columns:", len(df.columns))
        
        # Show sample values
        print("\n" + "="*80)
        print("SAMPLE OUTPUT:")
        print("-"*80)
        for const in CONSTANTS:
            col_idx = const['col_index']
            col_name = const['col_name']
            if len(df.columns) > col_idx:
                sample_vals = df.iloc[:3, col_idx].tolist()
                print(f"\nColumn {col_name} (index {col_idx}):")
                for i, val in enumerate(sample_vals):
                    print(f"  Row {i + 2}: {val}")
        
    except FileNotFoundError as e:
        print("\n[ERROR] File not found")
        print(" ", e)
        print("\nPlease check:")
        print("  1. File exists in the data folder")
        print("  2. File name matches exactly")
    except Exception as e:
        print("\n[ERROR]:", e)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()