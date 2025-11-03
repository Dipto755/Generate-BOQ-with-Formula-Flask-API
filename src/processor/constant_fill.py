"""
Constant Fill Processor
Fills specific columns in main_carriageway.xlsx with constant values
"""

import pandas as pd
import os
import sys
import io
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# ============================================================================
# FILE PATHS - Update these to match your folder structure
# ============================================================================

script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(script_dir, '..', '..')

# Input/Output file
MAIN_CARRIAGEWAY_FILE = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')
OUTPUT_EXCEL = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')


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
    df = pd.read_excel(main_carriageway_file)
    print("[OK] Read main_carriageway.xlsx:", len(df), "rows")
    print("  Current columns:", len(df.columns))
    
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
            df.insert(col_idx, col_name, value)
            print(f"  [OK] Created new column '{col_name}' with value {value}")
        else:
            # Column exists, update it
            df.iloc[:, col_idx] = value
            # Update column name if different
            if df.columns[col_idx] != col_name:
                df.columns.values[col_idx] = col_name
                print(f"  [OK] Updated column to '{col_name}' with value {value}")
            else:
                print(f"  [OK] Filled column '{col_name}' with value {value}")
        
        print(f"  [OK] All {len(df)} rows set to {value}")
    
    # Save to Excel
    print(f"\n[OK] Saving to {output_file}...")
    df.to_excel(output_file, index=False, sheet_name='Main Carriageway')
    
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