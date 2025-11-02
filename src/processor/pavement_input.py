"""
Pavement Input Processor
Reads Pavement_Input.xlsx from row 9, columns B, C, E, F
Populates main_carriageway.xlsx column AX from row 1 onwards
"""

import pandas as pd
import os


# ============================================================================
# FILE PATHS - Update these to match your folder structure
# ============================================================================

script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(script_dir, '..', '..')

# Input files
PAVEMENT_INPUT_FILE = os.path.join(root_dir, 'data', 'Pavement Input.xlsx')
MAIN_CARRIAGEWAY_FILE = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')

# Output file
OUTPUT_EXCEL = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')


# ============================================================================
# STEP 1: Read Pavement_Input.xlsx and Create Dictionary
# ============================================================================

def create_pavement_dictionary(pavement_input_file):
    """
    Reads Pavement_Input.xlsx starting from Excel row 9
    Creates dictionary with:
      - Keys from Column B (with cell ref + value + B1 suffix)
      - Keys from Column E (with cell ref + value + E1 suffix)
      - Values from Column C (for B keys) and Column F (for E keys)
    Returns: dictionary
    """
    print("="*80)
    print("STEP 1: Creating Pavement Dictionary")
    print("="*80)
    
    # Read the Excel file without headers
    df = pd.read_excel(pavement_input_file, header=None)
    
    print("✓ Read Pavement_Input.xlsx:", len(df), "total rows")
    
    # Get suffixes from row 1 (index 0)
    suffix_B = df.iloc[0, 1]  # B1 = "MCW"
    suffix_E = df.iloc[0, 4]  # E1 = "SR"
    
    print(f"✓ Suffix from B1: '{suffix_B}'")
    print(f"✓ Suffix from E1: '{suffix_E}'")
    
    # Create dictionary
    pavement_dict = {}
    
    # Process from row 9 onwards (index 8)
    data_start_row = 8
    
    print(f"\n✓ Processing from Excel row 9 (index {data_start_row})")
    
    # Process Column B and C
    for i in range(data_start_row, len(df)):
        b_value = df.iloc[i, 1]  # Column B
        c_value = df.iloc[i, 2]  # Column C
        
        if pd.notna(b_value):
            excel_row = i + 1
            key = f"B{excel_row}_{b_value}_{suffix_B}"
            pavement_dict[key] = c_value if pd.notna(c_value) else 0
    
    # Process Column E and F
    for i in range(data_start_row, len(df)):
        e_value = df.iloc[i, 4]  # Column E
        f_value = df.iloc[i, 5]  # Column F
        
        if pd.notna(e_value):
            excel_row = i + 1
            key = f"E{excel_row}_{e_value}_{suffix_E}"
            pavement_dict[key] = f_value if pd.notna(f_value) else 0
    
    print(f"✓ Created dictionary with {len(pavement_dict)} entries")
    
    # Show sample entries
    print("\n  Sample entries:")
    for i, (key, value) in enumerate(list(pavement_dict.items())[:5]):
        print(f"    {key}: {value}")
    
    return pavement_dict


# ============================================================================
# STEP 2: Populate main_carriageway.xlsx Column AX
# ============================================================================

def populate_column_ax(main_carriageway_file, pavement_dict, output_file):
    """
    Reads main_carriageway.xlsx
    Populates column AX (index 49) based on formula:
    IF any dict key contains "CTSB", return value/1000, else 0
    """
    print("\n" + "="*80)
    print("STEP 2: Populating main_carriageway.xlsx Column AX")
    print("="*80)
    
    # Read the main carriageway file
    df = pd.read_excel(main_carriageway_file)
    print("✓ Read main_carriageway.xlsx:", len(df), "rows")
    print("  Current columns:", len(df.columns))
    
    # Column AX = index 49
    AX_COL_INDEX = 49
    
    # Check if E9 contains "CTSB" (formula: IF E9="CTSB" THEN F9/1000 ELSE 0)
    ctsb_value = None
    ctsb_key = None
    
    for key, value in pavement_dict.items():
        if key.startswith('E9_CTSB_'):
            ctsb_value = value
            ctsb_key = key
            break
    
    if ctsb_value is not None:
        ax_value = ctsb_value / 1000
        print(f"✓ Found CTSB in dictionary: {ctsb_key} = {ctsb_value}")
        print(f"  Column AX value will be: {ax_value}")
    else:
        ax_value = 0
        print("✓ No CTSB found in dictionary")
        print("  Column AX value will be: 0")
        
    # Column BB = F11/1000 (Check E11 keys, as E column has F values)
    BB_COL_INDEX = 53
    bb_value = 0
    for key, value in pavement_dict.items():
        if key.startswith('E11_'):
            bb_value = value / 1000 if pd.notna(value) and value != 0 else 0
            print(f"✓ Found E11 in dictionary: {key} = {value}, BB value = {bb_value}")
            break
    if bb_value == 0:
        print("✓ No E11 found in dictionary, BB value = 0")
    
    # Column BC = C23/1000 (Check B23 keys, as B column has C values)
    BC_COL_INDEX = 54
    bc_value = 0
    for key, value in pavement_dict.items():
        if key.startswith('B23_'):
            bc_value = value / 1000 if pd.notna(value) and value != 0 else 0
            print(f"✓ Found B23 in dictionary: {key} = {value}, BC value = {bc_value}")
            break
    if bc_value == 0:
        print("✓ No B23 found in dictionary, BC value = 0")
    
    # Column BD = C24/1000 (Check B24 keys, as B column has C values)
    BD_COL_INDEX = 55
    bd_value = 0
    for key, value in pavement_dict.items():
        if key.startswith('B24_'):
            bd_value = value / 1000 if pd.notna(value) and value != 0 else 0
            print(f"✓ Found B24 in dictionary: {key} = {value}, BD value = {bd_value}")
            break
    if bd_value == 0:
        print("✓ No B24 found in dictionary, BD value = 0")
    
    # Ensure column AX exists
    if len(df.columns) <= AX_COL_INDEX:
        # Add empty columns up to AX
        while len(df.columns) < AX_COL_INDEX:
            df[f'Empty_{len(df.columns)}'] = None
        # Add column AX
        df.insert(AX_COL_INDEX, 'CTSB_Thickness', ax_value)
    else:
        # Column exists, update it
        df.iloc[:, AX_COL_INDEX] = ax_value
        if df.columns[AX_COL_INDEX] != 'CTSB_Thickness':
            df.columns.values[AX_COL_INDEX] = 'CTSB_Thickness'
    
    print(f"\n✓ Column AX (index {AX_COL_INDEX}) set to: {ax_value}")
    print(f"  Total columns: {len(df.columns)}")
    
    # Set columns BB, BC, BD
    for col_idx, col_value, col_name in [
        (BB_COL_INDEX, bb_value, 'LHS_AIL_Thickness'),
        (BC_COL_INDEX, bc_value, 'LHS_DLC_Thickness'),
        (BD_COL_INDEX, bd_value, 'LHS_PQC_Thickness')
    ]:
        if len(df.columns) <= col_idx:
            while len(df.columns) < col_idx:
                df[f'Empty_{len(df.columns)}'] = None
            df.insert(col_idx, col_name, col_value)
        else:
            df.iloc[:, col_idx] = col_value
            df.columns.values[col_idx] = col_name
        print(f"✓ Column index {col_idx} ({col_name}) set to: {col_value}")
    
    # Save to Excel
    print(f"\n✓ Saving to {output_file}...")
    df.to_excel(output_file, index=False, sheet_name='Main Carriageway')
    
    print("✓ Saved!")
    
    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function to execute all steps"""
    print("\n" + "="*80)
    print("PAVEMENT INPUT PROCESSOR")
    print("="*80)
    print("Configuration:")
    print("  • Pavement_Input.xlsx: Read from Excel row 9 (Col B, C, E, F)")
    print("  • main_carriageway.xlsx: Populate column AX")
    print("  • Formula: IF E9='CTSB' THEN F9/1000 ELSE 0")
    print("="*80 + "\n")
    
    try:
        # Step 1: Create pavement dictionary
        pavement_dict = create_pavement_dictionary(PAVEMENT_INPUT_FILE)
        
        # Step 2: Populate main_carriageway.xlsx
        df = populate_column_ax(MAIN_CARRIAGEWAY_FILE, pavement_dict, OUTPUT_EXCEL)
        
        # Success summary
        print("\n" + "="*80)
        print("SUCCESS! ✓")
        print("="*80)
        print("Output file:", OUTPUT_EXCEL)
        print("Total rows:", len(df))
        print("Total columns:", len(df.columns))
        
        # Show sample output
        print("\n" + "="*80)
        print("SAMPLE OUTPUT:")
        print("-"*80)
        print("First 3 rows, Column AX value:")
        for idx in range(min(3, len(df))):
            ax_val = df.iloc[idx, 49] if len(df.columns) > 49 else None
            print(f"  Row {idx + 2}: {ax_val}")
        
    except FileNotFoundError as e:
        print("\n✗ ERROR: File not found")
        print(" ", e)
        print("\nPlease check:")
        print("  1. Files exist in the data folder")
        print("  2. File names match exactly")
    except Exception as e:
        print("\n✗ ERROR:", e)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()