"""
Geogrid Calculator
Reads Pavement_Input.xlsx to check for Geogrid conditions
Calculates columns KY, KZ, LA, LB in main_carriageway.xlsx based on formulas
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

# Input files
PAVEMENT_INPUT_FILE = os.path.join(root_dir, 'data', 'Pavement Input.xlsx')
MAIN_CARRIAGEWAY_FILE = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')

# Output file
OUTPUT_EXCEL = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')


# ============================================================================
# STEP 1: Check Pavement_Input.xlsx for Geogrid Conditions
# ============================================================================

def check_geogrid_conditions(pavement_input_file):
    """
    Reads Pavement_Input.xlsx and checks if:
    - E9 contains "Geogrid Reinforced GSB"
    - E10 contains "Geogrid Reinforced WMM"
    - B9 contains "Geogrid Reinforced GSB"
    - B10 contains "Geogrid Reinforced WMM"
    Returns: dictionary with boolean flags
    """
    print("="*80)
    print("STEP 1: Checking Geogrid Conditions in Pavement Input")
    print("="*80)
    
    # Read the Excel file without headers
    df = pd.read_excel(pavement_input_file, header=None)
    
    print("[OK] Read Pavement_Input.xlsx:", len(df), "total rows")
    
    # Initialize conditions
    conditions = {
        'e9_geogrid_gsb': False,
        'e10_geogrid_wmm': False,
        'b9_geogrid_gsb': False,
        'b10_geogrid_wmm': False
    }
    
    # Check E9 (row index 8, column 4)
    if len(df) > 8:
        e9_value = df.iloc[8, 4]  # E9
        if pd.notna(e9_value) and "Geogrid Reinforced GSB" in str(e9_value):
            conditions['e9_geogrid_gsb'] = True
            print(f"[OK] E9 contains 'Geogrid Reinforced GSB': {e9_value}")
        else:
            print(f"[OK] E9 value: {e9_value} (not Geogrid Reinforced GSB)")
    
    # Check E10 (row index 9, column 4)
    if len(df) > 9:
        e10_value = df.iloc[9, 4]  # E10
        if pd.notna(e10_value) and "Geogrid Reinforced WMM" in str(e10_value):
            conditions['e10_geogrid_wmm'] = True
            print(f"[OK] E10 contains 'Geogrid Reinforced WMM': {e10_value}")
        else:
            print(f"[OK] E10 value: {e10_value} (not Geogrid Reinforced WMM)")
    
    # Check B9 (row index 8, column 1)
    if len(df) > 8:
        b9_value = df.iloc[8, 1]  # B9
        if pd.notna(b9_value) and "Geogrid Reinforced GSB" in str(b9_value):
            conditions['b9_geogrid_gsb'] = True
            print(f"[OK] B9 contains 'Geogrid Reinforced GSB': {b9_value}")
        else:
            print(f"[OK] B9 value: {b9_value} (not Geogrid Reinforced GSB)")
    
    # Check B10 (row index 9, column 1)
    if len(df) > 9:
        b10_value = df.iloc[9, 1]  # B10
        if pd.notna(b10_value) and "Geogrid Reinforced WMM" in str(b10_value):
            conditions['b10_geogrid_wmm'] = True
            print(f"[OK] B10 contains 'Geogrid Reinforced WMM': {b10_value}")
        else:
            print(f"[OK] B10 value: {b10_value} (not Geogrid Reinforced WMM)")
    
    print(f"\n[OK] Geogrid Conditions Summary:")
    print(f"  E9 Geogrid GSB: {conditions['e9_geogrid_gsb']}")
    print(f"  E10 Geogrid WMM: {conditions['e10_geogrid_wmm']}")
    print(f"  B9 Geogrid GSB: {conditions['b9_geogrid_gsb']}")
    print(f"  B10 Geogrid WMM: {conditions['b10_geogrid_wmm']}")
    
    return conditions


# ============================================================================
# STEP 2: Calculate Geogrid Columns
# ============================================================================

def calculate_geogrid_columns(main_carriageway_file, conditions, output_file):
    """
    Reads main_carriageway.xlsx and calculates geogrid columns based on conditions
    
    Formulas:
    KY = (IF(E9=Geogrid GSB, DL, 0) + IF(E10=Geogrid WMM, DS, 0)) * LENGTH
    KZ = (IF(B9=Geogrid GSB, EE, 0) + IF(B10=Geogrid WMM, EJ, 0)) * LENGTH
    LA = (IF(B9=Geogrid GSB, FF, 0) + IF(B10=Geogrid WMM, FD, 0)) * LENGTH
    LB = (IF(E9=Geogrid GSB, FY, 0) + IF(E10=Geogrid WMM, FS, 0)) * LENGTH
    """
    print("\n" + "="*80)
    print("STEP 2: Calculating Geogrid Columns")
    print("="*80)
    
    # Read the main carriageway file
    df = pd.read_excel(main_carriageway_file)
    print("[OK] Read main_carriageway.xlsx:", len(df), "rows")
    print("  Current columns:", len(df.columns))
    
    # Column indexes
    LENGTH_COL = 2  # Column C
    DL_COL = 115    # Column DL
    DS_COL = 122    # Column DS
    EE_COL = 134    # Column EE
    EJ_COL = 139    # Column EJ
    FD_COL = 159    # Column FD
    FF_COL = 161    # Column FF
    FS_COL = 174    # Column FS
    FY_COL = 180    # Column FY
    
    KY_COL_INDEX = 310  # Column KY
    KZ_COL_INDEX = 311  # Column KZ
    LA_COL_INDEX = 312  # Column LA
    LB_COL_INDEX = 313  # Column LB
    
    # Initialize geogrid columns
    for col_idx, col_name in [
        (KY_COL_INDEX, 'LHS_MC_Geogrid'),
        (KZ_COL_INDEX, 'RHS_MC_Geogrid'),
        (LA_COL_INDEX, 'SR_LHS_Geogrid'),
        (LB_COL_INDEX, 'SR_RHS_Geogrid')
    ]:
        if len(df.columns) <= col_idx:
            while len(df.columns) < col_idx:
                df[f'Empty_{len(df.columns)}'] = None
            df.insert(col_idx, col_name, 0)
        else:
            df.iloc[:, col_idx] = 0
            df.columns.values[col_idx] = col_name
        print(f"[OK] Column {col_name} (index {col_idx}) initialized")
    
    # Calculate values for each row
    print(f"\n[OK] Calculating geogrid values for {len(df)} rows...")
    
    for idx in range(len(df)):
        # Get length value
        length = df.iloc[idx, LENGTH_COL] if pd.notna(df.iloc[idx, LENGTH_COL]) else 0
        
        # KY = (IF E9=Geogrid GSB THEN DL ELSE 0 + IF E10=Geogrid WMM THEN DS ELSE 0) * length
        dl_val = df.iloc[idx, DL_COL] if len(df.columns) > DL_COL and pd.notna(df.iloc[idx, DL_COL]) else 0
        ds_val = df.iloc[idx, DS_COL] if len(df.columns) > DS_COL and pd.notna(df.iloc[idx, DS_COL]) else 0
        ky_val = ((dl_val if conditions['e9_geogrid_gsb'] else 0) + 
                  (ds_val if conditions['e10_geogrid_wmm'] else 0)) * length
        df.iloc[idx, KY_COL_INDEX] = ky_val
        
        # KZ = (IF B9=Geogrid GSB THEN EE ELSE 0 + IF B10=Geogrid WMM THEN EJ ELSE 0) * length
        ee_val = df.iloc[idx, EE_COL] if len(df.columns) > EE_COL and pd.notna(df.iloc[idx, EE_COL]) else 0
        ej_val = df.iloc[idx, EJ_COL] if len(df.columns) > EJ_COL and pd.notna(df.iloc[idx, EJ_COL]) else 0
        kz_val = ((ee_val if conditions['b9_geogrid_gsb'] else 0) + 
                  (ej_val if conditions['b10_geogrid_wmm'] else 0)) * length
        df.iloc[idx, KZ_COL_INDEX] = kz_val
        
        # LA = (IF B9=Geogrid GSB THEN FF ELSE 0 + IF B10=Geogrid WMM THEN FD ELSE 0) * length
        ff_val = df.iloc[idx, FF_COL] if len(df.columns) > FF_COL and pd.notna(df.iloc[idx, FF_COL]) else 0
        fd_val = df.iloc[idx, FD_COL] if len(df.columns) > FD_COL and pd.notna(df.iloc[idx, FD_COL]) else 0
        la_val = ((ff_val if conditions['b9_geogrid_gsb'] else 0) + 
                  (fd_val if conditions['b10_geogrid_wmm'] else 0)) * length
        df.iloc[idx, LA_COL_INDEX] = la_val
        
        # LB = (IF E9=Geogrid GSB THEN FY ELSE 0 + IF E10=Geogrid WMM THEN FS ELSE 0) * length
        fy_val = df.iloc[idx, FY_COL] if len(df.columns) > FY_COL and pd.notna(df.iloc[idx, FY_COL]) else 0
        fs_val = df.iloc[idx, FS_COL] if len(df.columns) > FS_COL and pd.notna(df.iloc[idx, FS_COL]) else 0
        lb_val = ((fy_val if conditions['e9_geogrid_gsb'] else 0) + 
                  (fs_val if conditions['e10_geogrid_wmm'] else 0)) * length
        df.iloc[idx, LB_COL_INDEX] = lb_val
        
        # Progress indicator
        if (idx + 1) % 200 == 0:
            print(f"  Processed {idx + 1}/{len(df)} rows...")
    
    print(f"[OK] Geogrid calculations completed for all rows")
    
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
    print("GEOGRID CALCULATOR")
    print("="*80)
    print("Configuration:")
    print("  • Pavement_Input.xlsx: Check for Geogrid conditions")
    print("  • main_carriageway.xlsx: Calculate columns KY, KZ, LA, LB")
    print("="*80 + "\n")
    
    try:
        # Step 1: Check geogrid conditions
        conditions = check_geogrid_conditions(PAVEMENT_INPUT_FILE)
        
        # Step 2: Calculate geogrid columns
        df = calculate_geogrid_columns(MAIN_CARRIAGEWAY_FILE, conditions, OUTPUT_EXCEL)
        
        # Success summary
        print("\n" + "="*80)
        print("SUCCESS! [OK]")
        print("="*80)
        print("Output file:", OUTPUT_EXCEL)
        print("Total rows:", len(df))
        print("Total columns:", len(df.columns))
        
        # Show sample output
        print("\n" + "="*80)
        print("SAMPLE OUTPUT (first 3 rows):")
        print("-"*80)
        for idx in range(min(3, len(df))):
            print(f"\nRow {idx + 2}:")
            print(f"  Length: {df.iloc[idx, 2]}")
            print(f"  KY (LHS_MC_Geogrid): {df.iloc[idx, 286]}")
            print(f"  KZ (RHS_MC_Geogrid): {df.iloc[idx, 287]}")
            print(f"  LA (SR_LHS_Geogrid): {df.iloc[idx, 288]}")
            print(f"  LB (SR_RHS_Geogrid): {df.iloc[idx, 289]}")
        
    except FileNotFoundError as e:
        print("\n[ERROR] File not found")
        print(" ", e)
        print("\nPlease check:")
        print("  1. Files exist in the data folder")
        print("  2. File names match exactly")
    except Exception as e:
        print("\n[ERROR]:", e)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()