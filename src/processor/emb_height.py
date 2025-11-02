"""
Embankment Height Processor
Reads Emb_Height.xlsx from row 5, columns A, E, F
Populates main_carriageway.xlsx columns AQ and AR from row 1 onwards
"""

import pandas as pd
import os

# ============================================================================
# FILE PATHS - Update these to match your folder structure
# ============================================================================

script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(script_dir, '..', '..')

# Input files
EMB_HEIGHT_FILE = os.path.join(root_dir, 'data', 'Emb Height.xlsx')
MAIN_CARRIAGEWAY_FILE = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')

# Output file
OUTPUT_EXCEL = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')


# ============================================================================
# STEP 1: Read Emb_Height.xlsx and Create Dictionary
# ============================================================================

def create_emb_height_dictionary(emb_height_file):
    """
    Reads Emb_Height.xlsx starting from Excel row 5
    Creates dictionary with Column A as key, Columns E and F as values
    Returns: dictionary {chainage: {'left': value_E, 'right': value_F}}
    """
    print("="*80)
    print("STEP 1: Creating Embankment Height Dictionary")
    print("="*80)
    
    # Read the Excel file
    df = pd.read_excel(emb_height_file)
    
    print("✓ Read Emb_Height.xlsx:", len(df), "total rows")
    
    # Start from Excel row 5 (pandas index 4)
    data_start_row = 3
    
    # Extract data from row 5 onwards
    data = df.iloc[data_start_row:]
    
    print("✓ Extracting data from Excel row 5 (pandas index", data_start_row, ")")
    print("✓ Total data rows:", len(data))
    
    # Create dictionary with Column A as key
    emb_dict = {}
    skipped = 0
    
    for idx, row in data.iterrows():
        # Column A (index 0) = Key (chainage)
        key = row.iloc[0]
        
        # Column E (index 4) = Left height
        value_e = row.iloc[4]
        
        # Column F (index 5) = Right height
        value_f = row.iloc[5]
        
        # Skip if key is NaN
        if pd.notna(key):
            emb_dict[float(key)] = {
                'left': float(value_e) if pd.notna(value_e) else None,
                'right': float(value_f) if pd.notna(value_f) else None
            }
        else:
            skipped += 1
    
    print("✓ Created dictionary with", len(emb_dict), "entries")
    if skipped > 0:
        print("  (Skipped", skipped, "rows with NaN keys)")
    
    # Show range
    if emb_dict:
        keys = sorted(emb_dict.keys())
        print("✓ Key (chainage) range: %.3f to %.3f" % (keys[0], keys[-1]))
        print("\n  Sample entries:")
        for i, key in enumerate(keys[:3]):
            print("    %s: Left=%s, Right=%s" % (key, emb_dict[key]['left'], emb_dict[key]['right']))
    
    return emb_dict


# ============================================================================
# STEP 2: Populate main_carriageway.xlsx with Embankment Heights
# ============================================================================

def populate_embankment_heights(main_carriageway_file, emb_dict, output_file):
    """
    Reads main_carriageway.xlsx
    Matches Column A with dict keys starting from Excel row 1
    Populates columns AQ and AR with embankment heights
    """
    print("\n" + "="*80)
    print("STEP 2: Populating main_carriageway.xlsx")
    print("="*80)
    
    # Read the main carriageway file
    df = pd.read_excel(main_carriageway_file)
    print("✓ Read main_carriageway.xlsx:", len(df), "rows")
    print("  Current columns:", len(df.columns))
    
    # Add new columns AQ and AR for embankment heights
    df['Emb_Height_Left'] = None
    df['Emb_Height_Right'] = None
    
    print("\n✓ Added columns AQ and AR:")
    print("  AQ: Emb_Height_Left")
    print("  AR: Emb_Height_Right")
    
    # Start matching from Excel row 1 (pandas index 0)
    match_start_row = 0
    
    print("\n✓ Matching will start from:")
    print("  Excel row 1 (pandas index", match_start_row, ") - ALL rows")
    print("  Total rows to process:", len(df))
    
    # Match and populate
    matched = 0
    unmatched = 0
    
    for idx in range(len(df)):
        # Get the key from Column A
        key = df.iloc[idx, 0]
        
        # Debug first row
        if idx == 0:
            print("\nDEBUG First Row:")
            print("  Key value:", key)
            print("  Key type:", type(key))
            if pd.notna(key):
                print("  Key as float:", float(key))
                print("  Key exists in dict?", float(key) in emb_dict)
            print("  First 5 dict keys:", list(emb_dict.keys())[:5])
        
        # Try to match in dictionary
        if pd.notna(key):
            key_float = round(float(key), 3)  # Round to 3 decimals
            if key_float in emb_dict:
                heights = emb_dict[float(key)]
                df.at[idx, 'Emb_Height_Left'] = heights['left']
                df.at[idx, 'Emb_Height_Right'] = heights['right']
                matched += 1
        else:
            unmatched += 1
        
        # Progress indicator
        if (idx + 1) % 200 == 0:
            print("  Processed %d/%d rows..." % (idx + 1, len(df)))
    
    print("\n✓ Matching complete:")
    print("  Matched (all rows):", matched)
    print("  Unmatched (all rows):", unmatched)
    
    if matched > 0:
        match_pct = matched / (matched + unmatched) * 100
        print("  Match rate: %.1f%%" % match_pct)
    
    # Save to Excel
    print("\n✓ Saving to", output_file, "...")
    df.to_excel(output_file, index=False, sheet_name='Main Carriageway')
    
    print("✓ Saved! Total columns:", len(df.columns))
    
    return df, matched, unmatched


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function to execute all steps"""
    print("\n" + "="*80)
    print("EMBANKMENT HEIGHT PROCESSOR")
    print("="*80)
    print("Configuration:")
    print("  • Emb_Height.xlsx: Read from Excel row 5 (Col A, E, F)")
    print("  • main_carriageway.xlsx: Match from Excel row 1 (all rows)")
    print("  • Output: Populate columns AQ and AR")
    print("="*80 + "\n")
    
    try:
        # Step 1: Create embankment height dictionary
        emb_dict = create_emb_height_dictionary(EMB_HEIGHT_FILE)
        
        # Step 2: Populate main_carriageway.xlsx
        df, matched, unmatched = populate_embankment_heights(
            MAIN_CARRIAGEWAY_FILE, emb_dict, OUTPUT_EXCEL
        )
        
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
        
        # Find rows with embankment heights populated
        rows_with_heights = df[df['Emb_Height_Left'].notna()]
        
        if len(rows_with_heights) > 0:
            print("\nFound", len(rows_with_heights), "rows with embankment heights")
            print("\nFirst 3 rows with heights:")
            
            for i, (idx, row) in enumerate(rows_with_heights.head(3).iterrows()):
                print("\n  Row %d (Excel):" % (idx + 2))
                print("    Column A (from):", row.iloc[0])
                if 'type_of_cross_section' in df.columns:
                    print("    Type:", row['type_of_cross_section'])
                print("    Emb_Height_Left (AQ):", row['Emb_Height_Left'])
                print("    Emb_Height_Right (AR):", row['Emb_Height_Right'])
        else:
            print("\n⚠ No matching chainages found")
            print("Possible reasons:")
            print("  1. Chainage ranges don't overlap")
            print("  2. Key values don't match exactly")
            print("\nColumns AQ and AR have been added but remain empty")
        
        # Column layout
        print("\n" + "="*80)
        print("FINAL COLUMN LAYOUT:")
        print("-"*80)
        print("  A-AP: Existing", len(df.columns) - 2, "columns")
        print("  AQ: Emb_Height_Left")
        print("  AR: Emb_Height_Right")
        
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