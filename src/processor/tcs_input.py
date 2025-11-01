"""
TCS Specification Populator - Custom Column Selection
Reads columns D-V and AA-AS from TCS_Input.xlsx
Writes them adjacently in main_carriageway.xlsx
"""

import pandas as pd
import json
import os

# ============================================================================
# FILE PATHS - Update these to match your folder structure
# ============================================================================

script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(script_dir, '..', '..')

# Input files
TCS_INPUT_FILE = os.path.join(root_dir, 'data', 'TCS Input.xlsx')
MAIN_CARRIAGEWAY_FILE = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')

# Output files
OUTPUT_JSON = os.path.join(root_dir, 'data', 'tcs_specifications.json')
OUTPUT_EXCEL = os.path.join(root_dir, 'data', 'main_carriageway.xlsx')


# ============================================================================
# COLUMN RANGES TO EXTRACT
# ============================================================================

# Excel columns D to V (0-indexed: 3 to 21)
RANGE_1_START = 3  # Column D
RANGE_1_END = 21   # Column V (inclusive)

# Excel columns AA to AS (0-indexed: 26 to 44)
RANGE_2_START = 26  # Column AA
RANGE_2_END = 44    # Column AS (inclusive)


# ============================================================================
# STEP 1: Read TCS_Input.xlsx and Create Dictionary (SPECIFIC COLUMNS)
# ============================================================================

def create_tcs_dictionary(tcs_input_file):
    """
    Reads TCS_Input.xlsx and creates a dictionary with TCS type as key
    Extracts only columns D-V and AA-AS
    Returns: dictionary with TCS specifications
    """
    print("="*80)
    print("STEP 1: Creating TCS Dictionary (columns D-V and AA-AS)")
    print("="*80)
    
    # Read the Excel file
    df = pd.read_excel(tcs_input_file, sheet_name='Input')
    
    # First row contains headers
    raw_headers = df.iloc[0].tolist()
    
    # Extract only the columns we want
    # Range 1: Columns D to V (indices 3-21)
    range_1_indices = list(range(RANGE_1_START, RANGE_1_END + 1))
    
    # Range 2: Columns AA to AS (indices 26-44)
    range_2_indices = list(range(RANGE_2_START, RANGE_2_END + 1))
    
    # Combined indices
    selected_indices = range_1_indices + range_2_indices
    
    print(f"✓ Extracting columns:")
    print(f"  Range 1: D to V (indices {RANGE_1_START} to {RANGE_1_END}) = {len(range_1_indices)} columns")
    print(f"  Range 2: AA to AS (indices {RANGE_2_START} to {RANGE_2_END}) = {len(range_2_indices)} columns")
    print(f"  Total columns to extract: {len(selected_indices)}")
    
    # Get headers for selected columns and add LHS/RHS suffixes for duplicates
    # First pass: collect all headers
    temp_headers = []
    for idx in selected_indices:
        if idx < len(raw_headers):
            header = raw_headers[idx]
            if header and str(header) != 'nan' and pd.notna(header):
                temp_headers.append(str(header))
            else:
                temp_headers.append(None)
        else:
            temp_headers.append(None)
    
    # Check for duplicates
    from collections import Counter
    header_counts = Counter([h for h in temp_headers if h])
    
    # Second pass: add suffixes
    selected_headers = []
    seen_counts = {}
    
    for i, idx in enumerate(selected_indices):
        header = temp_headers[i]
        
        if header:
            # Check if this header appears multiple times
            if header_counts[header] > 1:
                # Track how many times we've seen this header
                if header not in seen_counts:
                    seen_counts[header] = 0
                seen_counts[header] += 1
                
                # Add suffix based on which occurrence this is
                if seen_counts[header] == 1:
                    # First occurrence (from D-V range) - add _LHS
                    suffixed_header = f"{header}_LHS"
                else:
                    # Second occurrence (from AA-AS range) - add _RHS
                    suffixed_header = f"{header}_RHS"
                
                selected_headers.append((idx, suffixed_header))
            else:
                # Unique column, keep as is
                selected_headers.append((idx, header))
        else:
            selected_headers.append((idx, None))
    
    print(f"\n✓ Selected column headers:")
    for idx, header in selected_headers:
        if header:
            # Convert index to Excel column letter
            col_letter = ''
            temp_idx = idx
            while temp_idx >= 0:
                col_letter = chr(65 + (temp_idx % 26)) + col_letter
                temp_idx = temp_idx // 26 - 1
            print(f"  {col_letter} (index {idx}): {header}")
    
    # Find the CROSS SECTION TYPE column (should be column C, index 2)
    cs_type_idx = 2
    
    # Data rows start from second row
    data = df.iloc[1:].reset_index(drop=True)
    
    # Create dictionary with TCS type as key
    tcs_dict = {}
    
    for idx, row in data.iterrows():
        # Get the cross section type
        cs_type = row.iloc[cs_type_idx]
        
        if pd.notna(cs_type):
            # Create a dictionary for this row with only selected columns
            row_dict = {}
            
            for col_idx, header in selected_headers:
                if header:
                    value = row.iloc[col_idx]
                    
                    # Handle NaN values
                    if pd.isna(value):
                        row_dict[header] = None
                    # Keep numeric and string values
                    elif isinstance(value, (int, float)):
                        row_dict[header] = value
                    else:
                        row_dict[header] = str(value)
            
            tcs_dict[cs_type] = row_dict
    
    print(f"\n✓ Created dictionary with {len(tcs_dict)} TCS types")
    print(f"✓ Each type has {len(selected_headers)} specifications")
    
    return tcs_dict


# ============================================================================
# STEP 2: Populate main_carriageway.xlsx with Specifications
# ============================================================================

def populate_specifications(main_carriageway_file, tcs_dict, output_file):
    """
    Reads main_carriageway.xlsx and adds TCS specifications from dictionary
    Columns written from E to AP (38 columns)
    """
    print("\n" + "="*80)
    print("STEP 2: Populating main_carriageway.xlsx")
    print("="*80)
    
    # Read the main carriageway file
    df = pd.read_excel(main_carriageway_file)
    print(f"✓ Read main_carriageway.xlsx: {len(df)} rows")
    print(f"  Existing columns (A-D): {list(df.columns)}")
    
    # Create case-insensitive lookup dictionary
    tcs_dict_lower = {k.lower(): (k, v) for k, v in tcs_dict.items()}
    
    # Get specification column names IN ORDER
    spec_columns = []
    for specs in tcs_dict.values():
        if specs:
            # Preserve the order from the dictionary
            spec_columns = list(specs.keys())
            break
    
    print(f"✓ Will add {len(spec_columns)} specification columns from E to AP")
    
    # Create new dataframe with specifications
    # Start with original 4 columns (A-D)
    result_df = df[['from', 'to', 'length', 'type_of_cross_section']].copy()
    
    # Add specification columns (E onwards) in the exact order
    for col in spec_columns:
        result_df[col] = None
    
    print(f"✓ Column layout:")
    print(f"  A-D: from, to, length, type_of_cross_section")
    print(f"  E-AP: {len(spec_columns)} specification columns")
    
    # Fill in specifications for each row based on its TCS type
    print(f"\nFilling specifications...")
    matched_count = 0
    unmatched_types = set()
    type_counts = {}
    
    for idx, row in result_df.iterrows():
        cs_type = row['type_of_cross_section']
        
        # Count occurrences of each type
        if cs_type not in type_counts:
            type_counts[cs_type] = 0
        type_counts[cs_type] += 1
        
        # Try exact match first
        if cs_type in tcs_dict:
            specs = tcs_dict[cs_type]
            matched_count += 1
        # Try case-insensitive match
        elif cs_type.lower() in tcs_dict_lower:
            original_key, specs = tcs_dict_lower[cs_type.lower()]
            matched_count += 1
        else:
            unmatched_types.add(cs_type)
            continue
        
        # Fill in all specification columns for this row
        for col in spec_columns:
            if col in specs:
                result_df.at[idx, col] = specs[col]
        
        # Progress indicator every 200 rows
        if (idx + 1) % 200 == 0:
            print(f"  Processed {idx + 1}/{len(result_df)} rows...")
    
    print(f"\n✓ Matched specifications for {matched_count}/{len(result_df)} rows")
    
    # Show summary
    print(f"\nDictionary lookup statistics:")
    for cs_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        status = "✓" if cs_type in tcs_dict or cs_type.lower() in tcs_dict_lower else "✗"
        print(f"  {status} {cs_type}: {count} rows")
    
    if unmatched_types:
        print(f"\n⚠ WARNING: No specifications found for these types:")
        for cs_type in sorted(unmatched_types):
            print(f"    - {cs_type}")
    
    # Verify column count
    expected_total = 4 + len(spec_columns)  # Original 4 + specifications
    actual_total = len(result_df.columns)
    
    print(f"\n✓ Column verification:")
    print(f"  Expected total columns: {expected_total}")
    print(f"  Actual total columns: {actual_total}")
    print(f"  Match: {'✓ YES' if expected_total == actual_total else '✗ NO'}")
    
    # Show exact column range
    if len(spec_columns) == 38:
        print(f"  Columns E-AP: {len(spec_columns)} specification columns ✓")
    
    # Save to Excel file
    print(f"\n✓ Saving to {output_file}...")
    result_df.to_excel(output_file, index=False, sheet_name='Main Carriageway')
    
    print(f"✓ Saved! Total columns: {len(result_df.columns)} (A-D: 4 original + E-AP: {len(spec_columns)} specifications)")
    
    return result_df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function to execute all steps"""
    print("\n" + "="*80)
    print("TCS SPECIFICATION POPULATOR - CUSTOM COLUMN SELECTION")
    print("Extracts columns D-V and AA-AS from TCS_Input.xlsx")
    print("="*80 + "\n")
    
    try:
        # Step 1: Create TCS dictionary from TCS_Input.xlsx
        tcs_dict = create_tcs_dictionary(TCS_INPUT_FILE)
        
        # Save dictionary as JSON for reference
        with open(OUTPUT_JSON, 'w') as f:
            json.dump(tcs_dict, f, indent=2)
        print(f"✓ Saved dictionary to: {OUTPUT_JSON}")
        
        # Step 2: Populate main_carriageway.xlsx with specifications
        df = populate_specifications(MAIN_CARRIAGEWAY_FILE, tcs_dict, OUTPUT_EXCEL)
        
        # Success summary
        print("\n" + "="*80)
        print("SUCCESS! ✓")
        print("="*80)
        print(f"Output file: {OUTPUT_EXCEL}")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print(f"\nColumn layout in Excel:")
        print(f"  A: from")
        print(f"  B: to")
        print(f"  C: length")
        print(f"  D: type_of_cross_section")
        print(f"  E-AP: {len(df.columns) - 4} specification columns (38 columns)")
        print(f"    ├─ E-V: Columns from TCS_Input D-V (LHS specifications)")
        print(f"    └─ W-AP: Columns from TCS_Input AA-AS (RHS specifications)")
        
        # Show sample
        print(f"\n" + "="*80)
        print("SAMPLE OUTPUT (first row with specifications):")
        print("-"*80)
        
        # Find first row with non-null specifications
        for idx, row in df.iterrows():
            # Check if this row has any specifications
            has_specs = False
            for col in df.columns[4:]:  # Skip first 4 columns
                if pd.notna(row.get(col)):
                    has_specs = True
                    break
            
            if has_specs:
                print(f"Row {idx + 2} (Excel): {row['from']:.2f} to {row['to']:.2f}")
                print(f"  Type: {row['type_of_cross_section']}")
                
                # Show first few specification columns
                spec_cols = list(df.columns[4:])
                for col in spec_cols[:5]:
                    if pd.notna(row[col]):
                        print(f"  {col}: {row[col]}")
                
                if len(spec_cols) > 5:
                    print(f"  ... and {len(spec_cols) - 5} more columns")
                
                break
        
    except FileNotFoundError as e:
        print(f"\n✗ ERROR: File not found - {e}")
        print("Please check your file paths!")
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()