"""
TCS Specification Populator - Extended Version
================================================
Reads columns D-V and AA-AS from TCS_Input.xlsx
PLUS column W (LEFT PAVED SHOULDER) mapped to AS, AT, AU, AV in output

Author: Auto-generated
Date: 2025
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
OUTPUT_EXCEL = os.path.join(root_dir, 'data', 'main_carriageway_extended.xlsx')


# ============================================================================
# COLUMN RANGES TO EXTRACT
# ============================================================================

# Excel columns D to V (0-indexed: 3 to 21)
RANGE_1_START = 3  # Column D
RANGE_1_END = 21   # Column V (inclusive)

# Excel columns AA to AS (0-indexed: 26 to 44)
RANGE_2_START = 26  # Column AA
RANGE_2_END = 44    # Column AS (inclusive)

# Additional column W (0-indexed: 22) - LEFT PAVED SHOULDER
COLUMN_W_INDEX = 22  # Column W


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def idx_to_excel_col(idx):
    """Convert 0-based column index to Excel column letter"""
    col = ''
    idx += 1
    while idx > 0:
        idx -= 1
        col = chr(65 + (idx % 26)) + col
        idx //= 26
    return col


# ============================================================================
# STEP 1: Read TCS_Input.xlsx and Create Dictionary
# ============================================================================

def create_tcs_dictionary(tcs_input_file):
    """
    Reads TCS_Input.xlsx and creates a dictionary with TCS type as key
    Extracts columns D-V, AA-AS, and W
    Returns: (dictionary with TCS specifications, column W header name)
    """
    print("="*80)
    print("STEP 1: Creating TCS Dictionary (columns D-V, AA-AS, and W)")
    print("="*80)
    
    # Read the Excel file
    df = pd.read_excel(tcs_input_file, sheet_name='Input')
    
    # First row contains headers
    raw_headers = df.iloc[0].tolist()
    
    # Extract column ranges
    range_1_indices = list(range(RANGE_1_START, RANGE_1_END + 1))
    range_2_indices = list(range(RANGE_2_START, RANGE_2_END + 1))
    selected_indices = range_1_indices + range_2_indices
    
    print(f"✓ Extracting columns:")
    print(f"  Range 1: D to V (indices {RANGE_1_START} to {RANGE_1_END}) = {len(range_1_indices)} columns")
    print(f"  Range 2: AA to AS (indices {RANGE_2_START} to {RANGE_2_END}) = {len(range_2_indices)} columns")
    print(f"  Column W (index {COLUMN_W_INDEX}): {raw_headers[COLUMN_W_INDEX]}")
    print(f"  Total columns to extract: {len(selected_indices) + 1}")
    
    # Get headers and handle duplicates
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
    
    # Add suffixes to duplicates
    selected_headers = []
    seen_counts = {}
    
    for i, idx in enumerate(selected_indices):
        header = temp_headers[i]
        
        if header:
            if header_counts[header] > 1:
                if header not in seen_counts:
                    seen_counts[header] = 0
                seen_counts[header] += 1
                
                # Add _LHS or _RHS suffix
                if seen_counts[header] == 1:
                    suffixed_header = f"{header}_LHS"
                else:
                    suffixed_header = f"{header}_RHS"
                
                selected_headers.append((idx, suffixed_header))
            else:
                selected_headers.append((idx, header))
        else:
            selected_headers.append((idx, None))
    
    # Get column W header
    column_w_header = raw_headers[COLUMN_W_INDEX] if COLUMN_W_INDEX < len(raw_headers) else None
    
    print(f"\n✓ Selected column headers:")
    for idx, header in selected_headers[:5]:  # Show first 5
        if header:
            print(f"  {idx_to_excel_col(idx)} (index {idx}): {header}")
    print(f"  ... and {len(selected_headers) - 5} more columns")
    
    if column_w_header:
        print(f"  {idx_to_excel_col(COLUMN_W_INDEX)} (index {COLUMN_W_INDEX}): {column_w_header} → Will populate AS, AT, AU, AV")
    
    # Create dictionary with TCS type as key
    cs_type_idx = 2  # Column C
    data = df.iloc[1:].reset_index(drop=True)
    tcs_dict = {}
    
    for idx, row in data.iterrows():
        cs_type = row.iloc[cs_type_idx]
        
        if pd.notna(cs_type):
            row_dict = {}
            
            # Add specification columns
            for col_idx, header in selected_headers:
                if header:
                    value = row.iloc[col_idx]
                    if pd.isna(value):
                        row_dict[header] = None
                    elif isinstance(value, (int, float)):
                        row_dict[header] = value
                    else:
                        row_dict[header] = str(value)
            
            # Add column W value
            if column_w_header:
                w_value = row.iloc[COLUMN_W_INDEX]
                if pd.isna(w_value):
                    row_dict['COLUMN_W_VALUE'] = None
                elif isinstance(w_value, (int, float)):
                    row_dict['COLUMN_W_VALUE'] = w_value
                else:
                    row_dict['COLUMN_W_VALUE'] = str(w_value)
            
            tcs_dict[cs_type] = row_dict
    
    print(f"\n✓ Created dictionary with {len(tcs_dict)} TCS types")
    print(f"✓ Each type has {len(selected_headers) + 1} specifications (including column W)")
    
    return tcs_dict, column_w_header


# ============================================================================
# STEP 2: Populate main_carriageway.xlsx with Specifications
# ============================================================================

def populate_specifications(main_carriageway_file, tcs_dict, column_w_name, output_file):
    """
    Reads main_carriageway.xlsx and adds TCS specifications from dictionary
    Writes:
      - Columns E-AP: 38 specification columns
      - Columns AQ-AR: 2 placeholder columns
      - Columns AS-AV: 4 columns with column W values
    """
    print("\n" + "="*80)
    print("STEP 2: Populating main_carriageway.xlsx")
    print("="*80)
    
    # Read the main carriageway file
    df = pd.read_excel(main_carriageway_file)
    print(f"✓ Read main_carriageway.xlsx: {len(df)} rows")
    print(f"  Existing columns: {len(df.columns)}")
    
    # Create case-insensitive lookup dictionary
    tcs_dict_lower = {k.lower(): (k, v) for k, v in tcs_dict.items()}
    
    # Get specification column names (excluding COLUMN_W_VALUE)
    spec_columns = []
    for specs in tcs_dict.values():
        if specs:
            spec_columns = [k for k in specs.keys() if k != 'COLUMN_W_VALUE']
            break
    
    print(f"✓ Will add {len(spec_columns)} specification columns from E to AP")
    print(f"✓ Will add columns AQ-AR as placeholders")
    print(f"✓ Will add columns AS-AV with '{column_w_name}' values")
    
    # Create result dataframe
    result_df = df[['from', 'to', 'length', 'type_of_cross_section']].copy()
    
    # Add specification columns (E-AP)
    for col in spec_columns:
        result_df[col] = None
    
    # Add placeholder columns (AQ-AR)
    result_df['AQ_PLACEHOLDER'] = None
    result_df['AR_PLACEHOLDER'] = None
    
    # Add column W value columns (AS-AV)
    result_df['AS'] = None
    result_df['AT'] = None
    result_df['AU'] = None
    result_df['AV'] = None
    
    print(f"✓ Total output columns: {len(result_df.columns)}")
    
    # Fill in specifications
    print(f"\nFilling specifications...")
    matched_count = 0
    unmatched_types = set()
    type_counts = {}
    
    for idx, row in result_df.iterrows():
        cs_type = row['type_of_cross_section']
        
        if cs_type not in type_counts:
            type_counts[cs_type] = 0
        type_counts[cs_type] += 1
        
        # Try exact match first, then case-insensitive
        if cs_type in tcs_dict:
            specs = tcs_dict[cs_type]
            matched_count += 1
        elif cs_type.lower() in tcs_dict_lower:
            original_key, specs = tcs_dict_lower[cs_type.lower()]
            matched_count += 1
        else:
            unmatched_types.add(cs_type)
            continue
        
        # Fill specification columns
        for col in spec_columns:
            if col in specs:
                result_df.at[idx, col] = specs[col]
        
        # Fill column W values to AS, AT, AU, AV
        if 'COLUMN_W_VALUE' in specs:
            w_value = specs['COLUMN_W_VALUE']
            result_df.at[idx, 'AS'] = w_value
            result_df.at[idx, 'AT'] = w_value
            result_df.at[idx, 'AU'] = w_value
            result_df.at[idx, 'AV'] = w_value
        
        # Progress indicator
        if (idx + 1) % 200 == 0:
            print(f"  Processed {idx + 1}/{len(result_df)} rows...")
    
    print(f"\n✓ Matched specifications for {matched_count}/{len(result_df)} rows")
    
    # Show summary
    print(f"\nDictionary lookup statistics:")
    for cs_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        status = "✓" if cs_type in tcs_dict or cs_type.lower() in tcs_dict_lower else "✗"
        print(f"  {status} {cs_type}: {count} rows")
    
    if unmatched_types:
        print(f"\n⚠ WARNING: No specifications found for:")
        for cs_type in sorted(unmatched_types):
            print(f"    - {cs_type}")
    
    # Verify column count
    expected_total = 4 + len(spec_columns) + 2 + 4  # Core + specs + placeholders + W columns
    actual_total = len(result_df.columns)
    
    print(f"\n✓ Column verification:")
    print(f"  Expected: {expected_total}, Actual: {actual_total}")
    print(f"  Match: {'✓ YES' if expected_total == actual_total else '✗ NO'}")
    
    # Save to Excel
    print(f"\n✓ Saving to {output_file}...")
    result_df.to_excel(output_file, index=False, sheet_name='Main Carriageway')
    
    print(f"✓ Saved! Structure:")
    print(f"   A-D: 4 core columns")
    print(f"   E-AP: {len(spec_columns)} specification columns")
    print(f"   AQ-AR: 2 placeholder columns")
    print(f"   AS-AV: 4 '{column_w_name}' columns")
    
    return result_df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function to execute all steps"""
    print("\n" + "="*80)
    print("TCS SPECIFICATION POPULATOR - EXTENDED VERSION")
    print("Extracts columns D-V and AA-AS from TCS_Input.xlsx")
    print("PLUS column W mapped to Excel columns AS, AT, AU, AV")
    print("="*80 + "\n")
    
    try:
        # Step 1: Create TCS dictionary
        tcs_dict, column_w_name = create_tcs_dictionary(TCS_INPUT_FILE)
        
        # Save dictionary as JSON for reference
        with open(OUTPUT_JSON, 'w') as f:
            json.dump(tcs_dict, f, indent=2)
        print(f"✓ Saved dictionary to: {OUTPUT_JSON}")
        
        # Step 2: Populate main_carriageway.xlsx
        df = populate_specifications(MAIN_CARRIAGEWAY_FILE, tcs_dict, column_w_name, OUTPUT_EXCEL)
        
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
        print(f"  E-AP: 38 specification columns")
        print(f"    ├─ E-W: LHS specifications (from TCS_Input D-V)")
        print(f"    └─ X-AP: RHS specifications (from TCS_Input AA-AS)")
        print(f"  AQ-AR: 2 placeholder columns (empty)")
        print(f"  AS-AV: 4 columns containing '{column_w_name}' from TCS_Input column W")
        
        # Show sample
        print(f"\n" + "="*80)
        print("SAMPLE OUTPUT (first row with data):")
        print("-"*80)
        
        for idx, row in df.iterrows():
            has_specs = any(pd.notna(row.get(col)) for col in df.columns[4:])
            
            if has_specs:
                print(f"Row {idx + 2} (Excel): {row['from']:.2f} to {row['to']:.2f}")
                print(f"  Type: {row['type_of_cross_section']}")
                
                # Show first few specs
                spec_cols = [c for c in df.columns[4:-6] if pd.notna(row.get(c))][:5]
                for col in spec_cols:
                    print(f"  {col}: {row[col]}")
                
                # Show column W values
                print(f"\n  Column W values (AS-AV):")
                print(f"    AS ({column_w_name}): {row['AS']}")
                print(f"    AT ({column_w_name}): {row['AT']}")
                print(f"    AU ({column_w_name}): {row['AU']}")
                print(f"    AV ({column_w_name}): {row['AV']}")
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