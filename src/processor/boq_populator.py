import json
import os
from pathlib import Path
from openpyxl import load_workbook

def log_debug(message):
    debug_file = Path(__file__).parent / 'boq_debug.log'
    with open(debug_file, 'a') as f:
        f.write(f"{message}\n")
    print(message)

# Use session directories from environment
output_file = os.getenv('SESSION_OUTPUT_FILE', '')
session_id = os.getenv('SESSION_ID', '')

log_debug(f"=== BOQ POPULATOR - FORMULA WRITING TO MERGED TEMPLATE ===")

# File paths
project_root = Path(__file__).parent.parent.parent
boq_formula_json_path = project_root / 'boq_formula_mapping.json'
session_output_dir = Path(output_file).parent
boq_output_path = session_output_dir / f"{session_id}_main_carriageway_and_boq.xlsx"

try:
    # Load formula mapping from JSON
    if not boq_formula_json_path.exists():
        log_debug(f"ERROR: Formula mapping JSON not found at {boq_formula_json_path}")
        exit(1)
    
    with open(boq_formula_json_path, 'r', encoding='utf-8') as f:
        formula_mapping = json.load(f)
    
    log_debug(f"Loaded formula mapping with {len(formula_mapping)} items")

    # Check if the merged template file exists
    if not boq_output_path.exists():
        log_debug(f"ERROR: Merged template file not found at {boq_output_path}")
        exit(1)

    # Load the existing merged workbook
    wb = load_workbook(boq_output_path)
    sheet = wb['BOQ']
    
    populated_count = 0
    for item_code, formula_data in formula_mapping.items():
        excel_row = formula_data['excel_row']
        formula_E = formula_data['column_E']
        formula_F = formula_data['column_F']
        
        # Write formulas to the BOQ sheet
        if formula_E:
            sheet[f'E{excel_row}'] = formula_E
        
        if formula_F:
            sheet[f'F{excel_row}'] = formula_F
        
        populated_count += 1
        
        if populated_count % 50 == 0:
            log_debug(f"Processed {populated_count} items...")

    # Save the workbook
    wb.save(boq_output_path)
    wb.close()
    
    log_debug(f"Written {populated_count} formulas to merged template")

except Exception as e:
    log_debug(f"ERROR: {str(e)}")
    import traceback
    log_debug(f"TRACEBACK: {traceback.format_exc()}")

log_debug("=== FINISHED ===")