import json
import os
from pathlib import Path
import shutil
import pandas as pd
from openpyxl import load_workbook

def log_debug(message):
    debug_file = Path(__file__).parent / 'boq_debug.log'
    with open(debug_file, 'a') as f:
        f.write(f"{message}\n")
    print(message)

# Use session directories from environment
output_file = os.getenv('SESSION_OUTPUT_FILE', '')
session_id = os.getenv('SESSION_ID', '')

log_debug(f"=== BOQ POPULATOR - FORMULA WRITING WITH PANDAS + OPENPYXL ===")

# File paths
project_root = Path(__file__).parent.parent.parent
boq_formula_json_path = project_root / 'boq_formula_mapping.json'
boq_template_path = project_root / 'template' / 'BOQ.xlsx'
session_output_dir = Path(output_file).parent
boq_output_path = session_output_dir / f"{session_id}_BOQ.xlsx"

# Use the actual session filename
main_file_name = f"{session_id}_main_carriageway.xlsx"

try:
    # Load formula mapping from JSON
    if not boq_formula_json_path.exists():
        log_debug(f"ERROR: Formula mapping JSON not found at {boq_formula_json_path}")
        exit(1)
    
    with open(boq_formula_json_path, 'r', encoding='utf-8') as f:
        formula_mapping = json.load(f)
    
    log_debug(f"Loaded formula mapping with {len(formula_mapping)} items")

    # Copy BOQ template
    shutil.copy2(boq_template_path, boq_output_path)
    log_debug("BOQ template copied")

    # Load the workbook with openpyxl to preserve formatting
    wb = load_workbook(boq_output_path)
    sheet = wb['BOQ']
    
    populated_count = 0
    for item_code, formula_data in formula_mapping.items():
        excel_row = formula_data['excel_row']
        formula_E = formula_data['column_E']
        formula_F = formula_data['column_F']
        
        # Replace placeholder with actual session filename
        if formula_E:
            formula_E = formula_E.replace('{main_carriageway_file}', main_file_name)
            sheet[f'E{excel_row}'] = formula_E
        
        if formula_F:
            formula_F = formula_F.replace('{main_carriageway_file}', main_file_name)
            sheet[f'F{excel_row}'] = formula_F
        
        populated_count += 1
        

        log_debug(f"Row {excel_row} ('{item_code}'): E={formula_E}, F={formula_F}")

    # Save the workbook
    wb.save(boq_output_path)
    wb.close()
    
    log_debug(f"Written {populated_count} formulas to BOQ with preserved formatting")

except Exception as e:
    log_debug(f"ERROR: {str(e)}")
    import traceback
    log_debug(f"TRACEBACK: {traceback.format_exc()}")

log_debug("=== FINISHED ===")