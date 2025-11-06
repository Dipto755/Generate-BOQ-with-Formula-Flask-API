from openpyxl import load_workbook
import os
from pathlib import Path
import shutil

def log_debug(message):
    debug_file = Path(__file__).parent / 'boq_debug.log'
    with open(debug_file, 'a') as f:
        f.write(f"{message}\n")
    print(message)

# Use session directories from environment
output_file = os.getenv('SESSION_OUTPUT_FILE', '')
session_id = os.getenv('SESSION_ID', '')

log_debug(f"=== BOQ POPULATOR - SESSION FILENAME REFERENCES ===")

# BOQ template and output paths
boq_template_path = Path(__file__).parent.parent.parent / 'template' / 'BOQ.xlsx'
session_output_dir = Path(output_file).parent
boq_output_path = session_output_dir / f"{session_id}_BOQ.xlsx"

# Use the actual session filename
main_file_name = f"{session_id}_main_carriageway.xlsx"

try:
    # Copy BOQ template
    shutil.copy2(boq_template_path, boq_output_path)
    log_debug("BOQ template copied")

    # Load main file to build mapping
    main_wb = load_workbook(output_file)
    main_sheet = main_wb['Abstract']
    
    # Build row mapping
    row_mapping = {}
    for row in range(4, main_sheet.max_row + 1):
        item_code = main_sheet[f'J{row}'].value
        if item_code:
            row_mapping[str(item_code).strip()] = row

    main_wb.close()
    log_debug(f"Built mapping for {len(row_mapping)} items")

    # Update BOQ file with session filename references
    boq_wb = load_workbook(boq_output_path)
    boq_sheet = boq_wb['BOQ']
    
    populated_count = 0
    for row in range(3, min(boq_sheet.max_row + 1, 2200)):
        item_code = boq_sheet[f'A{row}'].value
        if item_code:
            item_code_str = str(item_code).strip()
            if item_code_str in row_mapping:
                abstract_row = row_mapping[item_code_str]
                
                # Create external reference formulas with session filename
                boq_sheet[f'E{row}'] = f"='[{main_file_name}]Abstract'!$F${abstract_row}"
                boq_sheet[f'F{row}'] = f"='[{main_file_name}]Abstract'!$I${abstract_row}"
                
                populated_count += 1
                log_debug(f"Added ref for '{item_code_str}' -> '[{main_file_name}]Abstract'!$F${abstract_row}")

    boq_wb.save(boq_output_path)
    boq_wb.close()
    
    log_debug(f"Added {populated_count} external reference formulas")

except Exception as e:
    log_debug(f"ERROR: {str(e)}")
    import traceback
    log_debug(f"TRACEBACK: {traceback.format_exc()}")

log_debug("=== FINISHED ===")