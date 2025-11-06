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

log_debug(f"=== BOQ POPULATOR WITH SHEET COPYING ===")

# BOQ template and output paths
boq_template_path = Path(__file__).parent.parent.parent / 'template' / 'BOQ.xlsx'
session_output_dir = Path(output_file).parent
boq_output_path = session_output_dir / f"{session_id}_BOQ.xlsx"

log_debug(f"BOQ template exists: {boq_template_path.exists()}")

try:
    # Copy BOQ template to session directory
    shutil.copy2(boq_template_path, boq_output_path)
    log_debug("BOQ template copied successfully")

    # Load both workbooks
    main_wb = load_workbook(output_file)
    boq_wb = load_workbook(boq_output_path)
    
    log_debug(f"Main sheets: {main_wb.sheetnames}")
    log_debug(f"BOQ sheets before: {boq_wb.sheetnames}")

    # Copy ALL sheets from main_carriageway to BOQ file
    for sheet_name in main_wb.sheetnames:
        if sheet_name not in boq_wb.sheetnames:  # Don't overwrite existing BOQ sheets
            source_sheet = main_wb[sheet_name]
            new_sheet = boq_wb.create_sheet(sheet_name)
            
            # Copy all cells with formatting
            for row in source_sheet.iter_rows():
                for cell in row:
                    new_sheet[cell.coordinate].value = cell.value
                    if cell.has_style:
                        new_sheet[cell.coordinate]._style = cell._style
            
            log_debug(f"Copied sheet: {sheet_name}")

    log_debug(f"BOQ sheets after: {boq_wb.sheetnames}")

    # Now add formulas in BOQ sheet that reference the copied sheets
    boq_sheet = boq_wb['BOQ']
    abstract_sheet_name = 'Abstract'  # Name of the copied main_carriageway sheet
    
    # Create mapping formulas in BOQ sheet
    populated_count = 0
    for row in range(3, boq_sheet.max_row + 1):
        item_code = boq_sheet[f'A{row}'].value
        if item_code:
            item_code_str = str(item_code).strip()
            
            # Find the row in Abstract sheet that matches this item code
            abstract_sheet = boq_wb[abstract_sheet_name]
            found_row = None
            
            for abs_row in range(4, abstract_sheet.max_row + 1):
                abs_item_code = abstract_sheet[f'J{abs_row}'].value
                if abs_item_code and str(abs_item_code).strip() == item_code_str:
                    found_row = abs_row
                    break
            
            if found_row:
                # Add Excel formulas that reference the Abstract sheet
                # Column E in BOQ = Column F in Abstract
                boq_sheet[f'E{row}'] = f"={abstract_sheet_name}!F{found_row}"
                # Column F in BOQ = Column I in Abstract  
                boq_sheet[f'F{row}'] = f"={abstract_sheet_name}!I{found_row}"
                populated_count += 1
                log_debug(f"Added formulas for '{item_code_str}' at row {row}")

    log_debug(f"Total formulas added: {populated_count}")

    # Save BOQ file
    boq_wb.save(boq_output_path)
    log_debug(f"BOQ file saved successfully")
    
    main_wb.close()
    boq_wb.close()

except Exception as e:
    log_debug(f"ERROR: {str(e)}")
    import traceback
    log_debug(f"TRACEBACK: {traceback.format_exc()}")

log_debug(f"=== BOQ POPULATOR FINISHED ===")