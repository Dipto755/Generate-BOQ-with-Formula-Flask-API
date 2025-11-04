import json
from pathlib import Path
from openpyxl import load_workbook
import sys
import os
import io
import time
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def wait_for_file_ready(file_path, max_wait_seconds=15):
    """Wait for file to be ready for access"""
    import os
    start_time = time.time()
    
    print(f"[INFO] Waiting for file to be ready: {os.path.basename(file_path)}")
    
    while time.time() - start_time < max_wait_seconds:
        try:
            # Try to open file in read mode to check if it's ready
            with open(file_path, 'rb') as f:
                pass
            # Additional check: try to read first few bytes
            with open(file_path, 'rb') as f:
                f.read(1024)  # Try to read first 1KB
            print(f"[OK] File is ready for access: {os.path.basename(file_path)}")
            return True
        except (IOError, PermissionError) as e:
            print(f"[WAIT] File not ready, waiting... ({time.time() - start_time:.1f}s elapsed)")
            time.sleep(1.0)
        except Exception as e:
            print(f"[ERROR] Unexpected error waiting for file: {e}")
            return False
    
    print(f"[ERROR] Timeout waiting for file to be ready: {file_path}")
    return False

def safe_workbook_operation(file_path, operation_func, max_retries=5):
    """Safely perform workbook operations with enhanced retry logic"""
    import os
    from openpyxl import load_workbook
    
    for attempt in range(max_retries):
        try:
            # Wait for file to be ready
            if not wait_for_file_ready(file_path, max_wait_seconds=10):
                raise IOError(f"File not ready for access: {file_path}")
            
            print(f"[INFO] Attempting workbook operation (attempt {attempt + 1}/{max_retries})")
            
            wb = load_workbook(file_path)
            result = operation_func(wb)
            
            # Save and close with additional delay
            wb.save(file_path)
            wb.close()
            
            # Wait longer after save to ensure file system completion
            time.sleep(2.0)
            
            print(f"[OK] Workbook operation completed successfully")
            return result
            
        except Exception as e:
            print(f"[WARNING] Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                print(f"[ERROR] All attempts failed for {os.path.basename(file_path)}")
                raise e
            
            # Exponential backoff with jitter
            wait_time = (2 ** attempt) * 0.5 + (attempt * 0.1)
            print(f"[INFO] Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)

def main(session_id=None):
    """Main function to execute formula application processing"""
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from api.session_manager import session_manager
    
    # Get session information
    if session_id:
        session = session_manager.get_session(session_id)
        if not session:
            print(f"[ERROR] Session {session_id} not found")
            return
        session_dir = session["output_dir"]
    else:
        # Fallback to original paths if no session_id
        session_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'output')
    
    # Get the script's directory and build relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.join(script_dir, '..', '..')
    
    # Template file in root directory
    template_path = os.path.join(root_dir, 'formula_template.json')
    
    # Input/output files in session directory with session_id suffix
    input_excel_path = os.path.join(session_dir, f'main_carriageway_{session_id}.xlsx')
    output_excel_path = os.path.join(session_dir, f'main_carriageway_{session_id}.xlsx')
    
    try:
        # Create FormulaApplier instance
        applier = FormulaApplier(
            template_path=template_path,
            input_excel_path=input_excel_path,
            output_excel_path=output_excel_path
        )
        
        # Get template info
        info = applier.get_template_info()
        print(f"Loaded template: {info['template_name']}")
        print(f"Total formulas: {info['total_formulas']}")
        print(f"Column range: {info['column_range']}")
        
        # Apply formulas to all data rows
        print(f"Auto-detecting data rows from column D...")
        result = applier.apply_formulas_to_all_data_rows('D', 7)
        
        print(f"✓ Input data rows: {result['input_first_row']} to {result['input_last_row']}")
        print(f"✓ Output rows: {result['output_start_row']} to {result['output_end_row']}")
        print(f"✓ Applied {result['total_formulas']} formulas to {result['input_total_rows']} rows")
        print(f"\nFormulas written to: {result['output_file']} (Sheet: {result['output_sheet']})")
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()


class FormulaApplier:
    """Apply formula templates to output/main_carriageway.xlsx by replacing {row} placeholders."""
    
    def __init__(self, template_path=None, input_excel_path=None, output_excel_path=None):
        if template_path is None:
            # Look for formula_template.json in project root
            current_dir = Path(__file__).parent
            template_path = current_dir.parent.parent / "formula_template.json"
        
        self.template_path = Path(template_path)
        self.template = self._load_template()
        
        # Use provided paths
        self.input_excel_path = Path(input_excel_path)
        self.output_excel_path = Path(output_excel_path)
        
        # Ensure output directory exists
        self.output_excel_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _load_template(self):
        """Load the formula template from JSON file."""
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template file not found: {self.template_path}")
        
        with open(self.template_path, 'r') as f:
            return json.load(f)
    
    def apply_formulas_to_all_data_rows(self, reference_column='D', start_row=7):
        """
        Automatically detect data rows based on a reference column and apply formulas
        to 'Quantity' sheet starting from specified row.
        
        Args:
            reference_column: Column to check for data (default: 'D')
            start_row: Starting row number for writing formulas (default: 7)
        
        Returns:
            Dictionary with statistics about the operation
        """
        print(f"Looking for input Excel file at: {self.input_excel_path}")
        print(f"Input Excel file exists: {self.input_excel_path.exists()}")
        
        if not self.input_excel_path.exists():
            raise FileNotFoundError(f"Input Excel file not found: {self.input_excel_path}")
        
        def process_workbook(wb):
            """Inner function to process the workbook"""
            input_sheet_name = "Quantity"
            
            if input_sheet_name not in wb.sheetnames:
                raise ValueError(f"Input sheet '{input_sheet_name}' not found in {self.input_excel_path}")
            
            input_sheet = wb[input_sheet_name]
            formulas = self.template.get("formulas", {})
            
            # Find data rows from input sheet STARTING FROM start_row
            data_rows = []
            for row_num in range(start_row, input_sheet.max_row + 1):  # Start from start_row instead of 1
                cell_value = input_sheet[f'{reference_column}{row_num}'].value
                if cell_value is not None and str(cell_value).strip():
                    data_rows.append(row_num)
            
            if not data_rows:
                raise ValueError(f"No data found in column {reference_column} from row {start_row}")
            
            print(f"Found {len(data_rows)} data rows in input sheet (starting from row {start_row})")
            
            # Load or create output workbook
            if self.output_excel_path.exists():
                output_wb = load_workbook(self.output_excel_path)
                print(f"Loaded existing output file: {self.output_excel_path}")
            else:
                # Create new workbook if output doesn't exist
                output_wb = load_workbook(self.input_excel_path)
                print(f"Created new output file: {self.output_excel_path}")
            
            # Ensure 'Quantity' sheet exists in output
            output_sheet_name = "Quantity"
            if output_sheet_name not in output_wb.sheetnames:
                # Create Quantity sheet if it doesn't exist
                output_wb.create_sheet(output_sheet_name)
                print(f"Created new sheet: {output_sheet_name}")
            
            output_sheet = output_wb[output_sheet_name]
            
            # Apply formulas to output sheet starting from start_row
            total_count = 0
            output_row = start_row
            
            for input_row_num in data_rows:
                for col_letter, formula_template in formulas.items():
                    if formula_template:
                        formula = formula_template.replace("{row}", str(input_row_num))
                        output_sheet[f"{col_letter}{output_row}"] = formula
                        total_count += 1
                output_row += 1
            
            return {
                "input_first_row": min(data_rows),
                "input_last_row": max(data_rows),
                "input_total_rows": len(data_rows),
                "output_start_row": start_row,
                "output_end_row": output_row - 1,
                "formulas_per_row": len(formulas),
                "total_formulas": total_count,
                "output_file": str(self.output_excel_path),
                "output_sheet": output_sheet_name
            }
        
        # Use safe workbook operation
        try:
            return safe_workbook_operation(str(self.input_excel_path), process_workbook)
        except Exception as e:
            print(f"[ERROR] Failed to apply formulas: {e}")
            raise
    
    def apply_formulas_with_custom_mapping(self, row_mapping, start_row=7):
        """
        Apply formulas with custom row mapping from input to output.
        
        Args:
            row_mapping: Dictionary mapping input_row -> output_row
            start_row: Starting row for output (default: 7)
        
        Returns:
            Dictionary with statistics about the operation
        """
        print(f"Loading input from: {self.input_excel_path}")
        print(f"Writing output to: {self.output_excel_path}")
        
        if not self.input_excel_path.exists():
            raise FileNotFoundError(f"Input Excel file not found: {self.input_excel_path}")
        
        def process_workbook(wb):
            """Inner function to process the workbook"""
            input_sheet_name = "Quantity"
            
            if input_sheet_name not in wb.sheetnames:
                raise ValueError(f"Input sheet '{input_sheet_name}' not found")
            
            input_sheet = wb[input_sheet_name]
            formulas = self.template.get("formulas", {})
            
            # Load or create output workbook
            if self.output_excel_path.exists():
                output_wb = load_workbook(self.output_excel_path)
            else:
                output_wb = load_workbook(self.input_excel_path)
            
            # Ensure 'Quantity' sheet exists
            output_sheet_name = "Quantity"
            if output_sheet_name not in output_wb.sheetnames:
                output_wb.create_sheet(output_sheet_name)
            
            output_sheet = output_wb[output_sheet_name]
            
            # Apply formulas using custom mapping
            total_count = 0
            for input_row, output_row in row_mapping.items():
                for col_letter, formula_template in formulas.items():
                    if formula_template:
                        formula = formula_template.replace("{row}", str(input_row))
                        output_sheet[f"{col_letter}{output_row}"] = formula
                        total_count += 1
            
            return {
                "total_mappings": len(row_mapping),
                "total_formulas": total_count,
                "output_file": str(self.output_excel_path),
                "output_sheet": output_sheet_name
            }
        
        # Use safe workbook operation
        try:
            return safe_workbook_operation(str(self.input_excel_path), process_workbook)
        except Exception as e:
            print(f"[ERROR] Failed to apply formulas: {e}")
            raise
    
    def get_template_info(self):
        """Get information about the loaded template."""
        formulas = self.template.get("formulas", {})
        return {
            "template_name": self.template.get("template_name"),
            "source_file": self.template.get("source_file"),
            "sheet_name": self.template.get("sheet_name"),
            "source_row": self.template.get("source_row"),
            "column_range": self.template.get("column_range"),
            "total_formulas": len(formulas)
        }


if __name__ == "__main__":
    main()
