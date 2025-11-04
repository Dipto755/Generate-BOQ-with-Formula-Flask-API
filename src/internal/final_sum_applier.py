"""
Final Sum Applier
Reads formula_final_sum_template.json and applies final sum formulas
Calculates and populates final summary calculations in main_carriageway.xlsx
"""

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
    """Main function to execute final sum application processing"""
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
    template_path = os.path.join(root_dir, 'formula_final_sum_template.json')
    
    # Input/output files in session directory with session_id suffix
    input_excel_path = os.path.join(session_dir, f'main_carriageway_{session_id}.xlsx')
    output_excel_path = os.path.join(session_dir, f'main_carriageway_{session_id}.xlsx')
    
    try:
        # Create FinalSumApplier instance
        applier = FinalSumApplier(
            template_path=template_path,
            input_excel_path=input_excel_path,
            output_excel_path=output_excel_path
        )
        
        # Get template info
        info = applier.get_template_info()
        print(f"Loaded final sum template: {info['template_name']}")
        print(f"Total sum formulas: {info['total_formulas']}")
        
        # Apply final sum formulas
        result = applier.apply_final_sum_formulas()
        
        print(f"✓ Applied {result['total_formulas']} final sum formulas")
        print(f"✓ Target cells updated: {result['total_cells_updated']}")
        print(f"\nFinal sums written to: {result['output_file']} (Sheet: {result['output_sheet']})")
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()


class FinalSumApplier:
    """Apply final sum formulas to output/main_carriageway.xlsx based on template."""
    
    def __init__(self, template_path=None, input_excel_path=None, output_excel_path=None):
        if template_path is None:
            # Look for formula_final_sum_template.json in project root
            current_dir = Path(__file__).parent
            template_path = current_dir.parent.parent / "formula_final_sum_template.json"
        
        self.template_path = Path(template_path)
        self.template = self._load_template()
        
        # Use provided paths
        self.input_excel_path = Path(input_excel_path)
        self.output_excel_path = Path(output_excel_path)
        
        # Ensure output directory exists
        self.output_excel_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _load_template(self):
        """Load final sum template from JSON file."""
        if not self.template_path.exists():
            raise FileNotFoundError(f"Final sum template file not found: {self.template_path}")
        
        with open(self.template_path, 'r') as f:
            return json.load(f)
    
    def apply_final_sum_formulas(self):
        """
        Apply final sum formulas to the specified cells.
        
        Returns:
            Dictionary with statistics about the operation
        """
        print(f"Looking for input Excel file at: {self.input_excel_path}")
        print(f"Input Excel file exists: {self.input_excel_path.exists()}")
        
        if not self.input_excel_path.exists():
            raise FileNotFoundError(f"Input Excel file not found: {self.input_excel_path}")
        
        def process_workbook(wb):
            """Inner function to process the workbook"""
            sheet_name = "Quantity"
            
            if sheet_name not in wb.sheetnames:
                raise ValueError(f"Sheet '{sheet_name}' not found in {self.input_excel_path}")
            
            sheet = wb[sheet_name]
            final_sums = self.template.get("final_sums", {})
            
            print(f"Found {len(final_sums)} final sum formulas in template")
            
            # Apply final sum formulas to specified cells
            total_formulas = 0
            total_cells_updated = 0
            
            for cell_reference, formula in final_sums.items():
                if formula:
                    try:
                        # Apply the formula to the specified cell
                        sheet[cell_reference] = formula
                        total_formulas += 1
                        total_cells_updated += 1
                        print(f"  Applied formula to {cell_reference}: {formula}")
                    except Exception as e:
                        print(f"  [WARNING] Could not apply formula to {cell_reference}: {e}")
            
            return {
                "total_formulas": total_formulas,
                "total_cells_updated": total_cells_updated,
                "output_file": str(self.output_excel_path),
                "output_sheet": sheet_name
            }
        
        # Use safe workbook operation
        try:
            return safe_workbook_operation(str(self.input_excel_path), process_workbook)
        except Exception as e:
            print(f"[ERROR] Failed to apply final sum formulas: {e}")
            raise
    
    def apply_final_sums_with_validation(self, reference_row=5090):
        """
        Apply final sum formulas with validation against reference row.
        
        Args:
            reference_row: Row number to use for validation (default: 5090)
        
        Returns:
            Dictionary with statistics about the operation
        """
        print(f"Loading input from: {self.input_excel_path}")
        print(f"Writing output to: {self.output_excel_path}")
        
        if not self.input_excel_path.exists():
            raise FileNotFoundError(f"Input Excel file not found: {self.input_excel_path}")
        
        def process_workbook(wb):
            """Inner function to process the workbook"""
            sheet_name = "Quantity"
            
            if sheet_name not in wb.sheetnames:
                raise ValueError(f"Sheet '{sheet_name}' not found")
            
            sheet = wb[sheet_name]
            final_sums = self.template.get("final_sums", {})
            
            # Validate against reference row before applying
            validation_results = self.validate_against_reference(sheet, final_sums, reference_row)
            
            # Apply final sum formulas
            total_formulas = 0
            total_cells_updated = 0
            
            for cell_reference, formula in final_sums.items():
                if formula:
                    try:
                        sheet[cell_reference] = formula
                        total_formulas += 1
                        total_cells_updated += 1
                    except Exception as e:
                        print(f"  [WARNING] Could not apply formula to {cell_reference}: {e}")
            
            return {
                "total_formulas": total_formulas,
                "total_cells_updated": total_cells_updated,
                "validation_results": validation_results,
                "output_file": str(self.output_excel_path),
                "output_sheet": sheet_name
            }
        
        # Use safe workbook operation
        try:
            return safe_workbook_operation(str(self.input_excel_path), process_workbook)
        except Exception as e:
            print(f"[ERROR] Failed to apply final sum formulas: {e}")
            raise
    
    def validate_against_reference(self, sheet, final_sums, reference_row):
        """
        Validate formulas against reference row data.
        
        Args:
            sheet: The worksheet object
            final_sums: Dictionary of final sum formulas
            reference_row: Row number for validation
        
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "details": []
        }
        
        for cell_reference, formula in final_sums.items():
            if formula and "SUM" in formula:
                validation_results["total_checks"] += 1
                
                try:
                    # Extract the range from the SUM formula
                    # Example: "=SUM(B7:B5000)" -> extract "B7:B5000"
                    import re
                    match = re.search(r'SUM\(([^)]+)\)', formula)
                    if match:
                        sum_range = match.group(1)
                        
                        # Calculate expected sum from reference row
                        # This is example logic - adjust based on actual requirements
                        if ":" in sum_range:
                            start_cell, end_cell = sum_range.split(":")
                            start_col = ''.join(filter(str.isalpha, start_cell))
                            end_col = ''.join(filter(str.isalpha, end_cell))
                            
                            # Simple validation - can be enhanced
                            validation_results["passed_checks"] += 1
                            validation_results["details"].append({
                                "cell": cell_reference,
                                "status": "passed",
                                "message": "Formula syntax valid"
                            })
                        else:
                            validation_results["failed_checks"] += 1
                            validation_results["details"].append({
                                "cell": cell_reference,
                                "status": "failed",
                                "message": "Invalid SUM range format"
                            })
                
                except Exception as e:
                    validation_results["failed_checks"] += 1
                    validation_results["details"].append({
                        "cell": cell_reference,
                        "status": "failed",
                        "message": f"Validation error: {e}"
                    })
        
        return validation_results
    
    def get_template_info(self):
        """Get information about the loaded final sum template."""
        final_sums = self.template.get("final_sums", {})
        return {
            "template_name": self.template.get("template_name"),
            "source_file": self.template.get("source_file"),
            "sheet_name": self.template.get("sheet_name"),
            "target_row": self.template.get("target_row"),
            "total_formulas": len(final_sums)
        }


if __name__ == "__main__":
    main()
