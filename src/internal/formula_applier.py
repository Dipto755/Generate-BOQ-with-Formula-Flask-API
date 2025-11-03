import json
import os
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter, column_index_from_string

class FormulaApplier:
    """Apply formula templates to Excel files by replacing {row} placeholders with actual row numbers."""
    
    def __init__(self, template_path=None):
        """
        Initialize the FormulaApplier with a template file.
        
        Args:
            template_path: Path to formula_template.json. If None, looks in project root.
        """
        if template_path is None:
            # Default: look for formula_template.json in project root
            current_dir = Path(__file__).parent
            template_path = current_dir.parent.parent / "formula_template.json"
        
        self.template_path = Path(template_path)
        self.template = self._load_template()
    
    def _load_template(self):
        """Load the formula template from JSON file."""
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template file not found: {self.template_path}")
        
        with open(self.template_path, 'r') as f:
            return json.load(f)
    
    def apply_formulas_to_row(self, excel_path, sheet_name, target_row):
        """
        Apply all formulas from template to a specific row in Excel file.
        
        Args:
            excel_path: Path to the Excel file
            sheet_name: Name of the sheet to write to
            target_row: Row number to apply formulas to
        
        Returns:
            Number of formulas applied
        """
        wb = load_workbook(excel_path)
        
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' not found in {excel_path}")
        
        sheet = wb[sheet_name]
        formulas = self.template.get("formulas", {})
        
        count = 0
        for col_letter, formula_template in formulas.items():
            if formula_template:
                # Replace {row} with actual row number
                formula = formula_template.replace("{row}", str(target_row))
                
                # Write formula to cell
                cell_address = f"{col_letter}{target_row}"
                sheet[cell_address] = formula
                count += 1
        
        wb.save(excel_path)
        wb.close()
        
        return count
    
    def apply_formulas_to_rows(self, excel_path, sheet_name, start_row, end_row):
        """
        Apply formulas to a range of rows.
        
        Args:
            excel_path: Path to the Excel file
            sheet_name: Name of the sheet to write to
            start_row: Starting row number (inclusive)
            end_row: Ending row number (inclusive)
        
        Returns:
            Total number of formulas applied
        """
        wb = load_workbook(excel_path)
        
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' not found in {excel_path}")
        
        sheet = wb[sheet_name]
        formulas = self.template.get("formulas", {})
        
        total_count = 0
        for row_num in range(start_row, end_row + 1):
            for col_letter, formula_template in formulas.items():
                if formula_template:
                    # Replace {row} with actual row number
                    formula = formula_template.replace("{row}", str(row_num))
                    
                    # Write formula to cell
                    cell_address = f"{col_letter}{row_num}"
                    sheet[cell_address] = formula
                    total_count += 1
        
        wb.save(excel_path)
        wb.close()
        
        return total_count
    
    def apply_formulas_bulk(self, excel_path, sheet_name, row_numbers):
        """
        Apply formulas to specific row numbers (list).
        
        Args:
            excel_path: Path to the Excel file
            sheet_name: Name of the sheet to write to
            row_numbers: List of row numbers to apply formulas to
        
        Returns:
            Total number of formulas applied
        """
        wb = load_workbook(excel_path)
        
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' not found in {excel_path}")
        
        sheet = wb[sheet_name]
        formulas = self.template.get("formulas", {})
        
        total_count = 0
        for row_num in row_numbers:
            for col_letter, formula_template in formulas.items():
                if formula_template:
                    # Replace {row} with actual row number
                    formula = formula_template.replace("{row}", str(row_num))
                    
                    # Write formula to cell
                    cell_address = f"{col_letter}{row_num}"
                    sheet[cell_address] = formula
                    total_count += 1
        
        wb.save(excel_path)
        wb.close()
        
        return total_count
    
    def get_formula_for_cell(self, col_letter, row_num):
        """
        Get the formula for a specific cell without writing to Excel.
        
        Args:
            col_letter: Column letter (e.g., 'BY', 'GB')
            row_num: Row number
        
        Returns:
            Formula string with {row} replaced, or None if column not in template
        """
        formulas = self.template.get("formulas", {})
        formula_template = formulas.get(col_letter)
        
        if formula_template:
            return formula_template.replace("{row}", str(row_num))
        return None
    
    def apply_formulas_to_all_data_rows(self, excel_path, sheet_name, reference_column='D'):
        """
        Automatically detect data rows based on a reference column and apply formulas.
        
        Args:
            excel_path: Path to the Excel file
            sheet_name: Name of the sheet to write to
            reference_column: Column to check for data (default: 'D')
        
        Returns:
            Dictionary with statistics about the operation
        """
        wb = load_workbook(excel_path)
        
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' not found in {excel_path}")
        
        sheet = wb[sheet_name]
        
        # Find first and last row with data in reference column
        first_row = None
        last_row = None
        
        max_row = sheet.max_row
        for row_num in range(1, max_row + 1):
            cell_value = sheet[f'{reference_column}{row_num}'].value
            if cell_value is not None and str(cell_value).strip() != '':
                if first_row is None:
                    first_row = row_num
                last_row = row_num
        
        if first_row is None:
            raise ValueError(f"No data found in column {reference_column}")
        
        # Apply formulas to all data rows
        formulas = self.template.get("formulas", {})
        total_count = 0
        
        for row_num in range(first_row, last_row + 1):
            for col_letter, formula_template in formulas.items():
                if formula_template:
                    # Replace {row} with actual row number
                    formula = formula_template.replace("{row}", str(row_num))
                    
                    # Write formula to cell
                    cell_address = f"{col_letter}{row_num}"
                    sheet[cell_address] = formula
                    total_count += 1
        
        wb.save(excel_path)
        wb.close()
        
        return {
            "first_row": first_row,
            "last_row": last_row,
            "total_rows": last_row - first_row + 1,
            "formulas_per_row": len(formulas),
            "total_formulas": total_count
        }
    
    def get_template_info(self):
        """Get information about the loaded template."""
        return {
            "template_name": self.template.get("template_name"),
            "source_file": self.template.get("source_file"),
            "sheet_name": self.template.get("sheet_name"),
            "source_row": self.template.get("source_row"),
            "column_range": self.template.get("column_range"),
            "total_formulas": len(self.template.get("formulas", {}))
        }


def main():
    """Example usage of FormulaApplier."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Apply formula templates to Excel files")
    parser.add_argument("excel_file", help="Path to Excel file")
    parser.add_argument("sheet_name", help="Sheet name to write to")
    parser.add_argument("--row", type=int, help="Single row to apply formulas to")
    parser.add_argument("--start-row", type=int, help="Starting row for range")
    parser.add_argument("--end-row", type=int, help="Ending row for range")
    parser.add_argument("--auto", action="store_true", help="Automatically detect data rows from reference column")
    parser.add_argument("--ref-column", default="D", help="Reference column for auto detection (default: D)")
    parser.add_argument("--template", help="Path to formula template JSON (optional)")
    
    args = parser.parse_args()
    
    # Initialize applier
    applier = FormulaApplier(template_path=args.template)
    
    # Show template info
    info = applier.get_template_info()
    print(f"Loaded template: {info['template_name']}")
    print(f"Total formulas in template: {info['total_formulas']}")
    print(f"Column range: {info['column_range']}")
    print()
    
    # Apply formulas
    if args.auto:
        print(f"Auto-detecting data rows from column {args.ref_column}...")
        result = applier.apply_formulas_to_all_data_rows(
            args.excel_file, 
            args.sheet_name,
            reference_column=args.ref_column
        )
        print(f"✓ Detected data rows: {result['first_row']} to {result['last_row']}")
        print(f"✓ Applied {result['total_formulas']} formulas to {result['total_rows']} rows")
    
    elif args.row:
        print(f"Applying formulas to row {args.row}...")
        count = applier.apply_formulas_to_row(args.excel_file, args.sheet_name, args.row)
        print(f"✓ Applied {count} formulas to row {args.row}")
    
    elif args.start_row and args.end_row:
        print(f"Applying formulas to rows {args.start_row} to {args.end_row}...")
        count = applier.apply_formulas_to_rows(
            args.excel_file, 
            args.sheet_name, 
            args.start_row, 
            args.end_row
        )
        print(f"✓ Applied {count} formulas to {args.end_row - args.start_row + 1} rows")
    
    else:
        print("Error: Must specify either --auto, --row, or both --start-row and --end-row")
        return 1
    
    print(f"\nFormulas written to: {args.excel_file}")
    print("Remember to run recalc.py to calculate formula values!")
    
    return 0


if __name__ == "__main__":
    exit(main())